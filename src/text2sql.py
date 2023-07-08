import pandas as pd
import clickhouse_connect
import openai
from langchain import LLMChain, PromptTemplate
from langchain.llms import OpenAI

class PromptSchemaTransformer:

    def __init__(self, client) -> None:

        self.__client = client


    def __get_database_schema(self) -> pd.DataFrame:

        """
        Get all tables, their columns and column data types in the database.
        """

        sql_query_db_schema = """
        SELECT DISTINCT
            TABLE_NAME,
            column_name,
            data_type
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            table_catalog = 'default'
            AND TABLE_NAME != 'statistics'
        """

        df_db_schema = self.__client.query_df(sql_query_db_schema)
        return df_db_schema


    def __create_db_schema_for_prompt(self, df_db_schema: pd.DataFrame) -> str:

        """
        Create DB schema description in text format for prompt engineering.
        The DB schema description is created in the following format:
        Table: {TABLE_NAME}
        Columns and data types:
        {Column_Name1}: {Data_Type1}
        {Column_Name2}: {Data_Type2}
        """

        db_schema_description = """"""

        # tables = list(df_db_schema['TABLE_NAME'].unique())
        # for table in tables:

        #     table_name_description = f'Table: {table}'
        #     columns_and_datatypes_line = 'Columns and data types:'
        #     table_columns = list(df_db_schema.loc[df_db_schema['TABLE_NAME'] == table, 'column_name'].unique())
        #     table_column_datatypes = list(df_db_schema.loc[df_db_schema['TABLE_NAME'] == table, 'data_type'].unique())
        #     table_column_descriptions = [f'{column}: {data_type}' for column, data_type in list(zip(table_columns, table_column_datatypes))]

        #     db_schema_description += """\n""".join([table_name_description, columns_and_datatypes_line] + table_column_descriptions + [''])

        db_schema_description = """
        # CUSTOMERS(CUSTOMER_ID: int, CUSTOMER_NAME: str)
        # CUSTOMERS_PLATFORMS(CUSTOMER_ID: int, PLATFORM_ID: int, CPC: float)
        # EVENTS(EVENT_ID: int, EVENT_TYPE: str)
        # EVENT_STATISTICS(date: datetime, customer_id: int, platform_id: int, event_id: int)
        # Primary keys:
        # CUSTOMERS: CUSTOMER_ID
        # EVENTS: EVENT_ID
        # Foreign keys:
        # CUSTOMERS_PLATFORMS: CUSTOMER_ID to CUSTOMERS.CUSTOMER_ID, PLATFORM_ID to PLATFORMS.PLATFORM_ID
        # EVENT_STATISTICS: customer_id to CUSTOMERS.CUSTOMER_ID, platform_id to PLATFORMS.PLATFORM_ID, event_id to EVENTS.EVENT_ID
        """

        return db_schema_description


    def transform_db_schema_for_prompt(self) -> str:

        """
        Transform the Clickhouse DB schema to a view
        suitable for the prompt.
        """

        df_db_schema = self.__get_database_schema()
        db_schema_for_prompt = self.__create_db_schema_for_prompt(df_db_schema)

        return db_schema_for_prompt



class Text2Sql:

    def __init__(self, model) -> None:

        self.__llm = model


    def __create_prompt_template(self) -> str:

        """
        Create a prompt template
        according to which we will conduct the prompt engineering.
        """

        template = """
        This is a task converting text into SQL statement. We will first given the dataset schema and then ask a question in
        text. You are asked to generate SQL statement.
        Here is an example: Convert text to SQL:
        [Schema (values)]: | farm | city : city_id , official_name , status , area_km_2 , population ,
        census_ranking | farm : farm_id , year , total_horses , working_horses , total_cattle , oxen
        , bulls , cows , pigs , sheep_and_goats | farm_competition : competition_id , year , theme ,
        host_city_id , hosts | competition_record : competition_id , farm_id , rank;
        [Column names (type)]: city : city_id (number)| city : official_name (text)| city : status (text
        )| city : area_km_2 (number)| city : population (number)| city : census_ranking (text)| farm
        : farm_id (number)| farm : year (number)| farm : total_horses (number)| farm : working_horses
        (number)| farm : total_cattle (number)| farm : oxen (number)| farm : bulls (number)| farm
        : cows (number)| farm : pigs (number)| farm : sheep_and_goats (number)| farm_competition :
        competition_id (number)| farm_competition : year (number)| farm_competition : theme (text)|
        farm_competition : host_city_id (number)| farm_competition : hosts (text)| competition_record
        : competition_id (number)| competition_record : farm_id (number)| competition_record : rank (
        number);
        [Primary Keys]: city : city_id | farm : farm_id | farm_competition : competition_id |
        competition_record : competition_id;
        [Foreign Keys]: farm_competition : host_city_id equals city : city_id | competition_record :
        farm_id equals farm : farm_id | competition_record : competition_id equals farm_competition :
        competition_id
        [Q]: What are the themes of farm competitions sorted by year in ascending order?;
        [SQL]: select theme from farm_competition order by year asc;
        Here is an example: Convert text to SQL:
        [Schema (values)]: | farm | city : city_id , official_name , status , area_km_2 , population ,
        census_ranking | farm : farm_id , year , total_horses , working_horses , total_cattle , oxen
        , bulls , cows , pigs , sheep_and_goats | farm_competition : competition_id , year , theme ,
        host_city_id , hosts | competition_record : competition_id , farm_id , rank;
        [Column names (type)]: city : city_id (number)| city : official_name (text)| city : status (text
        )| city : area_km_2 (number)| city : population (number)| city : census_ranking (text)| farm
        : farm_id (number)| farm : year (number)| farm : total_horses (number)| farm : working_horses
        (number)| farm : total_cattle (number)| farm : oxen (number)| farm : bulls (number)| farm
        : cows (number)| farm : pigs (number)| farm : sheep_and_goats (number)| farm_competition :
        competition_id (number)| farm_competition : year (number)| farm_competition : theme (text)|
        farm_competition : host_city_id (number)| farm_competition : hosts (text)| competition_record
        : competition_id (number)| competition_record : farm_id (number)| competition_record : rank (
        number);
        [Primary Keys]: city : city_id | farm : farm_id | farm_competition : competition_id |
        competition_record : competition_id; [Foreign Keys]: farm_competition : host_city_id equals
        city : city_id | competition_record : farm_id equals farm : farm_id | competition_record :
        competition_id equals farm_competition : competition_id
        [Q]: What are the maximum and minimum number of cows across all farms.;
        [SQL]: select max(cows), min(cows) from farm;
        Here is an example: Convert text to SQL:
        [Schema (values)]: | department_management | department : department_id , name , creation ,
        ranking , budget_in_billions , num_employees | head : head_id , name , born_state , age |
        management : department_id , head_id , temporary_acting ( Yes );
        [Column names (type)]: department : department_id (number)| department : name (text)| department
        : creation (text)| department : ranking (number)| department : budget_in_billions (number)
        | department : num_employees (number)| head : head_id (number)| head : name (text)| head :
        born_state (text)| head : age (number)| management : department_id (number)| management :
        head_id (number)| management : temporary_acting (text);
        [Primary Keys]: department : department_id | head : head_id | management : department_id;
        [Foreign Keys]: management : head_id equals head : head_id | management : department_id equals
        department : department_id
        [Q]: Show the name and number of employees for the departments managed by heads whose temporary
        acting value is ’Yes’?;
        [SQL]: select t1.name, t1.num_employees from department as t1 join management as t2 on t1.
        department_id = t2.department_id where t2.temporary_acting = ’Yes’;
        Here is an example: Convert text to SQL:
        [Schema (values)]: | farm | city : city_id , official_name , status , area_km_2 , population ,
        census_ranking | farm : farm_id , year , total_horses , working_horses , total_cattle , oxen
        , bulls , cows , pigs , sheep_and_goats | farm_competition : competition_id , year , theme ,
        host_city_id , hosts | competition_record : competition_id , farm_id , rank;
        [Column names (type)]: city : city_id (number)| city : official_name (text)| city : status (text
        )| city : area_km_2 (number)| city : population (number)| city : census_ranking (text)| farm
        : farm_id (number)| farm : year (number)| farm : total_horses (number)| farm : working_horses
        (number)| farm : total_cattle (number)| farm : oxen (number)| farm : bulls (number)| farm
        : cows (number)| farm : pigs (number)| farm : sheep_and_goats (number)| farm_competition :
        competition_id (number)| farm_competition : year (number)| farm_competition : theme (text)|
        farm_competition : host_city_id (number)| farm_competition : hosts (text)| competition_record
        : competition_id (number)| competition_record : farm_id (number)| competition_record : rank (
        number);
        [Primary Keys]: city : city_id | farm : farm_id | farm_competition : competition_id |
        competition_record : competition_id;
        [Foreign Keys]: farm_competition : host_city_id equals city : city_id | competition_record :
        farm_id equals farm : farm_id | competition_record : competition_id equals farm_competition :
        competition_id
        [Q]: Show the status of the city that has hosted the greatest number of competitions.;
        [SQL]: select t1.status from city as t1 join farm_competition as t2 on t1.city_id = t2.
        host_city_id group by t2.host_city_id order by count(*) desc limit 1;
        Here is the test question to be answered: Convert text to SQL:
        [Schema (values)]: | EVENTS_DB | CUSTOMERS: CUSTOMER_ID , CUSTOMER_NAME | CUSTOMERS_PLATFORMS: CUSTOMER_ID, PLATFORM_ID, CPC | EVENTS: EVENT_ID, EVENT_TYPE | EVENT_STATISTICS : date, customer_id, platform_id, event_id | PLATFORMS: PLATFORM_ID, PLATFORM_NAME;
        [Column names (type)]: CUSTOMERS: CUSTOMER_ID (number)| CUSTOMERS: CUSTOMER_NAME (text)| CUSTOMERS_PLATFORMS: CUSTOMER_ID(number)| CUSTOMERS_PLATFORMS: PLATFORM_ID (number)| CUSTOMERS_PLATFORMS: CPC 
        (float) | EVENTS: EVENT_ID(number)| EVENTS: EVENT_TYPE (text)| EVENT_STATISTICS : date(datetime)| EVENT_STATISTICS: customer_id(number)| EVENT_STATISTICS:
        platform_id(number)| EVENT_STATISTICS: event_id(number)| PLATFORMS: PLATFORM_ID(number)| PLATFORM_NAME(text);
        [Primary Keys]: CUSTOMERS: CUSTOMER_ID| EVENTS: EVENT_ID| PLATFORMS: PLATFORM_ID;
        [Foreign Keys]: CUSTOMERS_PLATFORMS: CUSTOMER_ID equals CUSTOMERS: CUSTOMER_ID| CUSTOMERS_PLATFORMS: PLATFORM_ID equals PLATFORMS: PLATFORM_ID| EVENT_STATISTICS: customer_id equals CUSTOMERS: customer_id| EVENT_STATISTICS: platform_id equals PLATFORMS: platform_id| EVENT_STATISTICS: event_id equals EVENTS: EVENT_ID;
        [Q]: {question};
        [SQL]:
        """

        # prompt_template = PromptTemplate(template=template, input_variables=["question", "db_schema"])
        prompt_template = PromptTemplate(template=template, input_variables=["question"])

        return prompt_template


    def __create_llm_chain(self, prompt_template: str, llm) -> LLMChain:

        """
        Create LLM chain taking pre-created prompt template and initialized LLM as inputs.
        """

        llm_chain = LLMChain(prompt=prompt_template, llm=llm)

        return llm_chain


    def __create_sql_by_question(self, llm_chain: LLMChain, question: str, db_schema: str) -> str:

        """
        Internal method creating a SQL by natural-language question and DB schema definition.
        """

        sql = llm_chain.run({'question': question})
        return sql


    def get_sql_by_question_and_schema(self, question: str, db_schema: str) -> str:

        """
        This function should be called externally to create an SQL query
        based on natural-language question and pre-defined DB schema.
        """

        prompt_template = self.__create_prompt_template()
        llm_chain = self.__create_llm_chain(prompt_template, self.__llm)
        sql = self.__create_sql_by_question(llm_chain, question=question, db_schema=db_schema)

        return sql