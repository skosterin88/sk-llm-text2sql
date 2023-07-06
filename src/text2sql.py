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

        tables = list(df_db_schema['TABLE_NAME'].unique())
        # for table in tables:

        #     table_name_description = f'Table: {table}'
        #     columns_and_datatypes_line = 'Columns and data types:'
        #     table_columns = list(df_db_schema.loc[df_db_schema['TABLE_NAME'] == table, 'column_name'].unique())
        #     table_column_datatypes = list(df_db_schema.loc[df_db_schema['TABLE_NAME'] == table, 'data_type'].unique())
        #     table_column_descriptions = [f'{column}: {data_type}' for column, data_type in list(zip(table_columns, table_column_datatypes))]

        #     db_schema_description += """\n""".join([table_name_description, columns_and_datatypes_line] + table_column_descriptions + [''])

        for table in tables:
            table_description = f'# {table}'
            table_columns = list(df_db_schema.loc[df_db_schema['TABLE_NAME'] == table, 'column_name'].unique())
            table_column_datatypes = list(df_db_schema.loc[df_db_schema['TABLE_NAME'] == table, 'data_type'].unique())

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
        ### Clickhouse SQL tables, with their properties:
        #
        {db_schema}
        ### A query to answer the following question: {question}
        ### Tables and columns names are case-sensitive.
        """

        prompt_template = PromptTemplate(template=template, input_variables=["question", "db_schema"])

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

        sql = llm_chain.run({'question': question, 'db_schema': db_schema})
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