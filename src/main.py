import pandas as pd
import yaml
import os
import clickhouse_connect
from langchain import LLMChain, PromptTemplate
from langchain.llms import OpenAI
from src.connector import ClickhouseConnector
from src.text2sql import PromptSchemaTransformer, Text2Sql

if __name__ == 'main':

  config_path = 'config/'
  with open(f"{config_path}config.yaml", "r") as stream:
    try:
        config = yaml.safe_load(stream)

        connections_config = config['connections']
        clickhouse_config = connections_config['clickhouse']
        clickhouse_host = clickhouse_config['host']
        clickhouse_username = clickhouse_config['username']
        clickhouse_password = clickhouse_config['password']
        clickhouse_port = clickhouse_config['port']

        models_config = config['models']
        openai_config = models_config['openai']
        openai_api_key = openai_config['api_key']
        openai_params = openai_config['params']
    except yaml.YAMLError as exc:
        print(exc)

    os.environ["OPENAI_API_KEY"] = openai_api_key

    conn = ClickhouseConnector(clickhouse_host, clickhouse_username, clickhouse_password, clickhouse_port)
    client = conn.create_connection()

    pst = PromptSchemaTransformer(client)
    db_schema = pst.transform_db_schema_for_prompt()

    model = OpenAI(**openai_params)
    text2sql = Text2Sql(model)

    question = 'How many LinkedIn clicks did we have in 2022?'

    sql = text2sql.get_sql_by_question_and_schema(question, db_schema)

    df = client.query_df(f"""{sql}""")
    print(df)