import clickhouse_connect

class ClickhouseConnector:

    def __init__(self, host: str, username: str, password: str, port: int = 8443) -> None:

        self.__host = host
        self.__username = username
        self.__password = password
        self.__port = port


    def create_connection(self):

        """
        Create a new ClickHouse connection.
        """

        client = clickhouse_connect.get_client(host=self.__host, port=self.__port, username=self.__username, password=self.__password)

        return client