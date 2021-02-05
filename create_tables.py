import psycopg2

from sql_queries import create_table_queries, drop_table_queries, insert_table_queries


class ETLPostgres:
    def __init__(self):
        self.user_name = "student"
        self.password = "student"
        self.database_name = "studentdb"
        self.database_sparkify = "sparkifydb"
        self.url = '127.0.0.1'

    def create_database(self):
        """
        - Creates and connects to the sparkifydb
        - Returns the connection and cursor to sparkifydb
        """

        # connect to default database
        conn = psycopg2.connect(f'host={self.url} dbname={self.database_name} user={self.user_name} \
        password={self.password}')
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS " + self.database_sparkify)
        cur.execute("CREATE DATABASE " + self.database_sparkify + " WITH ENCODING 'utf8' TEMPLATE template0")

        # close connection to default database
        conn.close()

        # connect to sparkify database
        conn = psycopg2.connect(f'host={self.url} dbname={self.database_sparkify} user={self.user_name} \
                password={self.password}')
        cur = conn.cursor()
        # Activate auto commit
        conn.set_session(autocommit=True)
        return cur, conn

    @staticmethod
    def drop_tables(cur):
        """
        Drops each table using the queries in `drop_table_queries` list.
        """
        for query in drop_table_queries:
            try:
                cur.execute(query)
            except Exception as e:
                print(e)

    @staticmethod
    def create_tables(cur):
        """
        Creates each table using the queries in `create_table_queries` list.
        """
        for query in create_table_queries:
            try:
                cur.execute(query)
            except Exception as e:
                print(e)
        print("Tables created successfully")

    @staticmethod
    def insert_tables(cur):
        """
        Creates each table using the queries in `create_table_queries` list.
        """
        for query in insert_table_queries:
            try:
                # Insert values before running
                data = ()
                cur.execute(query, data)
            except Exception as e:
                print(e)
        print("Values inserted to tables successfully")


def main():
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    try:
        etl = ETLPostgres()
        cur, conn = etl.create_database()

        etl.drop_tables(cur)
        etl.create_tables(cur)
        cur.close()
        conn.close()
    except Exception as e:
        print(e)
    # insert_tables(cur, conn)


if __name__ == "__main__":
    main()
