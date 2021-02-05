import psycopg2


class TestData:

    def connect_to_db(self):
        # connect to default database
        conn = psycopg2.connect('host=127.0.0.1 port=5432 dbname=sparkifydb user=admin password=welcome1')
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        return cur, conn

    def read_table(self, table_name):
        cursor, conn = self.connect_to_db()
        cursor.execute("select * from " + table_name)
        row = cursor.fetchone()
        while row:
            print(row)
            row = cursor.fetchone()


if __name__ == "__main__":
    tb = TestData()
    table_names = ['T_SONG_PLAY', 'T_USER', 'T_SONG', 'T_ARTIST', 'T_TIME_TABLE']

    for table in table_names:
        print("Table Name : " + table)
        tb.read_table(table)
