import psycopg2


def test_db():
    connection = psycopg2.connect(
    host="172.17.0.2",
    port="5432",
    database="postgres",
    user="postgres",
    password="shartekar123"
    )
    print(connection)
    print("Connection Established Successfully!!!!!!.........................")