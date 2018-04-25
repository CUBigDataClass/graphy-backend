from cassandra.cluster import Cluster
from cassandra.cqlengine import connection
from faker import Faker

def connect_to_cassandra(make_table = False):
    cluster = Cluster()
    session = cluster.connect('test01')
    rows = session.execute("SELECT * FROM countries WHERE id = 1")
    for country in rows:
        print(country)
    if make_table:
        session.execute("""CREATE TABLE Classified_Tweets (
        id INT PRIMARY KEY,
        text TEXT,
        label TEXT,
        url TEXT,
        author TEXT,
        lat DECIMAL ,
        long DECIMAL 
    );""")
    return session

def put_fake_data_in_cassandra(session, n = 10000, start = 0):
    fake = Faker()

    for i in range(start, start + n):
        id = i
        text = fake.catch_phrase()
        label = list(fake.random_sample_unique(['sports', 'politics','entertainment','mood'],1))[0]
        url = fake.domain_name()
        author = fake.email()
        lat = float(fake.latitude())
        long = float(fake.longitude())

        session.execute(
            """
            INSERT INTO Classified_Tweets (id, text, label, url, author, lat, long)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (id, text, label, url, author, lat, long)
        )


if __name__ == '__main__':
    # session = connect_to_cassandra(make_table=True)
    # put_fake_data_in_cassandra(session, n = 10000, start = 0)
    fake = Faker()