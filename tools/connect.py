import psycopg2

def create_connection(host, database, user, password, port=5432):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        
        return conn
    except psycopg2.Error as e:
        raise Exception(f"Unable to connect to the database, {str(e)}")
