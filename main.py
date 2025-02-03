from os import getenv
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from tools.connect import create_connection
from tools.fireflies import request_fireflies

load_dotenv()

# Fireflies
FF_URI = getenv("FF_URI")
FF_API_KEY = getenv("FF_API_KEY")

# OpenAI
OPENAI_API_KEY = getenv("OPENAI_API_KEY")

# PostgreSQL
DB_HOST = getenv("DB_HOST")
DB_NAME = getenv("DB_NAME")
DB_USER = getenv("DB_USER")
DB_PASSWORD = getenv("DB_PASSWORD")


if __name__ == "__main__":
    # Task 1: Fetch Fireflies API key from `integrations` and goals from `goals` by `pulse_id`
    conn = create_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
    
    with conn.cursor(RealDictCursor) as cursor:
        query = """
            SELECT 
                g.name, 
                g.description, 
                g.type AS goal_type, 
                i.api_key, 
                i.type AS integration_type
            FROM goals AS g
            JOIN integrations AS i
            ON g.pulse_id = i.pulse_id;
            """
        
        cursor.execute(query)
        relations = cursor.fetchall()
        print(relations)
    
    # Task 2: Provide a relation checker between `integrations` and `goals`
    pass

    # Task 3: Store meeting as "PENDING" to `meetings`
    pass

    # Task 4 & 5: Run as parallel tasks
    # Task 4: Store meeting as "COMPLETED" to `meetings`
    pass

    # Task 5: Ingest goals and meeting then send as notification summary
    pass