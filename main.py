import json

from os import getenv
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from tools.connect import create_connection
from tools.fireflies import request_fireflies

from langchain.prompts import PromptTemplate
from langchain_openai import OpenAI

load_dotenv()


if __name__ == "__main__":
    # Task 1.1: Fetch Fireflies API key from `integrations` and goals from `goals` by `pulse_id`
    DB_HOST = getenv("DB_HOST")
    DB_NAME = getenv("DB_NAME")
    DB_USER = getenv("DB_USER")
    DB_PASSWORD = getenv("DB_PASSWORD")

    # Goals
    goals_query = """
        SELECT
            name, pulse_id, organization_id, description
        FROM
            goals
        WHERE
            type = 'objectives';
    """
    
    conn = create_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(goals_query)
        goals = json.dumps(cursor.fetchall())
        print(goals)
    
    # Fireflies API Keys
    integrations_query = """
        SELECT
            user_id, api_key
        FROM
            integrations
        WHERE
            type = 'fireflies';
    """
    
    conn = create_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(integrations_query)
        ff_keys = json.dumps(cursor.fetchall())
        print(ff_keys)
            
    # Task 1.2: Fetch meetings from Fireflies
    try:
        fireflies_integrations = json.loads(ff_keys)
    except Exception as e:
        print("Error decoding fireflies API keys:", e)
        fireflies_integrations = []

    meeting = None
    if fireflies_integrations:
        # Use the first available API key from integrations to fetch new meetings from Fireflies
        api_key = fireflies_integrations[0].get("api_key")
        if api_key:
            query = "query Transcripts { transcripts { id title summary { keywords action_items overview } } }"
            data = {"query": query}
            result = request_fireflies(api_key, data)
            meetings = result.get("data", {}).get("transcripts", [])
            # print("Fetched meeting from Fireflies:", meetings)
        else:
            print("Fireflies integration entry missing 'api_key'.")
    else:
        print("No fireflies integrations available.")

    # Task 2: Provide a relation checker between `integrations` and `goals`
    OPENAI_API_KEY = getenv("OPENAI_API_KEY")
    llm = OpenAI(api_key=OPENAI_API_KEY, temperature=0)
    for meeting in meetings:
        prompt = PromptTemplate(
            input_variables=["goals", "meeting"],
            template="Act as an intelligent transcript analyzer, Given the following transcript data and goal information, please determine if the transcript is highly related to the goal. Base your evaluation on the evidence provided in the transcript fields. The goal data is {goals} and the transcript data is {meeting}. Answer with a 'yes' or 'no' and add a short explanation." 
        )
        chain = prompt | llm
        response = chain.invoke({
            "goals": goals,
            "meeting": meeting
        })
        is_related = response.lower().strip()

        print(is_related, meeting.get("title"))

        # Task 3: Store meeting as "PENDING" to `meetings`
        pass

        # Task 4 & 5: Run as parallel tasks
        # Task 4: Store meeting as "COMPLETED" to `meetings`
        pass

        # Task 5: Ingest goals and meeting then send as notification summary
        pass