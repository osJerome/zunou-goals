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
    # Task 1.1: Fetch Fireflies API key from `integrations` and strategies from `strategies` by `pulse_id`
    DB_HOST = getenv("DB_HOST")
    DB_NAME = getenv("DB_NAME")
    DB_USER = getenv("DB_USER")
    DB_PASSWORD = getenv("DB_PASSWORD")

    # strategies
    strategies_query= """
        SELECT
            name, description
        FROM
            strategies
        WHERE
            type = 'objectives';
    """
    
    conn = create_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(strategies_query)
        strategies = json.dumps(cursor.fetchall())
        print(strategies)
    
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

    meetings = None
    cache_file = "fireflies_meetings_cache.txt"

    # Try to load from cache first
    try:
        with open(cache_file, 'r') as f:
            meetings = json.loads(f.read())
            print("Using cached meetings data")
    except (FileNotFoundError, json.JSONDecodeError):
        # If cache doesn't exist or is invalid, fetch from API
        if fireflies_integrations:
            api_key = fireflies_integrations[0].get("api_key")
            if api_key:
                query = "query Transcripts { transcripts { title summary { keywords short_summary } } }"
                data = {"query": query}
                result = request_fireflies(api_key, data)
                meetings = result.get("data", {}).get("transcripts", [])
                
                # Store the fetched data in cache
                try:
                    with open(cache_file, 'w') as f:
                        json.dump(meetings, f)
                    print("Stored meetings data in cache")
                except Exception as e:
                    print(f"Error storing cache: {e}")
                
                print("Fetched meeting from Fireflies:", meetings)
            else:
                print("Fireflies integration entry missing 'api_key'.")
        else:
            print("No fireflies integrations available.")

    # Task 2: Provide a relation checker between `integrations` and `strategies`
    OPENAI_API_KEY = getenv("OPENAI_API_KEY")
    llm = OpenAI(api_key=OPENAI_API_KEY, temperature=0)
    for meeting in meetings:
        meeting_title = meeting.get("title")
        # if there is no summary, proceed to the next meeting
        if not meeting.get("summary"):
            continue
        meeting_short_summary = meeting.get("summary", {}).get("short_summary")

        prompt = PromptTemplate(input_variables=["strategies", "title", "short_summary"], template='''
            Meetings tend to be unrelated to the strategies listed. Based on the information below, determine if the meeting is irrelevant to the strategies.
            Strategies:
            {strategies}

            Meeting Title:
            {title}
                            
            Meeting Short Summary:
            {short_summary}

            If uncertain default to 'No'.
            If the meeting title contains words that are in strategies, default to 'Yes'.
            If there is any doubt about the relevance of the meeting to the strategy, respond with 'No'.
            Is the meeting relevant to the strategy? Reply with 'Yes' or 'No'.
        ''')
        chain = prompt | llm
        response = chain.invoke({
            "strategies": strategies,
            "title": meeting_title,
            "short_summary": meeting_short_summary
        })
        is_related = response.lower().strip()

        print(is_related, meeting.get("title"))

        # Task 3: Store meeting as "PENDING" to `meetings`
        pass

        # Task 4 & 5: Run as parallel tasks
        # Task 4: Store meeting as "COMPLETED" to `meetings`
        pass

        # Task 5: Ingest strategies and meeting then send as notification summary
        pass