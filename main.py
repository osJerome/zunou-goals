from os import getenv
import logging
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from tools.connect import create_connection
from tools.fireflies import request_fireflies

from langchain.prompts import PromptTemplate
from langchain_openai import OpenAI

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()


def fetch_goals_and_integrations() -> tuple:
    """Fetch goals and Fireflies integrations from database"""
    DB_HOST = getenv("DB_HOST")
    DB_NAME = getenv("DB_NAME")
    DB_USER = getenv("DB_USER")
    DB_PASSWORD = getenv("DB_PASSWORD")

    goals = []
    integrations = []
    
    try:
        # Use a single connection with context manager
        conn = create_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
        with conn:
            # Fetch goals
            goals_query = """
                SELECT
                    name, pulse_id, organization_id, description
                FROM
                    goals
                WHERE
                    type = 'objectives';
            """
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(goals_query)
                goals = cursor.fetchall()
            
            # Fetch Fireflies API Keys
            integrations_query = """
                SELECT
                    pulse_id, user_id, api_key
                FROM
                    integrations
                WHERE
                    type = 'fireflies';
            """
            
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(integrations_query)
                integrations = cursor.fetchall()
                
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise
    
    return goals, integrations


def get_relations(goals: List[Dict], integrations: List[Dict]) -> List[Dict]:
    """Group goals by pulse_id and create relations with integrations"""
    # Group goals by `pulse_id`
    goals_dict = {}
    for goal in goals:
        pulse_id = goal["pulse_id"]
        if pulse_id not in goals_dict:
            goals_dict[pulse_id] = []
        goals_dict[pulse_id].append({
            "name": goal["name"],
            "description": goal["description"]
        })

    relations = []
    for integration in integrations:
        pulse_id = integration["pulse_id"]
        relations.append({
            "user_id": integration["user_id"],
            "pulse_id": pulse_id,
            "api_key": integration["api_key"],
            "goals": goals_dict.get(pulse_id, [])
        })
    
    return relations


def fetch_fireflies_meetings(relations: List[Dict]) -> List[Dict]:
    """Fetch meetings from Fireflies API for each relation"""
    meetings = []
    
    for relation in relations:
        try:
            # Get user info
            user_query = {"query": "{ users { name user_id } }"}
            user_response = request_fireflies(relation["api_key"], user_query)
            
            if not user_response.get("data") or not user_response["data"].get("users"):
                logger.warning(f"No users found for relation {relation['pulse_id']}")
                continue
                
            user = user_response["data"]["users"][0]
            
            # Get transcripts
            transcript_query = {
                "query": "query Transcripts($userId: String) { transcripts(user_id: $userId) { title id } }",
                "variables": {"userId": user["user_id"]},
            }
            
            transcripts_response = request_fireflies(relation["api_key"], transcript_query)
            
            if not transcripts_response.get("data") or not transcripts_response["data"].get("transcripts"):
                logger.warning(f"No transcripts found for user {user['user_id']}")
                continue
            
            for transcript in transcripts_response["data"]["transcripts"]:
                summary_query = {
                    "query": """
                    query Transcript($transcriptId: String!) {
                        transcript(id: $transcriptId) {
                            summary {
                                short_summary
                            }
                        }
                    }
                    """,
                    "variables": {
                        "transcriptId": transcript["id"],
                    },
                }
                
                summary_response = request_fireflies(relation["api_key"], summary_query)
                
                if (not summary_response.get("data") or 
                    not summary_response["data"].get("transcript") or 
                    not summary_response["data"]["transcript"].get("summary")):
                    logger.warning(f"No summary found for transcript {transcript['id']}")
                    continue
                
                short_summary = summary_response["data"]["transcript"]["summary"]["short_summary"]
                
                meetings.append({
                    "transcript_id": transcript["id"],
                    "title": transcript["title"],
                    "user_id": user["user_id"],
                    "user_name": user["name"],
                    "pulse_id": relation["pulse_id"],
                    "short_summary": short_summary,
                    "goals": relation["goals"]
                })
                
        except Exception as e:
            logger.error(f"Error fetching Fireflies data: {e}")
            continue
    
    return meetings


def check_meeting_goal_relation(meeting: Dict, llm: OpenAI) -> bool:
    """Check if meeting is related to goals using LLM"""
    try:
        prompt = PromptTemplate(
            input_variables=["goals", "short_summary"],
            template="""
                Goals:
                {goals}
                    
                Short Summary:
                {short_summary}
                
                
                Determine the relationship between goals and the meeting summary.
                Reply with 'Yes' or 'No'.
            """
        )
        chain = prompt | llm
        
        response = chain.invoke({
            "goals": meeting["goals"],
            "short_summary": meeting["short_summary"]
        })
        
        is_related = 'yes' in response.lower()
        return is_related
        
    except Exception as e:
        logger.error(f"Error checking relation: {e}")
        return False


def store_meeting(meeting: Dict, status: str, db_connection) -> None:
    """Store meeting info in database"""
    try:
        query = """
            INSERT INTO meetings (transcript_id, title, user_id, pulse_id, summary, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (transcript_id) DO UPDATE
            SET status = %s, summary = %s
        """
        
        with db_connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    meeting["transcript_id"],
                    meeting["title"],
                    meeting["user_id"],
                    meeting["pulse_id"],
                    meeting["short_summary"],
                    status,
                    status,
                    meeting["short_summary"]
                )
            )
        db_connection.commit()
        
    except Exception as e:
        logger.error(f"Error storing meeting: {e}")
        db_connection.rollback()


def send_notification_summary(meeting: Dict, is_related: bool) -> None:
    """Send notification summary for related meetings"""
    # Implement notification logic here
    if is_related:
        logger.info(f"Sending notification for meeting: {meeting['title']}")
        # Add your notification code here
    

def main():
    try:
        # 1. Fetch goals and integrations
        goals, integrations = fetch_goals_and_integrations()
        logger.info(f"Fetched {len(goals)} goals and {len(integrations)} integrations")
        
        # 2. Group by pulse_id
        relations = get_relations(goals, integrations)
        logger.info(f"Created {len(relations)} relations")
        
        # 3. Fetch meetings from Fireflies
        meetings = fetch_fireflies_meetings(relations)
        logger.info(f"Fetched {len(meetings)} meetings")
        
        # 4. Initialize OpenAI
        OPENAI_API_KEY = getenv("OPENAI_API_KEY")
        if not OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY environment variable not set")
            
        llm = OpenAI(api_key=OPENAI_API_KEY, temperature=0, model="gpt-4o-mini")
        
        # 5. Connect to database for storing meetings
        DB_HOST = getenv("DB_HOST")
        DB_NAME = getenv("DB_NAME")
        DB_USER = getenv("DB_USER")
        DB_PASSWORD = getenv("DB_PASSWORD")
        
        conn = create_connection(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
        
        # Process each meeting
        for meeting in meetings:
            # Store as PENDING first
            store_meeting(meeting, "PENDING", conn)
            
            # Check relation
            is_related = check_meeting_goal_relation(meeting, llm)
            
            # Update status to COMPLETED and send notification if related
            store_meeting(meeting, "COMPLETED", conn)
            
            if is_related:
                send_notification_summary(meeting, is_related)
        
        conn.close()
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Main execution error: {e}")
        raise


if __name__ == "__main__":
    main()