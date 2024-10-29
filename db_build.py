## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv("TIMESCALE_SERVICE_URL")

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector;"


CREATE_PODCAST_TABLE = """
    CREATE TABLE IF NOT EXISTS podcast (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL
    );
"""

CREATE_SEGMENT_TABLE = """
    CREATE TABLE IF NOT EXISTS podcast_segment (
        id TEXT PRIMARY KEY,
        start_time TIME NOT NULL,
        end_time TIME NOT NULL,
        content TEXT NOT NULL,
        embedding VECTOR NOT NULL,
        podcast_id TEXT NOT NULL
    );
"""

# Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)
conn = psycopg2.connect(CONNECTION)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute(CREATE_EXTENSION)
cursor.execute(CREATE_PODCAST_TABLE)
cursor.execute(CREATE_SEGMENT_TABLE)

conn.commit()
conn.close()