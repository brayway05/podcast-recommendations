## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONNECTION = os.getenv("TIMESCALE_SERVICE_URL")

conn = psycopg2.connect(CONNECTION)
conn.autocommit = True
cursor = conn.cursor()

# Q1) What are the five most similar segments to segment "267:476"
# For each result return the podcast name, the segment id, segment raw text, the start time, stop time, raw text and embedding distance

Q1 = """
    SELECT 
        p.title,
        ps.id,
        ps.content,
        ps.start_time,
        ps.end_time,
        ps.embedding <-> (
            SELECT
                embedding
            FROM    
                podcast_segment
            WHERE
                id = '267:476'
        ) embedding_distance
    FROM 
        podcast_segment ps
    JOIN 
        podcast p
    ON
        ps.podcast_id = p.id
    WHERE 
        ps.id != '267:476'
        AND ps.podcast_id != (
            SELECT
                podcast_id
            FROM    
                podcast_segment
            WHERE
                id = '267:476'
        )
    ORDER BY
        embedding_distance ASC
    LIMIT 5;
"""

# cursor.execute(Q1)
# results = cursor.fetchall() 
# print(results) 

# Q2) What are the five most dissimilar segments to segment "267:476"
# For each result return the podcast name, the segment id, segment raw text, the start time, stop time, raw text and embedding distance

Q2 = """
    SELECT 
        p.title,
        ps.id,
        ps.content,
        ps.start_time,
        ps.end_time,
        ps.embedding <-> (
            SELECT
                embedding
            FROM    
                podcast_segment
            WHERE
                id = '267:476'
        ) embedding_distance
    FROM 
        podcast_segment ps
    JOIN 
        podcast p
    ON
        ps.podcast_id = p.id
    WHERE 
        ps.id != '267:476'
        AND ps.podcast_id != (
            SELECT
                podcast_id
            FROM    
                podcast_segment
            WHERE
                id = '267:476'
        )
    ORDER BY
        embedding_distance DESC
    LIMIT 5;
"""

# cursor.execute(Q2)
# results = cursor.fetchall() 
# print(results) 

# Q3) What are the five most similar segments to segment '48:511'
# For each result return the podcast name, the segment id, segment raw text, the start time, stop time, raw text and embedding distance    

Q3 = """
    SELECT 
        p.title,
        ps.id,
        ps.content,
        ps.start_time,
        ps.end_time,
        ps.embedding <-> (
            SELECT
                embedding
            FROM    
                podcast_segment
            WHERE
                id = '48:511'
        ) embedding_distance
    FROM 
        podcast_segment ps
    JOIN 
        podcast p
    ON
        ps.podcast_id = p.id
    WHERE 
        ps.id != '48:511'
        AND ps.podcast_id != (
            SELECT
                podcast_id
            FROM    
                podcast_segment
            WHERE
                id = '48:511'
        )
    ORDER BY
        embedding_distance ASC
    LIMIT 5;
"""

# cursor.execute(Q3)
# results = cursor.fetchall() 
# print(results) 

# Q4) What are the five most similar segments to segment '51:56'
# For each result return the podcast name, the segment id, segment raw text, the start time, stop time, raw text and embedding distance

Q4 = """
    SELECT 
        p.title,
        ps.id,
        ps.content,
        ps.start_time,
        ps.end_time,
        ps.embedding <-> (
            SELECT
                embedding
            FROM    
                podcast_segment
            WHERE
                id = '51:56'
        ) embedding_distance
    FROM 
        podcast_segment ps
    JOIN 
        podcast p
    ON
        ps.podcast_id = p.id
    WHERE 
        ps.id != '51:56'
        AND ps.podcast_id != (
            SELECT
                podcast_id
            FROM    
                podcast_segment
            WHERE
                id = '51:56'
        )
    ORDER BY
        embedding_distance ASC
    LIMIT 5;
"""

# cursor.execute(Q4)
# results = cursor.fetchall() 
# print(results) 

# Q5) For each of the following podcast segments, find the five most similar podcast episodes. Hint: You can do this by averaging over the embedding vectors within a podcast episode.
# For each result return the Podcast title and the embedding distance

#     a) Segment "267:476"

Q5a = """
    SELECT 
        p.title,
        AVG(ps.embedding) <-> (
            SELECT
                AVG(embedding)
            FROM    
                podcast_segment
            WHERE
                id = '267:476'
        ) embedding_distance
    FROM 
        podcast_segment ps
    JOIN 
        podcast p
    ON
        ps.podcast_id = p.id
    GROUP BY
        p.title
    ORDER BY
        embedding_distance ASC
    LIMIT 5;
"""

# cursor.execute(Q5a)
# results = cursor.fetchall() 
# print(results) 

#     b) Segment '48:511'

Q5b = """
    SELECT 
        p.title,
        AVG(ps.embedding) <-> (
            SELECT
                AVG(embedding)
            FROM    
                podcast_segment
            WHERE
                id = '48:511'
        ) embedding_distance
    FROM 
        podcast_segment ps
    JOIN 
        podcast p
    ON
        ps.podcast_id = p.id
    GROUP BY
        p.title
    ORDER BY
        embedding_distance ASC
    LIMIT 5;
"""

# cursor.execute(Q5b)
# results = cursor.fetchall() 
# print(results) 

#     c) Segment '51:56'

Q5c = """
    SELECT 
        p.title,
        AVG(ps.embedding) <-> (
            SELECT
                AVG(embedding)
            FROM    
                podcast_segment
            WHERE
                id = '51:56'
        ) embedding_distance
    FROM 
        podcast_segment ps
    JOIN 
        podcast p
    ON
        ps.podcast_id = p.id
    GROUP BY
        p.title
    ORDER BY
        embedding_distance ASC
    LIMIT 5;
"""

# cursor.execute(Q5c)
# results = cursor.fetchall() 
# print(results) 


# Q6) For podcast episode id = VeH7qKZr0WI, find the five most similar podcast episodes. Hint: you can do a similar averaging procedure as Q5
# For each result return the Podcast title and the embedding distance

Q6 = """
    SELECT 
        p.title,
        AVG(ps.embedding) <-> (
            SELECT
                AVG(embedding)
            FROM    
                podcast_segment
            WHERE
                podcast_id = 'VeH7qKZr0WI'
        ) embedding_distance
    FROM
        podcast_segment ps
    JOIN    
        podcast p
    ON  
        ps.podcast_id = p.id
    WHERE
        ps.podcast_id != 'VeH7qKZr0WI'
    GROUP BY
        p.title
    ORDER BY    
        embedding_distance ASC
    LIMIT 5;
"""

# cursor.execute(Q6)
# results = cursor.fetchall()
# print(results)