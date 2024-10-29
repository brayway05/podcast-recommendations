## This script is used to insert data into the database
import os
import json
from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd

from utils import fast_pg_insert

load_dotenv()

# Read the embedding files
embeddings_path = "data/embedding"
embeddings_files = os.listdir(embeddings_path)

embeddings_df = pd.DataFrame()

for file in embeddings_files:
    with open(os.path.join(embeddings_path, file), "r") as f:
        embeddings = []
        for line in f:
            try:
                embeddings.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in file {file}: {e}")
        embedding_df = pd.DataFrame(embeddings)
        embedding_df['embedding'] = embedding_df['response'].apply(lambda x: x['body']['data'][0]['embedding'])
        embedding_df = embedding_df[['custom_id', 'embedding']]
        embeddings_df = pd.concat([embeddings_df, embedding_df], ignore_index=True)


# print(embeddings_df.head())

# Read documents files
documents_path = "data/documents"
documents_files = os.listdir(documents_path)

podcasts_df = pd.DataFrame()
segments_df = pd.DataFrame()

for file in documents_files:
    with open(os.path.join(documents_path, file), "r") as f:
        documents = []
        for line in f:
            try:
                documents.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON in file {file}: {e}")
        podcast_df = pd.DataFrame(documents)
        podcast_df['title'] = podcast_df['body'].apply(lambda x: x['metadata']['title'] if 'metadata' in x and 'title' in x['metadata'] else None)
        podcast_df['id'] = podcast_df['body'].apply(lambda x: x['metadata']['podcast_id'] if 'metadata' in x and 'podcast_id' in x['metadata'] else None)
        podcast_df = podcast_df[['id', 'title']]

        podcasts_df = pd.concat([podcasts_df, podcast_df], ignore_index=True)
        podcasts_df.drop_duplicates(inplace=True)

        segment_df = pd.DataFrame(documents)
        segment_df['start_time'] = segment_df['body'].apply(lambda x: x['metadata']['start_time'] if 'metadata' in x and 'start_time' in x['metadata'] else None)
        segment_df['end_time'] = segment_df['body'].apply(lambda x: x['metadata']['stop_time'] if 'metadata' in x and 'stop_time' in x['metadata'] else None)
        segment_df['content'] = segment_df['body'].apply(lambda x: x['input'] if 'input' in x else None)
        segment_df['podcast_id'] = segment_df['body'].apply(lambda x: x['metadata']['podcast_id'] if 'metadata' in x and 'podcast_id' in x['metadata'] else None)
        segment_df = segment_df[['custom_id', 'start_time', 'end_time', 'content', 'podcast_id']]

        segments_df = pd.concat([segments_df, segment_df], ignore_index=True)

segments_df = segments_df.merge(embeddings_df, on='custom_id', how='inner')
segments_df.rename(columns={'custom_id': 'id'}, inplace=True)
segments_df['start_time'] = pd.to_datetime(segments_df['start_time'], unit='s').dt.time
segments_df['end_time'] = pd.to_datetime(segments_df['end_time'], unit='s').dt.time

print(segments_df.head())

# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
# ds = load_dataset("Whispering-GPT/lex-fridman-podcast")


# Insert into postgres
# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time

fast_pg_insert(podcasts_df, connection=os.getenv("TIMESCALE_SERVICE_URL"), table_name="podcast", columns=['id', 'title'])
fast_pg_insert(segments_df, connection=os.getenv("TIMESCALE_SERVICE_URL"), table_name="podcast_segment", columns=['id', 'start_time', 'end_time', 'content', 'podcast_id', 'embedding'])