import requests
import pandas as pd
import time
import env
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

api_key = env.api_key

# Define the folder path where the existing chunks are stored
folder_path = r'C:\Users\shegg\PycharmProjects\pythonFinalExam\final_project'
chunk_file_prefix = 'updated_horror_movies_chunk_'
chunk_file_suffix = '.csv'


# Function to get additional movie data from OMDb with retries
def get_additional_omdb_data(imdb_id, api_key):
    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={api_key}&tomatoes=true"
    for attempt in range(3):  # Retry up to 3 times
        try:
            response = requests.get(url, timeout=5)
            data = response.json()
            if response.status_code == 200 and data.get('Response', 'False') == 'True':
                return {
                    'Country': data.get('Country', 'N/A'),
                    'Metascore': data.get('Metascore', 'N/A'),
                    'tomatoRating': data.get('tomatoRating', 'N/A'),
                    'tomatoUserRating': data.get('tomatoUserRating', 'N/A'),
                    'Rated': data.get('Rated', 'N/A'),
                    'Rating': data.get('imdbRating', 'N/A')
                }
            else:
                return {'Country': 'N/A', 'Metascore': 'N/A', 'tomatoRating': 'N/A', 'tomatoUserRating': 'N/A',
                        'Rated': 'N/A', 'Rating': 'N/A'}
        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"Error fetching {imdb_id}, attempt {attempt + 1}: {e}")
            time.sleep(2)  # Wait before retrying
    return {'Country': 'N/A', 'Metascore': 'N/A', 'tomatoRating': 'N/A', 'tomatoUserRating': 'N/A', 'Rated': 'N/A',
            'Rating': 'N/A'}


# Function to process a chunk and upload additional data using multithreading
def process_chunk(i, chunk_file_path, api_key):
    print(f"Processing chunk file: {chunk_file_path}")
    chunk_df = pd.read_csv(chunk_file_path)

    new_columns = ['Country', 'Metascore', 'tomatoRating', 'tomatoUserRating', 'Rated', 'Rating']
    for col in new_columns:
        if col not in chunk_df.columns:
            chunk_df[col] = pd.NA

    with ThreadPoolExecutor(max_workers=5) as executor:  # Run up to 5 parallel requests
        futures = {
            executor.submit(get_additional_omdb_data, row['tconst'], api_key): (index, row['tconst'])
            for index, row in chunk_df.iterrows() if pd.isna(row['Country'])  # Skip if already fetched
        }
        for future in as_completed(futures):
            index, imdb_id = futures[future]
            try:
                omdb_additional_data = future.result()
                for col in new_columns:
                    chunk_df.loc[index, col] = omdb_additional_data[col]
            except Exception as e:
                print(f"Failed to process {imdb_id}: {e}")

    # Save the updated chunk back to the file
    try:
        chunk_df.to_csv(chunk_file_path, index=False)
        print(f"Updated and saved chunk {i} to {chunk_file_path}")
    except Exception as e:
        print(f"Error saving updated chunk {i}: {e}")


# Start processing from chunk 10 to the last one
start_chunk = 10
end_chunk = 21

for i in range(start_chunk, end_chunk + 1):
    chunk_file_path = os.path.join(folder_path, f"{chunk_file_prefix}{i}{chunk_file_suffix}")

    if os.path.exists(chunk_file_path):
        process_chunk(i, chunk_file_path, api_key)
    else:
        print(f"Chunk file {chunk_file_path} does not exist.")
