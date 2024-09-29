import requests
import pandas as pd
import time
import env
import json
import os

api_key = env.api_key

# Define the folder path where the existing chunks are stored
folder_path = r'C:\Users\shegg\PycharmProjects\pythonFinalExam\final_project'
chunk_file_prefix = 'updated_horror_movies_chunk_'
chunk_file_suffix = '.csv'


# Function to get additional movie data from OMDb
def get_additional_omdb_data(imdb_id, api_key):
    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={api_key}&tomatoes=true"
    response = requests.get(url)
    try:
        data = json.loads(response.content)
        if response.status_code == 200 and 'Response' in data and data['Response'] == 'True':
            return {
                'Country': data.get('Country', 'N/A'),  # Corrected key case
                'Metascore': data.get('Metascore', 'N/A'),  # Corrected key case
                'tomatoRating': data.get('tomatoRating', 'N/A'),  # Corrected key case
                'tomatoUserRating': data.get('tomatoUserRating', 'N/A'),  # Corrected key case
                'Rated': data.get('Rated', 'N/A'),  # Corrected key case
                'Rating': data.get('imdbRating', 'N/A')  # Corrected key, usually 'imdbRating' for IMDb rating
            }
        else:
            print(f"Error fetching data for {imdb_id}: {data.get('Error', 'Unknown Error')}")
            return {
                'Country': 'N/A',
                'Metascore': 'N/A',
                'tomatoRating': 'N/A',
                'tomatoUserRating': 'N/A',
                'Rated': 'N/A',
                'Rating': 'N/A'
            }
    except json.JSONDecodeError:
        print(f"Invalid JSON response for {imdb_id}: {response.content}")
        return {
            'Country': 'N/A',
            'Metascore': 'N/A',
            'tomatoRating': 'N/A',
            'tomatoUserRating': 'N/A',
            'Rated': 'N/A',
            'Rating': 'N/A'
        }


# Iterate through the existing chunk files and update them
start_chunk = 1  # Start from the 1st chunk
end_chunk = 21  # End at the 21st chunk

for i in range(start_chunk, end_chunk + 1):
    chunk_file_path = os.path.join(folder_path, f"{chunk_file_prefix}{i}{chunk_file_suffix}")

    # Check if the file exists
    if os.path.exists(chunk_file_path):
        print(f"Processing chunk file: {chunk_file_path}")

        # Load the existing chunk file
        chunk_df = pd.read_csv(chunk_file_path)

        # Check if new columns already exist; if not, add them with correct dtype
        new_columns = ['Country', 'Metascore', 'tomatoRating', 'tomatoUserRating', 'Rated', 'Rating']
        for col in new_columns:
            if col not in chunk_df.columns:
                chunk_df[col] = pd.NA

        # Iterate over the rows in each chunk and fetch additional OMDb data
        for index, row in chunk_df.iterrows():
            imdb_id = row['tconst']
            omdb_additional_data = get_additional_omdb_data(imdb_id, api_key)
            print(f"Processing {imdb_id}: {omdb_additional_data}")  # Debugging API response

            # Update the dataframe with the OMDb additional data using .loc
            for col in new_columns:
                chunk_df.loc[index, col] = omdb_additional_data[col]

            # To avoid hitting API limits, sleep for a second between requests
            time.sleep(1)

        # Save the updated chunk to the same CSV file, with error handling
        try:
            chunk_df.to_csv(chunk_file_path, index=False)
            print(f"Updated and saved chunk {i} to {chunk_file_path}")
        except Exception as e:
            print(f"Error saving updated chunk {i}: {e}")
    else:
        print(f"Chunk file {chunk_file_path} does not exist.")

# Check the result (displaying the first few rows of the last processed chunk)
print(chunk_df.head())

