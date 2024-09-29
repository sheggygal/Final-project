import requests
import pandas as pd
import time
import env
import json

api_key = env.api_key

# Use the absolute path to CSV file
file_path = r'C:\Users\shegg\PycharmProjects\pythonFinalExam\final_project\horror_movies (1).csv'
df = pd.read_csv(file_path)

# Function to get movie data from OMDb
def get_omdb_data(imdb_id, api_key):
    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={api_key}"
    response = requests.get(url)
    try:
        # Check if the content is JSON by using response.content and json.loads
        data = json.loads(response.content)
        if response.status_code == 200 and 'Response' in data and data['Response'] == 'True':
            return {
                'ProductionBudget': data.get('BoxOffice', 'N/A'),
                'BoxOffice': data.get('BoxOffice', 'N/A'),
                'Awards': data.get('Awards', 'N/A'),
                'PlotKeywords': data.get('Plot', 'N/A'),
                'ProductionStudio': data.get('Production', 'N/A'),
                'Country': data.get('Country', 'N/A'),
                'Metascore': data.get('Metascore', 'N/A'),
                'tomatoRating': data.get('tomatoRating', 'N/A'),
                'tomatoUserRating': data.get('tomatoUserRating', 'N/A'),
                'Rated': data.get('Rated', 'N/A'),  # Corrected key case
            }
        else:
            print(f"Error fetching data for {imdb_id}: {data.get('Error', 'Unknown Error')}")
            return {
                'ProductionBudget': 'N/A',
                'BoxOffice': 'N/A',
                'Awards': 'N/A',
                'PlotKeywords': 'N/A',
                'ProductionStudio': 'N/A',
                'Country': 'N/A',
                'Metascore': 'N/A',
                'tomatoRating': 'N/A',
                'tomatoUserRating': 'N/A',
                'Rated': 'N/A'
            }
    except json.JSONDecodeError:
        print(f"Invalid JSON response for {imdb_id}: {response.content}")
        return {
            'ProductionBudget': 'N/A',
            'BoxOffice': 'N/A',
            'Awards': 'N/A',
            'PlotKeywords': 'N/A',
            'ProductionStudio': 'N/A',
            'Country': 'N/A',
            'Metascore': 'N/A',
            'tomatoRating': 'N/A',
            'tomatoUserRating': 'N/A',
            'Rated': 'N/A'
        }

# Split the data into chunks of 10,000 rows each
chunk_size = 10000
num_chunks = len(df) // chunk_size + 1

# Adjust the range to only process chunks 13 through 21
start_chunk = 17  # Start from the 18th chunk (index is 12)
end_chunk = num_chunks  # Process up to the last chunk (21st)

# Iterate through chunks
for i in range(start_chunk, end_chunk):
    chunk_df = df[i*chunk_size:(i+1)*chunk_size].copy()  # Make a copy of the chunk to avoid SettingWithCopyWarning
    print(f"Processing chunk {i+1} of {num_chunks}...")

    # Create new columns for the additional data in each chunk
    chunk_df.loc[:, 'ProductionBudget'] = None
    chunk_df.loc[:, 'BoxOffice'] = None
    chunk_df.loc[:, 'Awards'] = None
    chunk_df.loc[:, 'PlotKeywords'] = None
    chunk_df.loc[:, 'ProductionStudio'] = None  # New column for production studio
    chunk_df.loc[:, 'Country'] = None
    chunk_df.loc[:, 'Metascore'] = None
    chunk_df.loc[:, 'tomatoRating'] = None
    chunk_df.loc[:, 'tomatoUserRating'] = None
    chunk_df.loc[:, 'Rated'] = None

    # Iterate over the rows in each chunk and fetch OMDb data
    for index, row in chunk_df.iterrows():
        imdb_id = row['tconst']
        omdb_data = get_omdb_data(imdb_id, api_key)
        print(f"Processing {imdb_id}: {omdb_data}")  # Debugging API response

        # Update the dataframe with the OMDb data using .loc
        chunk_df.loc[index, 'ProductionBudget'] = omdb_data['ProductionBudget']
        chunk_df.loc[index, 'BoxOffice'] = omdb_data['BoxOffice']
        chunk_df.loc[index, 'Awards'] = omdb_data['Awards']
        chunk_df.loc[index, 'PlotKeywords'] = omdb_data['PlotKeywords']
        chunk_df.loc[index, 'ProductionStudio'] = omdb_data['ProductionStudio']
        chunk_df.loc[index, 'Country'] = omdb_data['Country']
        chunk_df.loc[index, 'Metascore'] = omdb_data['Metascore']
        chunk_df.loc[index, 'tomatoRating'] = omdb_data['tomatoRating']
        chunk_df.loc[index, 'tomatoUserRating'] = omdb_data['tomatoUserRating']
        chunk_df.loc[index, 'Rated'] = omdb_data['Rated']

        # To avoid hitting API limits, sleep for a second between requests
        time.sleep(1)

    # Save each chunk as a separate CSV file, with added error handling and data check
    if not chunk_df.empty:
        chunk_file_path = f'C:\\Users\\shegg\\PycharmProjects\\pythonFinalExam\\final_project\\updated_horror_movies_chunk_{i+1}.csv'
        try:
            chunk_df.to_csv(chunk_file_path, index=False)
            print(f"Saved chunk {i+1} to {chunk_file_path}")
        except Exception as e:
            print(f"Error saving chunk {i+1}: {e}")
    else:
        print(f"Chunk {i+1} is empty, not saving.")

# Check the result (displaying the first few rows of the last processed chunk)
print(chunk_df.head())




