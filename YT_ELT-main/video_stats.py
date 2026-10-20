import requests 
import json
import os
from dotenv import load_dotenv

# Resolve .env relative to this file, not the directory the script is run from
load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"

def get_playlist_id():  
       
    try: 
            
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        print(response)

        # Raise on 4xx/5xx (e.g. missing/invalid API key) instead of failing later with KeyError
        response.raise_for_status()

        data = response.json()

        #print(json.dumps(data, indent=4))

        channel_items= data['items'][0]

        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']; 

        print(channel_playlistId)

        return channel_playlistId

    except requests.exceptions.RequestException as e: 
        print(f"An error occurred while making the request: {e}")
        return None


if __name__ == "__main__":
    print("get_playlist_id() will be executed when this script is run directly.")
    get_playlist_id()
else:
    print("get_playlist_id() will not be executed when this script is imported as a module.")