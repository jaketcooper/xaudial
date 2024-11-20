import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from pathlib import Path
import json
import os
from typing import List, Dict
from datetime import datetime, timedelta
import dotenv
from dotenv import load_dotenv, set_key
import time

class SpotifyPlaylistLister:
    def __init__(self, client_id: str, client_secret: str, redirect_uri: str):
        """Initialize Spotify client with user authentication"""
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.scope = "playlist-read-private playlist-read-collaborative"
        
        # Initialize the client with valid authentication
        self.authenticate()

    def authenticate(self):
        """Handle authentication process, including token management"""
        # Load environment variables
        load_dotenv()
        
        # Check for existing token and timestamp
        token = os.getenv('SPOTIFY_ACCESS_TOKEN')
        token_timestamp = os.getenv('SPOTIFY_TOKEN_TIMESTAMP')
        
        needs_new_token = True
        
        if token and token_timestamp:
            try:
                # Convert timestamp to datetime
                token_time = datetime.fromtimestamp(float(token_timestamp))
                # Check if token is less than 50 minutes old (giving 10-minute buffer)
                if datetime.now() - token_time < timedelta(minutes=50):
                    needs_new_token = False
                    print("Using existing token...")
            except ValueError:
                needs_new_token = True
        
        if needs_new_token:
            print("Token missing or expired. Starting authentication process...")
            # Initialize OAuth manager with special options for manual input
            self.oauth = SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope=self.scope,
                open_browser=False  # Prevent automatic browser opening
            )
            
            # Get the auth URL and request user input
            auth_url = self.oauth.get_authorize_url()
            print("\nPlease visit this URL to authorize the application:")
            print(auth_url)
            print("\nAfter authorizing, you'll be redirected to a URL containing a 'code' parameter.")
            print("Please copy the entire URL you were redirected to:")
            response_url = input("\nEnter the URL you were redirected to: ").strip()
            
            # Extract code from response URL and get token
            code = self.oauth.parse_response_code(response_url)
            token_info = self.oauth.get_access_token(code)
            
            # Save token and timestamp to environment
            token = token_info['access_token']
            current_timestamp = str(time.time())
            
            # Update .env file
            env_file = Path('.env')
            if not env_file.exists():
                env_file.touch()
            
            set_key(env_file, 'SPOTIFY_ACCESS_TOKEN', token)
            set_key(env_file, 'SPOTIFY_TOKEN_TIMESTAMP', current_timestamp)
            
            print("New token saved to environment variables.")
        
        # Initialize Spotify client with the token
        self.sp = spotipy.Spotify(auth=token)

    def get_all_playlists(self) -> List[Dict]:
        """Fetch all playlists for the authenticated user"""
        try:
            playlists = []
            offset = 0
            limit = 50  # Maximum allowed by Spotify API
            
            while True:
                results = self.sp.current_user_playlists(limit=limit, offset=offset)
                
                if not results['items']:
                    break
                    
                for playlist in results['items']:
                    # Get the owner's display name
                    owner_name = playlist['owner']['display_name']
                    if not owner_name:  # Fallback to ID if display name is None
                        owner_name = playlist['owner']['id']
                    
                    playlists.append({
                        'name': playlist['name'],
                        'id': playlist['id'],
                        'owner': owner_name,
                        'tracks_total': playlist['tracks']['total'],
                        'public': playlist['public'],
                        'collaborative': playlist['collaborative'],
                    })
                
                offset += limit
                
                if len(results['items']) < limit:
                    break
            
            return playlists
            
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 401:  # Unauthorized - token might be expired
                print("Token expired during operation. Re-authenticating...")
                self.authenticate()
                return self.get_all_playlists()  # Retry with new token
            raise

    def save_playlists(self, playlists: List[Dict]):
        """Save playlists to various formats"""
        # Create output directory
        output_dir = Path('spotify_playlists')
        output_dir.mkdir(exist_ok=True)
        
        # Get current timestamp for filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Convert to DataFrame
        df = pd.DataFrame(playlists)
        
        # Save as CSV
        df.to_csv(output_dir / f'playlists_{timestamp}.csv', index=False)
        
        # Save as Excel
        df.to_excel(output_dir / f'playlists_{timestamp}.xlsx', index=False)
        
        # Save as JSON
        with open(output_dir / f'playlists_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(playlists, f, indent=2, ensure_ascii=False)
        
        # Save as simple text file with just names and IDs
        with open(output_dir / f'playlist_ids_{timestamp}.txt', 'w', encoding='utf-8') as f:
            for playlist in playlists:
                f.write(f"{playlist['id']}\n")
        
        return output_dir

def main():
    # Load environment variables
    load_dotenv()
    
    # Your Spotify API credentials from .env file
    CLIENT_ID = os.getenv('CLIENT_ID', "3226c7189a0b403c9daf846e26cd1221")
    CLIENT_SECRET = os.getenv('CLIENT_SECRET', "1103f3318e9e4ff7a3f1ecdcb397f52e")
    REDIRECT_URI = os.getenv('REDIRECT_URI', "http://3.213.192.126:23578/callback")
    
    try:
        # Initialize the playlist lister
        lister = SpotifyPlaylistLister(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI)
        
        print("\nFetching your playlists...")
        playlists = lister.get_all_playlists()
        
        if not playlists:
            print("No playlists found!")
            return
        
        # Save the playlists
        output_dir = lister.save_playlists(playlists)
        
        # Print summary
        print(f"\nFound {len(playlists)} playlists!")
        print(f"\nPlaylist files saved to: {output_dir}")
        print("\nSummary of playlists:")
        print("-" * 80)
        
        # Sort playlists by track count
        sorted_playlists = sorted(playlists, key=lambda x: x['tracks_total'], reverse=True)
        
        for playlist in sorted_playlists:
            owner_info = " (Collaborative)" if playlist['collaborative'] else f" (by {playlist['owner']})"
            visibility = "Public" if playlist['public'] else "Private"
            print(f"{playlist['name']:<50} {playlist['tracks_total']:>5} tracks  {visibility}{owner_info}")
            print(f"ID: {playlist['id']}")
            print("-" * 80)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()