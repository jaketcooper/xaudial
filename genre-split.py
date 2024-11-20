import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
import sys
import os
from pathlib import Path
from tqdm import tqdm
from collections import defaultdict
import json

class SpotifyGenreOrganizer:
    def __init__(self):
        """Initialize Spotify client with OAuth"""
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id="3226c7189a0b403c9daf846e26cd1221",
            client_secret="1103f3318e9e4ff7a3f1ecdcb397f52e",
            redirect_uri="http://3.213.192.126/callback",
            scope="playlist-modify-public playlist-modify-private"
        ))
        self.genre_cache = {}  # Cache for artist genres to reduce API calls

    def get_artist_genres(self, artist_id: str) -> list:
        """Get genres for an artist, using cache if available"""
        if artist_id in self.genre_cache:
            return self.genre_cache[artist_id]
        
        try:
            artist_info = self.sp.artist(artist_id)
            genres = artist_info['genres']
            self.genre_cache[artist_id] = genres
            return genres
        except Exception as e:
            print(f"Error fetching genres for artist {artist_id}: {e}")
            return []

    def get_artist_id(self, artist_name: str) -> str:
        """Search for an artist and return their Spotify ID"""
        try:
            results = self.sp.search(q=artist_name, type='artist', limit=1)
            if results['artists']['items']:
                return results['artists']['items'][0]['id']
        except Exception as e:
            print(f"Error searching for artist {artist_name}: {e}")
        return None

    def organize_by_genre(self, csv_path: str):
        """Load CSV and organize matching tracks by genre"""
        # Load and validate CSV
        try:
            df = pd.read_csv(csv_path)
            required_columns = {'id', 'meets_criteria', 'name', 'artists'}
            if not all(col in df.columns for col in required_columns):
                print("Error: CSV file missing required columns (id, meets_criteria, name, artists)")
                sys.exit(1)
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            sys.exit(1)

        # Filter for tracks that meet criteria
        matching_tracks = df[df['meets_criteria'] == True].copy()
        
        if matching_tracks.empty:
            print("No tracks found that meet the flow state criteria!")
            sys.exit(1)
        
        print(f"Processing {len(matching_tracks)} tracks that meet flow state criteria")

        # Create genre mapping
        genre_tracks = defaultdict(list)
        tracks_without_genre = []

        for _, track in tqdm(matching_tracks.iterrows(), desc="Fetching genre information"):
            # Get first artist for genre lookup (could be expanded to check all artists)
            artist_name = track['artists'].split(',')[0].strip()
            artist_id = self.get_artist_id(artist_name)
            
            if artist_id:
                genres = self.get_artist_genres(artist_id)
                if genres:
                    # Add track to each genre it belongs to
                    for genre in genres:
                        genre_tracks[genre].append({
                            'name': track['name'],
                            'artists': track['artists'],
                            'id': track['id'],
                            'tempo': track.get('tempo', 'N/A'),
                            'energy': track.get('energy', 'N/A'),
                            'loudness': track.get('loudness', 'N/A')
                        })
                else:
                    tracks_without_genre.append({
                        'name': track['name'],
                        'artists': track['artists'],
                        'id': track['id'],
                        'tempo': track.get('tempo', 'N/A'),
                        'energy': track.get('energy', 'N/A'),
                        'loudness': track.get('loudness', 'N/A')
                    })
            else:
                tracks_without_genre.append({
                    'name': track['name'],
                    'artists': track['artists'],
                    'id': track['id'],
                    'tempo': track.get('tempo', 'N/A'),
                    'energy': track.get('energy', 'N/A'),
                    'loudness': track.get('loudness', 'N/A')
                })

        # Generate reports
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path('genre_analysis') / timestamp
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create summary report
        with open(output_dir / "genre_summary.txt", "w") as f:
            f.write("Flow State Tracks by Genre\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Tracks Analyzed: {len(matching_tracks)}\n")
            f.write(f"Unique Genres Found: {len(genre_tracks)}\n")
            f.write(f"Tracks Without Genre: {len(tracks_without_genre)}\n\n")
            
            f.write("Genre Distribution:\n")
            f.write("-" * 30 + "\n")
            for genre, tracks in sorted(genre_tracks.items(), key=lambda x: len(x[1]), reverse=True):
                f.write(f"{genre}: {len(tracks)} tracks\n")

        # Create detailed genre reports
        genre_data = {}
        for genre, tracks in genre_tracks.items():
            genre_data[genre] = tracks

        # Save as JSON for easy processing
        with open(output_dir / "genre_tracks.json", "w") as f:
            json.dump(genre_data, f, indent=2)

        # Save as CSV for each genre
        for genre, tracks in genre_tracks.items():
            df = pd.DataFrame(tracks)
            safe_genre = "".join(c for c in genre if c.isalnum() or c in (' ', '-', '_')).rstrip()
            df.to_csv(output_dir / f"{safe_genre}_tracks.csv", index=False)

        # Save tracks without genre
        if tracks_without_genre:
            df = pd.DataFrame(tracks_without_genre)
            df.to_csv(output_dir / "tracks_without_genre.csv", index=False)

        print("\nGenre Analysis Complete!")
        print(f"Results saved to: {output_dir}/")
        print(f"Found {len(genre_tracks)} unique genres")
        print(f"{len(tracks_without_genre)} tracks had no genre information")
        
        # Print top genres
        print("\nTop Genres by Track Count:")
        for genre, tracks in sorted(genre_tracks.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
            print(f"{genre}: {len(tracks)} tracks")

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 genre-split.py analysis.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)
    
    organizer = SpotifyGenreOrganizer()
    organizer.organize_by_genre(csv_path)

if __name__ == "__main__":
    main()