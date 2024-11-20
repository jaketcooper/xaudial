import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
import sys
import os
from pathlib import Path
from tqdm import tqdm
import json

def create_flow_playlist(csv_path: str):
    # Load and validate the CSV
    try:
        df = pd.read_csv(csv_path)
        # Check minimum required columns
        required_columns = {'id', 'name', 'artists'}
        if not all(col in df.columns for col in required_columns):
            print("Error: CSV file missing required columns (id, name, artists)")
            sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)

    # Check if we should filter by criteria
    if 'meets_criteria' in df.columns:
        matching_tracks = df[df['meets_criteria'] == True]
        print(f"Found {len(matching_tracks)} tracks that meet flow state criteria")
    else:
        matching_tracks = df
        print(f"No criteria column found - using all {len(matching_tracks)} tracks")
    
    if matching_tracks.empty:
        print("No tracks found to add to playlist!")
        sys.exit(1)

    # Initialize Spotify client with OAuth
    try:
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id="3226c7189a0b403c9daf846e26cd1221",
            client_secret="1103f3318e9e4ff7a3f1ecdcb397f52e",
            redirect_uri="http://3.213.192.126/callback",
            scope="playlist-modify-public playlist-modify-private"
        ))
        
        # Get current user's info
        user_info = sp.current_user()
        user_id = user_info['id']
        
    except Exception as e:
        print(f"Error authenticating with Spotify: {e}")
        sys.exit(1)

    # Create playlist name based on CSV filename and timestamp
    csv_name = Path(csv_path).stem
    timestamp = datetime.now().strftime('%m-%d-%y %H.%M.%S')
    playlist_name = f"Flow - {csv_name} - {timestamp}"
    
    try:
        playlist = sp.user_playlist_create(
            user=user_id,
            name=playlist_name,
            public=False,
            description=f"Tracks from {csv_name}"
        )
        
        print(f"\nCreated playlist: {playlist_name}")
        
        # Add tracks in batches
        track_ids = matching_tracks['id'].tolist()
        batch_size = 100  # Spotify API limit
        
        for i in tqdm(range(0, len(track_ids), batch_size), desc="Adding tracks"):
            batch = track_ids[i:i + batch_size]
            sp.playlist_add_items(playlist['id'], batch)
        
        # Generate success report
        print("\nPlaylist Creation Summary:")
        print("-" * 50)
        print(f"Playlist Name: {playlist_name}")
        print(f"Total Tracks Added: {len(track_ids)}")
        print(f"Playlist URL: {playlist['external_urls']['spotify']}")
        
        # Save detailed report
        report_dir = Path("flow_playlists")
        report_dir.mkdir(exist_ok=True)
        
        with open(report_dir / f"playlist_report_{timestamp}.txt", "w") as f:
            f.write(f"Playlist Creation Report\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Playlist Name: {playlist_name}\n")
            f.write(f"Source File: {csv_path}\n")
            f.write(f"Created: {timestamp}\n")
            f.write(f"Total Tracks: {len(track_ids)}\n")
            f.write(f"Playlist URL: {playlist['external_urls']['spotify']}\n\n")
            f.write("Tracks Added:\n")
            f.write("-" * 50 + "\n")
            
            for _, track in matching_tracks.iterrows():
                f.write(f"• {track['name']} by {track['artists']}\n")
                if 'tempo' in track:
                    f.write(f"  Tempo: {track['tempo']:.1f} BPM")
                if 'energy' in track:
                    f.write(f", Energy: {track['energy']:.2f}")
                if 'loudness' in track:
                    f.write(f", Loudness: {track['loudness']:.1f} dB")
                f.write("\n")
        
        print(f"\nDetailed report saved to: {report_dir}/playlist_report_{timestamp}.txt")
        
    except Exception as e:
        print(f"Error creating playlist: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 script.py analysis.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)
    
    create_flow_playlist(csv_path)

if __name__ == "__main__":
    main()