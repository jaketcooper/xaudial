import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time
from typing import List, Dict, Any, Set, Tuple
import logging
from dataclasses import dataclass
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress all spotipy related warnings
logging.getLogger('spotipy').setLevel(logging.ERROR)

@dataclass
class FlowStateThresholds:
    min_tempo: float = 140.0
    max_tempo: float = 900.0
    min_loudness: float = -7.0
    max_loudness: float = 0
    min_energy: float = 0.85
    min_beat_confidence: float = 0.9
    min_section_confidence: float = 0.8
    min_section_duration: float = 20.0
    mode: int = 1

class SpotifyFlowAnalyzer:
    def __init__(self, client_id: str, client_secret: str):
        """Initialize the Spotify client"""
        self.sp = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret,
                cache_handler=None
            )
        )
        self.thresholds = FlowStateThresholds()
        self.batch_size = 50

    def get_playlist_tracks(self, playlist_id: str) -> List[Dict]:
        """Fetch all tracks from a playlist efficiently"""
        tracks = []
        offset = 0
        limit = 100
        
        while True:
            try:
                results = self.sp.playlist_tracks(
                    playlist_id,
                    offset=offset,
                    limit=limit,
                    fields="items(track(id,name,artists(name))),total"
                )
                
                if not results['items']:
                    break
                
                tracks.extend(results['items'])
                offset += limit
                
                if len(results['items']) < limit:
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching tracks from playlist {playlist_id}: {e}")
                time.sleep(2)
                continue
                
        return tracks

    def collect_all_tracks(self, playlist_ids: List[str]) -> Tuple[List[str], Dict]:
        """Collect all unique track IDs and their metadata from multiple playlists"""
        logger.info("Collecting tracks from all playlists...")
        all_track_ids = set()
        track_metadata = {}
        playlist_info = {}  # Store playlist names for reporting
        
        for playlist_id in tqdm(playlist_ids, desc="Processing playlists"):
            playlist_id = playlist_id.strip()
            if not playlist_id:
                continue
            
            try:
                # Get playlist name
                playlist_details = self.sp.playlist(playlist_id, fields='name')
                playlist_info[playlist_id] = playlist_details['name']
                playlist_track_count = 0  # Track count for this playlist
                
                # Get tracks
                tracks = self.get_playlist_tracks(playlist_id)
                
                # Process valid tracks
                for item in tracks:
                    if item['track'] and item['track']['id']:
                        track = item['track']
                        track_id = track['id']
                        playlist_track_count += 1
                        all_track_ids.add(track_id)
                        
                        # Store track metadata if we haven't seen it before
                        if track_id not in track_metadata:
                            track_metadata[track_id] = {
                                'name': track['name'],
                                'artists': ", ".join(artist['name'] for artist in track['artists']),
                                'playlists': []
                            }
                        track_metadata[track_id]['playlists'].append(playlist_info[playlist_id])
                
                logger.info(f"Found {playlist_track_count} tracks in playlist: {playlist_info[playlist_id]}")
                
            except Exception as e:
                logger.error(f"Error processing playlist {playlist_id}: {e}")
                continue
        
        unique_tracks = len(all_track_ids)
        if unique_tracks == 0:
            logger.warning("No valid tracks found in any of the playlists!")
            return [], {}
            
        logger.info(f"Found {unique_tracks} unique tracks across {len(playlist_ids)} playlists")
        return list(all_track_ids), track_metadata

    def get_audio_features_batch(self, track_ids: List[str]) -> List[Dict]:
        """Get audio features for a batch of tracks"""
        try:
            return self.sp.audio_features(track_ids)
        except Exception as e:
            logger.error(f"Error fetching audio features: {e}")
            time.sleep(2)
            return None

    def analyze_tracks(self, track_ids: List[str], track_metadata: Dict) -> pd.DataFrame:
        """Analyze audio features for all tracks"""
        results = []
        track_batches = [track_ids[i:i + self.batch_size] 
                        for i in range(0, len(track_ids), self.batch_size)]
        
        with tqdm(total=len(track_ids), desc="Analyzing tracks") as pbar:
            for batch in track_batches:
                features_batch = self.get_audio_features_batch(batch)
                if not features_batch:
                    continue
                
                for track_id, features in zip(batch, features_batch):
                    if not features:
                        continue
                    
                    metadata = track_metadata[track_id]
                    
                    # Check if track meets flow state criteria
                    meets_criteria = True
                    reasons = []
                    
                    if not (self.thresholds.min_tempo <= features['tempo'] <= self.thresholds.max_tempo):
                        meets_criteria = False
                        reasons.append(f"Tempo: {features['tempo']:.1f} BPM")
                    
                    if not (self.thresholds.min_loudness <= features['loudness'] <= self.thresholds.max_loudness):
                        meets_criteria = False
                        reasons.append(f"Loudness: {features['loudness']:.1f} dB")
                    
                    if features['energy'] < self.thresholds.min_energy:
                        meets_criteria = False
                        reasons.append(f"Energy: {features['energy']:.2f}")
                    
                    if features['mode'] != self.thresholds.mode:
                        meets_criteria = False
                        reasons.append("Minor mode")
                    
                    results.append({
                        "name": metadata['name'],
                        "artists": metadata['artists'],
                        "id": track_id,
                        "playlists": "; ".join(metadata['playlists']),
                        "meets_criteria": meets_criteria,
                        "reasons": "; ".join(reasons) if reasons else "All criteria met",
                        "tempo": features['tempo'],
                        "loudness": features['loudness'],
                        "energy": features['energy'],
                        "mode": features['mode']
                    })
                    
                    pbar.update(1)
        
        return pd.DataFrame(results)

    def generate_report(self, df: pd.DataFrame, output_dir: Path):
        """Generate analysis report"""
        with open(output_dir / "analysis_report.txt", "w") as f:
            f.write("Flow State Analysis Report\n")
            f.write("=" * 50 + "\n\n")
            
            total_tracks = len(df)
            meeting_criteria = df['meets_criteria'].sum()
            
            f.write("Overall Statistics:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Total tracks analyzed: {total_tracks}\n")
            f.write(f"Tracks meeting criteria: {meeting_criteria} ({(meeting_criteria/total_tracks)*100:.1f}%)\n")
            f.write(f"Tracks not meeting criteria: {total_tracks - meeting_criteria}\n\n")
            
            f.write("Tracks Meeting All Criteria:\n")
            f.write("-" * 30 + "\n")
            for _, row in df[df['meets_criteria']].iterrows():
                f.write(f"• {row['name']} by {row['artists']}\n")
                f.write(f"  Found in playlists: {row['playlists']}\n")
                f.write(f"  Tempo: {row['tempo']:.1f} BPM, Loudness: {row['loudness']:.1f} dB, Energy: {row['energy']:.2f}\n\n")
            
            f.write("\nTracks Not Meeting Criteria:\n")
            f.write("-" * 30 + "\n")
            for _, row in df[~df['meets_criteria']].iterrows():
                f.write(f"• {row['name']} by {row['artists']}\n")
                f.write(f"  Found in playlists: {row['playlists']}\n")
                f.write(f"  Issues: {row['reasons']}\n\n")

def get_timestamped_dir(base_dir: Path) -> Path:
    """Create and return a timestamped directory"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = base_dir / timestamp
    output_dir.mkdir(exist_ok=True)
    return output_dir

def main():
    CLIENT_ID = "3226c7189a0b403c9daf846e26cd1221"
    CLIENT_SECRET = "1103f3318e9e4ff7a3f1ecdcb397f52e"
    
    try:
        analyzer = SpotifyFlowAnalyzer(CLIENT_ID, CLIENT_SECRET)
        playlist_ids = []
        
        # Check for direct command line IDs first
        if len(sys.argv) > 1 and not os.path.isfile(sys.argv[1]):
            playlist_ids = sys.argv[1:]
        # Check for file input
        elif len(sys.argv) > 1 and os.path.isfile(sys.argv[1]):
            with open(sys.argv[1], 'r') as f:
                playlist_ids = f.readlines()
        # Check for piped input
        elif not sys.stdin.isatty():
            playlist_ids = sys.stdin.readlines()
        else:
            print("Error: Please provide playlist IDs using one of these methods:")
            print("1. Direct IDs: python3 app.py ID1 ID2 ID3")
            print("2. File input: python3 app.py playlists.txt")
            print("3. Piped input: cat playlists.txt | python3 app.py")
            sys.exit(1)
        
        # Clean IDs
        playlist_ids = [pid.strip() for pid in playlist_ids if pid.strip()]
        
        if not playlist_ids:
            print("Error: No valid playlist IDs provided")
            sys.exit(1)
        
        # Create base directory
        base_dir = Path('spotify_analysis')
        base_dir.mkdir(exist_ok=True)
        
        # Create timestamped directory for this run
        output_dir = get_timestamped_dir(base_dir)
        
        # Collect all unique tracks
        track_ids, track_metadata = analyzer.collect_all_tracks(playlist_ids)
        
        # Analyze tracks
        df = analyzer.analyze_tracks(track_ids, track_metadata)
        
        # Save results in timestamped directory
        df.to_csv(output_dir / "analysis.csv", index=False)
        df.to_excel(output_dir / "analysis.xlsx", index=False)
        
        # Generate report
        analyzer.generate_report(df, output_dir)
        
        # Save playlist IDs that were analyzed
        with open(output_dir / "analyzed_playlists.txt", "w") as f:
            for playlist_id in playlist_ids:
                f.write(f"{playlist_id}\n")
        
        print("\nAnalysis Complete!")
        print(f"Results saved to {output_dir}/")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise