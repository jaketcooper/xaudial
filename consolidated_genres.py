import json
import pandas as pd
from pathlib import Path
import sys
from typing import Dict, Set

class GenreConsolidator:
    def __init__(self):
        # Define genre mappings
        self.genre_mappings = {
            'ELECTRONIC': {
                'brostep', 'dubstep', 'drum and bass', 'liquid funk', 'dancefloor dnb', 'uk dnb',
                'edm', 'complextro', 'electro house', 'filthstep', 'chillstep', 'gaming edm',
                'deathstep', 'future funk', 'uk dance', 'pop edm', 'stateside dnb', 'darksynth',
                'danish electronic', 'progressive house', 'synthpop', 'bass trap', 'canadian electronic',
                'hardcore techno', 'bass house', 'jump up', 'deep liquid', 'deep dnb', 'austrian dnb',
                'drift phonk', 'gym phonk', 'dutch edm', 'glitch', 'hard trance', 'progressive trance',
                'trance', 'uplifting trance', 'uk house', 'electronic trap', 'riddim dubstep',
                'fidget house', 'synthwave', 'hardwave', 'future bass', 'nz electronic', 'memphis phonk',
                'phonk brasileiro', 'progressive electro house', 'vocal house', 'old school bassline',
                'modern jungle', 'aussietronica', 'neurostep', 'brazilian dnb', 'jazzy dnb', 'sambass',
                'ragga jungle', 'belgian dnb', 'big room', 'slap house', 'trance mexicano',
                'moombahton', 'drumfunk', 'eurodance', 'europop', 'italo dance', 'speedrun',
                'gaming dubstep', 'jungle', 'neurofunk', 'dark clubbing'
            },
            'ROCK_METAL': {
                'rock', 'alternative rock', 'christian rock', 'punk', 'hard rock', 'future rock',
                'classic rock', 'permanent wave', 'new romantic', 'new wave', 'album rock',
                'mellow gold', 'soft rock', 'alternative metal', 'doom metal', 'epic doom',
                'swedish doom metal', 'early us punk', 'hardcore punk', 'dance rock', 'post-punk',
                'new wave pop', 'heartland rock', 'singer-songwriter', 'metal', 'milwaukee indie',
                'j-rock', 'visual kei', 'pop punk', 'ska', 'skate punk', 'socal pop punk',
                'madchester', 'uk post-punk', 'art punk', 'synth punk', 'dance-punk', 'el paso indie',
                'emo', 'post-hardcore', 'glam rock', 'country rock', 'folk rock', 'old school thrash',
                'thrash metal', 'nu metal', 'speed metal', 'pop emo', 'christian alternative rock'
            },
            'JAPANESE_ANIME': {
                'vocaloid', 'anime', 'japanese progressive house', 'otacore', 'j-pixie', 'anime rock',
                'kawaii future bass', 'j-pop', 'kawaii edm', 'denpa-kei', 'japanese vgm',
                'j-core', 'japanese vtuber', 'anime score', 'j-metal', 'seinen',
                'japanese soundtrack', 'japanese classical', 'japanese celtic', 'seiyu',
                'vocaloid metal', 'anime rap', 'touhou', 'pixie'
            }
        }

    def get_main_genre(self, subgenre: str) -> str:
        """Determine which main genre a subgenre belongs to"""
        subgenre = subgenre.lower()
        for main_genre, subgenres in self.genre_mappings.items():
            if subgenre in subgenres:
                return main_genre
        return "OTHER"

    def consolidate_genres(self, json_path: str):
        """Consolidate tracks from genre_tracks.json into main genres"""
        try:
            # Load the JSON file
            with open(json_path, 'r') as f:
                genre_data = json.load(f)

            # Create dictionaries for main genres
            main_genre_tracks = {
                'ELECTRONIC': [],
                'ROCK_METAL': [],
                'JAPANESE_ANIME': [],
                'OTHER': []
            }

            # Track IDs we've seen to avoid duplicates
            seen_tracks = {genre: set() for genre in main_genre_tracks.keys()}

            # Reorganize tracks into main genres
            for subgenre, tracks in genre_data.items():
                main_genre = self.get_main_genre(subgenre)
                
                for track in tracks:
                    if track['id'] not in seen_tracks[main_genre]:
                        main_genre_tracks[main_genre].append(track)
                        seen_tracks[main_genre].add(track['id'])

            # Create output directory
            output_dir = Path('consolidated_genres')
            output_dir.mkdir(exist_ok=True)

            # Save consolidated CSVs
            for genre, tracks in main_genre_tracks.items():
                if tracks:  # Only create files for genres with tracks
                    df = pd.DataFrame(tracks)
                    csv_path = output_dir / f"{genre.lower()}_tracks.csv"
                    df.to_csv(csv_path, index=False)
                    print(f"Created {genre} CSV with {len(tracks)} tracks")

            # Generate summary report
            with open(output_dir / "consolidation_report.txt", "w") as f:
                f.write("Genre Consolidation Report\n")
                f.write("=" * 50 + "\n\n")
                
                for genre, tracks in main_genre_tracks.items():
                    f.write(f"{genre}: {len(tracks)} tracks\n")
                    
                f.write("\nFiles created:\n")
                for genre in main_genre_tracks:
                    if main_genre_tracks[genre]:
                        f.write(f"- {genre.lower()}_tracks.csv\n")

        except Exception as e:
            print(f"Error processing genre data: {e}")
            raise

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 consolidate_genres.py path/to/genre_tracks.json")
        sys.exit(1)

    json_path = sys.argv[1]
    if not Path(json_path).exists():
        print(f"Error: File not found: {json_path}")
        sys.exit(1)

    consolidator = GenreConsolidator()
    consolidator.consolidate_genres(json_path)

if __name__ == "__main__":
    main()