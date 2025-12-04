import nfl_data_py as nfl
import pandas as pd
import numpy as np
import firebase_admin
from firebase_admin import credentials, firestore
import json

# --- 1. SETUP FIREBASE ---
# GitHub Actions creates this file from your secret key
cred = credentials.Certificate('serviceAccountKey.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

print("🏈 Fetching Play-by-Play Data...")
# We use 2024 data as a placeholder until the 2025 season starts
pbp = nfl.import_pbp_data([2024])

print("⚙️ Calculating Custom Efficiency Grades...")

# --- HELPER: Z-Score Normalization (0-100 Scale) ---
# This converts raw stats (like pressure rate) into a grade from 0 to 100
def calculate_grade(series, inverted=False):
    mean = series.mean()
    std = series.std()
    z_scores = (series - mean) / std
    # Invert for stats where lower is better (like Sacks Allowed)
    if inverted:
        z_scores = -z_scores
    # Map Z-Score of -2..+2 to roughly 50..100
    grades = 75 + (z_scores * 12.5) 
    return grades.clip(0, 100).round(1)

# --- 2. O-LINE RANKINGS (Pressure Rate Allowed) ---
# Group by offensive team (posteam), calculate sack + hit rate
oline_stats = pbp.groupby('posteam').agg({
    'sack': 'mean',
    'qb_hit': 'mean'
}).reset_index()
oline_stats['pressure_rate'] = oline_stats['sack'] + oline_stats['qb_hit']
oline_stats['oline_grade'] = calculate_grade(oline_stats['pressure_rate'], inverted=True)

# --- 3. D-LINE RANKINGS (Pressure Rate Generated) ---
# Group by defensive team (defteam)
dline_stats = pbp.groupby('defteam').agg({
    'sack': 'mean',
    'qb_hit': 'mean'
}).reset_index()
dline_stats['pressure_gen'] = dline_stats['sack'] + dline_stats['qb_hit']
dline_stats['dline_grade'] = calculate_grade(dline_stats['pressure_gen'], inverted=False)

# --- 4. SECONDARY RANKINGS (EPA/Pass Allowed) ---
# Filter for pass plays only
pass_defense = pbp[pbp['play_type'] == 'pass'].groupby('defteam').agg({
    'epa': 'mean'
}).reset_index()
# Lower EPA allowed is better, so we invert
pass_defense['secondary_grade'] = calculate_grade(pass_defense['epa'], inverted=True)

# --- 5. OFFENSE RANKINGS (EPA/Play) ---
# Filter for pass and run plays
offense = pbp[pbp['play_type'].isin(['pass', 'run'])].groupby('posteam').agg({
    'epa': 'mean'
}).reset_index()
offense['offense_grade'] = calculate_grade(offense['epa'], inverted=False)

# --- 6. MERGE & UPLOAD ---
print("☁️ Uploading to Firebase...")

# Merge all stats into one master table
teams = oline_stats[['posteam', 'oline_grade']].merge(
    dline_stats[['defteam', 'dline_grade']], left_on='posteam', right_on='defteam'
).merge(
    pass_defense[['defteam', 'secondary_grade']], on='defteam'
).merge(
    offense[['posteam', 'offense_grade']], on='posteam'
)

# This creates a collection called 'team_analytics' in your database
collection_ref = db.collection('team_analytics')

for index, row in teams.iterrows():
    team_id = row['posteam']
    if not team_id: continue
    
    # The data payload for each team
    data = {
        'id': team_id,
        'grades': {
            'oline': row['oline_grade'],
            'dline': row['dline_grade'],
            'secondary': row['secondary_grade'],
            'offense': row['offense_grade']
        },
        'updated_at': firestore.SERVER_TIMESTAMP
    }
    
    # Save to Firebase
    collection_ref.document(team_id).set(data, merge=True)
    print(f"   -> Updated {team_id}")

print("✅ Sync Complete!")
