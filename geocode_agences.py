import os
import time
import re
import pandas as pd
from geopy.geocoders import Nominatim
from pandas.errors import EmptyDataError

# === chemins ===
input_file = os.path.join(os.path.dirname(__file__), 'scraper', 'agences-ce_merged_pb.csv')
output_dir = os.path.join(os.path.dirname(__file__), 'scraper', 'agences-ce_merged_pb_geocoded')
os.makedirs(output_dir, exist_ok=True)
out_path = os.path.join(output_dir, os.path.basename(input_file))

# === geocoder ===
geolocator = Nominatim(user_agent="agences_geocoder_bp_cm", timeout=10)

# === helpers ===
CEDEX_PAT = re.compile(r'\b(cdex|cedex)\b\s*\d*', re.IGNORECASE)
CS_PAT    = re.compile(r'\bC(?:\.|\s*)S\b\s*\d+', re.IGNORECASE)

def clean_adresse(addr: str) -> str:
    if not isinstance(addr, str):
        return ""
    s = addr.strip()
    s = CEDEX_PAT.sub('', s)
    s = CS_PAT.sub('', s)
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r'\s*,\s*', ', ', s)
    return s.strip(' ,')

def build_query(adresse: str, code_postal) -> str:
    cp = ""
    if pd.notna(code_postal):
        m = re.search(r'(\d{4,5})', str(code_postal))
        if m:
            cp = m.group(1).zfill(5)
    parts = [p for p in [clean_adresse(adresse), cp, "France"] if p]
    return ', '.join(parts)

def geocode(address: str):
    if not address:
        return None
    try:
        return geolocator.geocode(address, exactly_one=True, country_codes='fr')
    except Exception:
        return None

# === lecture ===
try:
    if os.path.exists(out_path):
        # Si un fichier partiellement géocodé existe, on le reprend
        df = pd.read_csv(out_path)
        print(f"Reprise depuis {out_path}")
    else:
        df = pd.read_csv(input_file)
except EmptyDataError:
    print(f"Fichier vide : {input_file}")
    raise SystemExit(1)

# s'assurer des colonnes
for col in ('latitude', 'longitude'):
    if col not in df.columns:
        df[col] = None
for col in ('nom', 'adresse', 'code_postal'):
    if col not in df.columns:
        df[col] = ""

total = df.shape[0]

# boucle ligne par ligne
for idx, row in df.iterrows():
    # saute si déjà géocodé
    if pd.notna(row['latitude']) and pd.notna(row['longitude']):
        print(f"[{idx+1}/{total}] {row['nom']} - déjà géocodé")
        continue

    query = build_query(row.get('adresse', ''), row.get('code_postal', ''))
    loc = geocode(query)

    if not loc:
        # fallback ville
        addr = str(row.get('adresse', ''))
        segs = [s.strip() for s in addr.split(',') if s.strip()]
        if len(segs) >= 1:
            city_guess = segs[-1]
            loc = geocode(f"{city_guess}, France")
            if loc:
                print(f"[{idx+1}/{total}] {row['nom']} - Fallback ville pour : {city_guess}")

    if loc:
        df.at[idx, 'latitude'] = loc.latitude
        df.at[idx, 'longitude'] = loc.longitude
        print(f"[{idx+1}/{total}] {row['nom']} - OK ({loc.latitude}, {loc.longitude})")
    else:
        print(f"[{idx+1}/{total}] {row['nom']} - ❌ Pas de résultat pour : {query}")

    # Écrit le CSV à chaque ligne
    df.to_csv(out_path, index=False, encoding='utf-8')
    time.sleep(1)  # rate limit

# colonnes finales
if 'region_url' in df.columns and 'region_source' not in df.columns:
    df.rename(columns={'region_url': 'region_source'}, inplace=True)

print(f"\n✅ Fichier créé/mis à jour en continu : {out_path} ({df.shape[0]} lignes)")
