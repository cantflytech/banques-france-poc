import os
import time
import pandas as pd
import re
from geopy.geocoders import Nominatim
from pandas.errors import EmptyDataError
from tqdm import tqdm

input_dir = os.path.join(os.path.dirname(__file__), 'scraper', 'agences_cm')
output_dir = os.path.join(os.path.dirname(__file__), 'scraper', 'agences_cm_geocoded')
os.makedirs(output_dir, exist_ok=True)

geolocator = Nominatim(user_agent="agences_geocoder", timeout=10)

def prepare_address(addr):
    if not addr or not isinstance(addr, str):
        return None
    addr_clean = addr.strip()
    addr_clean = re.sub(r"\bR\b", "Rue", addr_clean, flags=re.IGNORECASE)
    addr_clean = re.sub(r"\bPL\b", "Place", addr_clean, flags=re.IGNORECASE)
    addr_clean = re.sub(r"\s+", " ", addr_clean)
    parts = [p.strip().title() for p in addr_clean.split(',') if p.strip()]
    if not any('france' in p.lower() for p in parts):
        parts.append('France')
    return parts

def address_from_parts(parts):
    return ', '.join(parts)

def geocode_address(address):
    try:
        return geolocator.geocode(address, exactly_one=True, country_codes='fr')
    except Exception:
        return None

for filename in os.listdir(input_dir):
    if not filename.lower().endswith('.csv'):
        continue

    in_path = os.path.join(input_dir, filename)
    out_path = os.path.join(output_dir, filename)

    if os.path.exists(out_path):
        print(f"Ignoré (déjà traité) : {filename}")
        continue

    if os.path.getsize(in_path) == 0:
        print(f"Fichier vide, ignoré : {filename}")
        continue

    try:
        df = pd.read_csv(in_path)
    except EmptyDataError:
        print(f"Pas de données dans : {filename}, ignoré.")
        continue

    # Filtrage des lignes valides (avec une adresse correcte et un vrai nom)
    df = df[df['adresse'].notna() & df['adresse'].str.contains(r'\d{5}', na=False)]

    # Ajout colonnes lat/lon si manquantes
    for col in ('latitude', 'longitude'):
        if col not in df.columns:
            df[col] = None

    for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Géocodage {filename}", unit="ligne"):

        if pd.isna(row['latitude']) or pd.isna(row['longitude']):
            raw = row.get('adresse')
            parts = prepare_address(raw)
            if not parts:
                print(f"Adresse invalide ligne {idx} dans {filename}")
                continue

            full_addr = address_from_parts(parts)
            loc = geocode_address(full_addr)
            if not loc:
                if len(parts) >= 2:
                    city = parts[-2] + ', France'
                    loc = geocode_address(city)
                    if loc:
                        print(f"Fallback ville pour : {city}")
                if not loc:
                    print(f"Pas de résultat pour : {full_addr} et fallback")
                    continue

            df.at[idx, 'latitude'] = loc.latitude
            df.at[idx, 'longitude'] = loc.longitude
            time.sleep(1)

    # Renommer region_url → region_source
    if 'region_url' in df.columns:
        df.rename(columns={'region_url': 'region_source'}, inplace=True)

    # Filtrer les colonnes finales souhaitées
    final_cols = ['nom', 'adresse', 'code_postal', 'latitude', 'longitude', 'region_source']
    df = df[final_cols]

    df.to_csv(out_path, index=False)
    print(f"Fichier créé : {out_path}")

print("✅ Géocodage terminé pour toutes les agences Crédit Mutuel !")
