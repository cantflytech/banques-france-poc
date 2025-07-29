import os
import time
import pandas as pd
import re
from geopy.geocoders import Nominatim
from pandas.errors import EmptyDataError

# Répertoires d'entrée et de sortie
input_dir = os.path.join(os.path.dirname(__file__), 'scraper', 'agences_sg')
output_dir = os.path.join(os.path.dirname(__file__), 'scraper', 'agences_sg_geocoded')

# Crée le dossier de sortie si nécessaire
os.makedirs(output_dir, exist_ok=True)

# Initialise le géocodeur avec un biais France
geolocator = Nominatim(user_agent="agences_geocoder", timeout=10)

# Prépare l'adresse pour améliorer le taux de réussite
import re
def prepare_address(addr):
    if not addr or not isinstance(addr, str):
        return None
    addr_clean = addr.strip()
    # Remplace les abréviations ' R ' et ' PL '
    addr_clean = re.sub(r"\bR\b", "Rue", addr_clean, flags=re.IGNORECASE)
    addr_clean = re.sub(r"\bPL\b", "Place", addr_clean, flags=re.IGNORECASE)
    addr_clean = re.sub(r"\s+", " ", addr_clean)
    parts = [p.strip().title() for p in addr_clean.split(',') if p.strip()]
    if not any('france' in p.lower() for p in parts):
        parts.append('France')
    return parts

# Transforme les parts en string d'adresse
def address_from_parts(parts):
    return ', '.join(parts)

# Géocode l'adresse ciblée sur la France
def geocode_address(address):
    try:
        return geolocator.geocode(address, exactly_one=True, country_codes='fr')
    except Exception:
        return None

# Parcours tous les CSV dans agences_sg
for filename in os.listdir(input_dir):
    if not filename.lower().endswith('.csv'):
        continue

    in_path = os.path.join(input_dir, filename)
    out_path = os.path.join(output_dir, filename)

    # Skip si déjà traité
    if os.path.exists(out_path):
        print(f"Ignoré (déjà traité) : {filename}")
        continue

    # Skip si vide ou sans données
    if os.path.getsize(in_path) == 0:
        print(f"Fichier vide, ignoré : {filename}")
        continue

    # Lecture du CSV
    try:
        df = pd.read_csv(in_path)
    except EmptyDataError:
        print(f"Pas de données dans : {filename}, ignoré.")
        continue

    # Assure colonnes latitude/longitude
    for col in ('latitude', 'longitude'):
        if col not in df.columns:
            df[col] = None

    # Géocode chaque ligne manquante
    for idx, row in df.iterrows():
        if pd.isna(row['latitude']) or pd.isna(row['longitude']):
            raw = row.get('adresse') or row.get('Adresse')
            parts = prepare_address(raw)
            if not parts:
                print(f"Adresse invalide ligne {idx} dans {filename}")
                continue

            # Essai sur adresse complète
            full_addr = address_from_parts(parts)
            loc = geocode_address(full_addr)
            if not loc:
                # Fallback sur ville
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

    # Sauvegarde du CSV mis à jour
    df.to_csv(out_path, index=False)
    print(f"Fichier créé : {out_path}")

print("Géocodage terminé pour toutes les agences SG !")