#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Remplit latitude/longitude à partir du code postal.

Entrée (ex) :
nom,adresse,code_postal,latitude,longitude,region_source
ARLYSERE UGINE,"75, Pl du Val d'Arly, nan, UGINE",73400,,,nan

Référentiel requis : fr_postcodes.csv avec au minimum :
code_postal,lat,lon
73400,45.7540,6.4230
...

Usage :
  python fill_latlon_from_cp.py --input agences.csv --postcodes fr_postcodes.csv --out agences_out.csv
"""

import argparse
import pandas as pd
import re

def zfill5(x):
    """Garde 5 chiffres du code postal (ex: '73400', '7508'->'07508')."""
    if pd.isna(x):
        return ""
    s = str(x).strip()
    m = re.search(r"(\d{4,5})", s)
    return m.group(1).zfill(5) if m else ""

def load_postcodes(path):
    """Charge le référentiel CP -> lat/lon (colonnes exigées : code_postal, lat, lon)."""
    ref = pd.read_csv(path)
    # normalise noms possibles
    col_map = {
        "code_postal": "code_postal",
        "cp": "code_postal",
        "postcode": "code_postal",
        "lat": "lat",
        "latitude": "lat",
        "lon": "lon",
        "lng": "lon",
        "longitude": "lon",
    }
    ref = ref.rename(columns={k: v for k, v in col_map.items() if k in ref.columns})
    required = {"code_postal", "lat", "lon"}
    missing = required - set(ref.columns)
    if missing:
        raise ValueError(f"Colonnes manquantes dans {path}: {missing}")
    ref["code_postal"] = ref["code_postal"].apply(zfill5)
    return ref[["code_postal", "lat", "lon"]]

def main():
    ap = argparse.ArgumentParser(description="Remplir latitude/longitude à partir du code postal.")
    ap.add_argument("--input", required=True, help="CSV d'entrée (avec code_postal)")
    ap.add_argument("--postcodes", required=True, help="CSV référentiel (code_postal, lat, lon)")
    ap.add_argument("--out", required=True, help="CSV de sortie")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    # s'assure des colonnes
    for c in ("latitude", "longitude"):
        if c not in df.columns:
            df[c] = pd.NA
    if "code_postal" not in df.columns:
        raise ValueError("Colonne 'code_postal' absente du CSV d'entrée.")

    # normalise CP
    df["code_postal"] = df["code_postal"].apply(zfill5)

    # charge référentiel
    ref = load_postcodes(args.postcodes)

    # merge et remplissage
    merged = df.merge(ref, on="code_postal", how="left", suffixes=("", "_ref"))
    merged["latitude"] = merged["latitude"].fillna(merged["lat"])
    merged["longitude"] = merged["longitude"].fillna(merged["lon"])

    # drop colonnes techniques
    merged = merged.drop(columns=[c for c in ("lat", "lon") if c in merged.columns])

    merged.to_csv(args.out, index=False, encoding="utf-8")
    print(f"[OK] Écrit : {args.out} ({merged.shape[0]} lignes)")

if __name__ == "__main__":
    main()
