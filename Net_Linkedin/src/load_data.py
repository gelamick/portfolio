# src/load_data.py

import pandas as pd

def load_connections(csv_path: str) -> pd.DataFrame:
    """
    Charge et nettoie les connexions LinkedIn depuis un fichier CSV.

    Args:
        csv_path (str): Chemin vers le fichier CSV.

    Returns:
        pd.DataFrame: Le dataframe nettoyé des connexions.
    """
    df = pd.read_csv(csv_path, sep=',', encoding='utf-8-sig')

    # Nettoyage des noms de colonnes
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace('\ufeff', '')

    # Nettoyage des champs texte
    for col in ['First Name', 'Last Name', 'Company', 'Position']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Standardisation des noms d'entreprises
    company_standardization = {
        "Dell": "Dell Technologies",
        "DELL": "Dell Technologies",
        "Dell EMC": "Dell Technologies",
        "Self-employed": "Freelance",
        "Self Employed": "Freelance",
        "Indépendant": "Freelance",
        "Freelancer": "Freelance"
    }
    df['Company'] = df['Company'].replace(company_standardization)

    # Standardisation des intitulés de poste
    job_title_standardization = {
        "Owner": "Fondateur",
        "Fondatrice": "Fondatrice",
        "Founder": "Fondateur",
        "Présidente": "Président",
    }
    df['Position'] = df['Position'].replace(job_title_standardization)

    # Création de la colonne Full Name
    df['Full Name'] = df['First Name'] + ' ' + df['Last Name']

    # Conversion des dates
    df['Connected On'] = pd.to_datetime(df['Connected On'], errors='coerce')

    # Suppression des doublons pertinents
    df = df.drop_duplicates(subset=['Full Name', 'Company', 'Connected On'])

    return df
