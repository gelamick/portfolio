import pandas as pd

def filter_by_year(df, year):
    """
    Garde uniquement les connexions d'une année donnée.
    Args:
        df (pd.DataFrame): DataFrame complet.
        year (int): Année (ex : 2024).
    Returns:
        pd.DataFrame: filtré.
    """
    df['Connected On'] = pd.to_datetime(df['Connected On'])
    return df[df['Connected On'].dt.year == year]

def filter_by_company(df, company_name):
    """
    Garde uniquement les personnes d'une entreprise spécifique.
    Args:
        df (pd.DataFrame): DataFrame complet.
        company_name (str): Nom de l'entreprise à filtrer.
    Returns:
        pd.DataFrame: filtré.
    """
    return df[df['Company'].str.contains(company_name, case=False, na=False)]
