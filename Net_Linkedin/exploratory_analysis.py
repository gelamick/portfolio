# exploratory_analysis.ipynb

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import logging

# Configurer le logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Fonction d'analyse exploratoire des données et de génération de recommandations.
    Enregistre les graphiques sous forme d'images et affiche les graphiques si possible.
    
    Args:
        output_dir (str): Répertoire où les images des graphiques seront enregistrées.
    
    Affiche les informations et fournit des recommandations basées sur les résultats de l'analyse.
    """

    # Vérifier si le fichier CSV existe
    if not os.path.exists('data/connections.csv'):
        logger.error("Le fichier CSV 'connections.csv' est introuvable dans le répertoire 'data'.")
        return

    # Charger les données
    df = pd.read_csv('data/connections.csv', sep=',', encoding='utf-8-sig')

    # Normalisation des noms d'entreprises
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

    # Normalisation des postes d'entreprises
    job_title_standardization = {
        "Owner": "Fondateur",
        "Fondatrice": "Fondatrice",
        "Founder": "Fondateur",
        "Présidente": "Président",
    }
    
    df['Position'] = df['Position'].replace(job_title_standardization)

    # Créer le répertoire de sortie si nécessaire
    output_dir = 'output_images'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # --- Aperçu des données ---
    logger.info("Aperçu des données :")
    logger.info(df.head())

    # --- Informations générales ---
    logger.info("\nInformations sur le dataset :")
    logger.info(df.info())

    # --- Statistiques descriptives ---
    logger.info("\nStatistiques descriptives :")
    logger.info(df.describe(include='all'))

    # --- Valeurs manquantes ---
    logger.info("\nValeurs manquantes par colonne :")
    logger.info(df.isna().sum())

    # Convertir la colonne 'Connected On' en datetime et gérer les erreurs
    df['Connected On'] = pd.to_datetime(df['Connected On'], errors='coerce')

    # Vérifier si la conversion a bien eu lieu (afficher les premières lignes après conversion)
    logger.info("Exemple de dates après conversion :")
    logger.info(df['Connected On'].head())

    # Vérifier si des valeurs sont manquantes après conversion
    missing_dates = df['Connected On'].isna().sum()
    logger.info(f"Nombre de dates manquantes après conversion : {missing_dates}")

    # --- Créer la colonne 'Year' après conversion ---
    df['Year'] = df['Connected On'].dt.year

    # --- Connexions par année ---
    plt.figure(figsize=(10, 5))
    sns.countplot(x='Year', data=df, hue='Year', palette='Blues', legend=False)  # Ajout du 'hue'
    plt.title("Nombre de connexions par année")
    plt.xticks(rotation=45)
    plt.tight_layout()
    # Enregistrer l'image
    plt.savefig(f"{output_dir}/connections_by_year.png")
    plt.show()

    # --- Top entreprises ---
    top_companies = df['Company'].value_counts().head(10)
    plt.figure(figsize=(10, 5))
    sns.barplot(x=top_companies.values, y=top_companies.index, hue=top_companies.index, palette='Greens', legend=False)  # Ajout du 'hue'
    plt.title("Top 10 des entreprises dans les connexions")
    plt.xlabel("Nombre de connexions")
    plt.ylabel("Entreprise")
    plt.tight_layout()
    # Enregistrer l'image
    plt.savefig(f"{output_dir}/top_companies.png")
    plt.show()

    # --- Top positions ---
    top_positions = df['Position'].value_counts().head(10)
    plt.figure(figsize=(10, 5))
    sns.barplot(x=top_positions.values, y=top_positions.index, hue=top_positions.index, palette='Purples', legend=False)  # Ajout du 'hue'
    plt.title("Top 10 des postes dans les connexions")
    plt.xlabel("Nombre de connexions")
    plt.ylabel("Poste")
    plt.tight_layout()
    # Enregistrer l'image
    plt.savefig(f"{output_dir}/top_positions.png")
    plt.show()

    # --- Doublons ---
    logger.info("\nNombre de doublons exacts : %d", df.duplicated().sum())
    
    # --- Recommandations basées sur l'analyse ---
    logger.info("\nRecommandations basées sur l'analyse :")

    # Recommandations pour les valeurs manquantes
    missing_values = df.isna().sum()
    if missing_values.any():
        logger.info("- Il existe des valeurs manquantes dans certaines colonnes (comme 'Email Address', 'Company', 'Position'). Il est recommandé de les traiter avant toute analyse plus approfondie.")
    else:
        logger.info("- Aucun problème de valeurs manquantes détecté.")

    # Recommandations pour les doublons
    if df.duplicated().sum() > 0:
        logger.info("- Des doublons exacts ont été trouvés. Il est recommandé de les supprimer pour éviter la redondance dans l'analyse.")
    else:
        logger.info("- Aucun doublon exact trouvé.")

    # Recommandations sur les connexions par année
    recent_years = df['Year'].max()
    if recent_years > 2020:
        logger.info(f"- Vous avez des connexions récentes jusqu'à l'année {recent_years}. Il peut être intéressant d'analyser les tendances récentes de votre réseau.")
    else:
        logger.info(f"- Les connexions s'arrêtent en {recent_years}. Vous pouvez essayer d'actualiser les données pour avoir un aperçu plus actuel de votre réseau.")

    # Recommandations sur les entreprises et postes
    if top_companies.empty:
        logger.info("- Aucun groupe d'entreprise distinct trouvé dans vos connexions. Vous pouvez envisager d'enrichir votre réseau avec des connexions dans d'autres entreprises.")
    else:
        logger.info("- Les entreprises les plus fréquentes sont bien représentées, vous pourriez cibler davantage d'entreprises qui ne sont pas encore dans votre réseau.")
    
    if top_positions.empty:
        logger.info("- Aucun groupe de postes distinct trouvé dans vos connexions. Vous pouvez cibler des profils de postes différents pour étendre votre réseau.")
    else:
        logger.info("- Les postes les plus fréquents sont dans votre réseau, vous pourriez explorer des opportunités dans d'autres domaines ou niveaux de responsabilité.")

    # Recommandations sur les connexions spécifiques
    if df['Connected On'].isnull().sum() > 0:
        logger.info("- Certaines connexions n'ont pas de date de connexion. Il est possible qu'il s'agisse de contacts plus anciens ou incomplets.")
    
    logger.info("\nAnalyse terminée.")

if __name__ == "__main__":
    main()