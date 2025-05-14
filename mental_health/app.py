import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import io

# Pondérations
poids = {
    "absenteisme_psy": 0.3,
    "charge_mentale": 0.25,
    "soutien_managerial": 0.2,
    "participation_prevention": 0.15
}

# Risques selon tranche d'âge
age_risque = {
    "22-29": 1.3,
    "30-39": 1.2,
    "40-49": 0.9,
    "50-55": 1.0,
    "56+":   1.2
}

# Fonction de calcul du score
def calcul_score(row):
    score_brut = (
        row["absenteisme_psy"] * poids["absenteisme_psy"] +
        row["charge_mentale"] * poids["charge_mentale"] +
        row["soutien_managerial"] * poids["soutien_managerial"] +
        row["participation_prevention"] * poids["participation_prevention"]
    )
    facteur_age = age_risque.get(row["tranche_age"], 1.0)
    return round(score_brut * facteur_age, 2)

# Titre de l'application
st.title("Évaluation du Risque Santé Mentale")

# Explication
st.write("""
Ce projet évalue le risque santé mentale des collaborateurs en fonction de plusieurs indicateurs RH et de leur tranche d'âge.
Les utilisateurs peuvent télécharger un fichier CSV contenant ces indicateurs, calculer les scores et télécharger un fichier avec les résultats.
""")

# Ajouter un template de fichier CSV
def generate_sample_csv():
    data = {
        'absenteisme_psy': [0, 1, 2],
        'charge_mentale': [1, 2, 3],
        'soutien_managerial': [4, 3, 2],
        'participation_prevention': [5, 4, 3],
        'tranche_age': ['22-29', '30-39', '40-49']
    }
    df = pd.DataFrame(data)
    csv = df.to_csv(index=False)
    return csv

# Ajouter un bouton pour télécharger le template CSV
st.subheader("Télécharger un Template CSV")
csv_template = generate_sample_csv()
st.download_button(
    label="Télécharger le template CSV",
    data=csv_template,
    file_name="template_indicateurs_sante_mentale.csv",
    mime="text/csv"
)

# Téléchargement du fichier CSV par l'utilisateur
uploaded_file = st.file_uploader("Téléchargez votre fichier CSV (format attendu)", type=["csv"])

if uploaded_file is not None:
    # Charger les données
    df = pd.read_csv(uploaded_file)
    
    # Vérifier que les colonnes sont correctes
    required_columns = ['absenteisme_psy', 'charge_mentale', 'soutien_managerial', 'participation_prevention', 'tranche_age']
    if not all(col in df.columns for col in required_columns):
        st.error(f"Le fichier CSV doit contenir les colonnes suivantes : {', '.join(required_columns)}.")
    else:
        # Calcul du score de santé mentale
        df["score_sante_mentale"] = df.apply(calcul_score, axis=1)
        
        # Afficher les résultats dans un tableau
        st.subheader("Données et scores calculés")
        st.write(df)
        
        # Graphique : Distribution des scores de santé mentale
        st.subheader("Distribution des scores par tranche d'âge")
        plt.figure(figsize=(10, 6))
        sns.boxplot(x="tranche_age", y="score_sante_mentale", data=df, palette="Set3")
        plt.title("Distribution des scores de santé mentale par tranche d'âge")
        plt.xlabel("Tranche d'âge")
        plt.ylabel("Score santé mentale")
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        st.pyplot()

        # Télécharger le fichier des résultats
        st.download_button(
            label="Télécharger les résultats (CSV)",
            data=df.to_csv(index=False),
            file_name="resultats_scores.csv",
            mime="text/csv"
        )
