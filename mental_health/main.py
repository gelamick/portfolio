import pandas as pd

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

def main():
    df = pd.read_csv("indicateurs_sante_mentale.csv")
    df["score_sante_mentale"] = df.apply(calcul_score, axis=1)
    df.to_csv("resultats_scores.csv", index=False)
    print("Analyse terminée. Fichier 'resultats_scores.csv' généré.")

if __name__ == "__main__":
    main()
