# Évaluation du Risque Santé Mentale en Entreprise

Ce projet propose un algorithme simple pour évaluer un **score de risque santé mentale** basé sur plusieurs indicateurs RH et la tranche d'âge des collaborateurs. L'application peut être exécutée en ligne via Streamlit pour une utilisation simple et rapide.

## Fichiers

- `indicateurs_sante_mentale.csv` : Données d'exemple (factices)
- `main.py` : Script Python pour calculer les scores
- `app.py` : Application Streamlit pour visualiser les résultats
- `resultats_scores.csv` : Fichier de sortie avec les scores (généré après exécution)
- `README.md` : Ce fichier d’explication

## Indicateurs utilisés

- `absenteisme_psy` (0 à 5)
- `charge_mentale` (0 à 5)
- `soutien_managerial` (0 à 5)
- `participation_prevention` (0 à 5)
- `tranche_age` : âge du collaborateur (impact sur le facteur de risque)

## Tranches d'âge et facteur de risque associé

| Tranche d’âge | Facteur de risque | Justification |
|---------------|-------------------|----------------|
| 22–29         | 1.3               | Début de carrière, précarité, surcharge cognitive |
| 30–39         | 1.2               | Concilier vie pro/perso, responsabilités croissantes |
| 40–49         | 0.9               | Stabilité accrue, meilleure gestion du stress |
| 50–55         | 1.0               | Transition douce, mais possible fatigue |
| 56+           | 1.2               | Fatigue chronique, isolement, santé physique |

## Interprétation du score

- `0.0 – 1.5` : Zone verte — pas d’alerte
- `1.6 – 2.5` : Zone orange — vigilance
- `2.6 – 3.5` : Zone rouge — attention requise
- `>3.5` : Alerte critique

## Installation des dépendances

Avant d'exécuter le script, vous devez installer les dépendances nécessaires.
```bash
pip install -r requirements.txt

### 1. Créer un environnement virtuel (optionnel mais recommandé)
```bash
python3 -m venv venv  # Créer un environnement virtuel
source venv/bin/activate  # Sur Linux/macOS
venv\Scripts\activate  # Sur Windows

### 2. Lancer Streamlit
streamlit run app.py
