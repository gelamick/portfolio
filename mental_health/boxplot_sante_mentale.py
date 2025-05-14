import matplotlib.pyplot as plt
import seaborn as sns

def generate_boxplot(df):
    plt.figure(figsize=(10, 6))
    sns.boxplot(x="tranche_age", y="score_sante_mentale", data=df, palette="Set3")
    plt.title("Distribution des scores de santé mentale par tranche d'âge")
    plt.xlabel("Tranche d'âge")
    plt.ylabel("Score santé mentale")
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    return plt
