from src.load_data import load_connections
from src.advanced_build_graph import build_advanced_network, detect_super_connectors
from src.advanced_visualize_graph import visualize_advanced_network, plot_heatmap_of_network
from src.generate_report import generate_report
from src.generate_excel import generate_strategic_contacts_excel
from src.filter_data import filter_by_year, filter_by_company

import warnings
warnings.filterwarnings('ignore')

def main():
    # Charger les données
    data_path = 'data/connections.csv'
    df = load_connections(data_path)

    # Construire le graphe avancé
    G, communities = build_advanced_network(df)

    # Détection des super-connecteurs
    super_connectors = detect_super_connectors(G, top_n=10)
    print(f"Top 10 super-connecteurs : {super_connectors}")

    # Visualisation du réseau
    visualize_advanced_network(G, save_path='data/reseau_global.png')

    # Visualiser la carte de chaleur du réseau
    plot_heatmap_of_network(G)

    # Générer le rapport global
    generate_report(G, output_pdf_path='data/rapport_reseau_global.pdf')

    # Générer le rapport des connexions 2024
    df_2024 = filter_by_year(df, 2024)
    if not df_2024.empty:
        G_2024, _ = build_advanced_network(df_2024)
        generate_report(G_2024, output_pdf_path='data/rapport_reseau_2024.pdf')

    # Rapport des anciens collègues d'ABC Corp
    df_abc = filter_by_company(df, 'ABC Corp')
    if not df_abc.empty:
        G_abc, _ = build_advanced_network(df_abc)
        generate_report(G_abc, output_pdf_path='data/rapport_reseau_ABC_Corp.pdf')

    # Générer l'Excel des contacts stratégiques
    generate_strategic_contacts_excel(G)

if __name__ == "__main__":
    main()
