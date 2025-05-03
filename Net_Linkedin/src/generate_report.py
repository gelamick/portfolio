import matplotlib.pyplot as plt
from fpdf import FPDF
import networkx as nx

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 16)
        self.cell(0, 10, 'Rapport Réseau LinkedIn', ln=True, align='C')
        self.ln(10)

    def chapter_title(self, title):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, ln=True, align='L')
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Arial', '', 12)
        self.multi_cell(0, 8, body)
        self.ln(5)

def generate_community_bar_chart(G, output_path='data/community_sizes.png'):
    """
    Génère un graphique des tailles de communautés.
    Args:
        G (networkx.Graph): Graphe du réseau.
        output_path (str): Chemin pour sauvegarder le graphique.
    """
    communities = nx.get_node_attributes(G, 'community')
    community_counts = {}

    for node, com in communities.items():
        community_counts[com] = community_counts.get(com, 0) + 1

    # Trier les communautés par taille
    sorted_communities = dict(sorted(community_counts.items(), key=lambda x: x[1], reverse=True))

    plt.figure(figsize=(10, 6))
    plt.bar([f"Communauté {k}" for k in sorted_communities.keys()], sorted_communities.values(), color='skyblue')
    plt.xlabel('Communautés')
    plt.ylabel('Nombre de membres')
    plt.title('Taille des Communautés LinkedIn')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()

def generate_report(G, output_pdf_path='rapport_reseau.pdf'):
    """
    Génère un rapport PDF avec graphique à partir du graphe.
    Args:
        G (networkx.Graph): Graphe LinkedIn.
        output_pdf_path (str): chemin de sortie du fichier PDF.
    """
    pdf = PDF()
    pdf.add_page()

    # Récupérer les infos
    total_nodes = G.number_of_nodes()
    communities = set(nx.get_node_attributes(G, 'community').values())
    total_communities = len(communities)

    print(f"Nombre total de nœuds: {total_nodes}")  # Débogage
    print(f"Communautés détectées: {communities}")  # Débogage
    print(f"Centralité des nœuds: {nx.get_node_attributes(G, 'centrality')}")  # Débogage

    pdf.chapter_title("Résumé général")
    pdf.chapter_body(f"Nombre total de connexions : {total_nodes}")
    pdf.chapter_body(f"Nombre de communautés détectées : {total_communities}")

    # Top 10 contacts
    centrality = nx.get_node_attributes(G, 'centrality')
    sorted_centrality = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:10]

    pdf.chapter_title("Top 10 des contacts les plus connectés")
    for idx, (name, cent) in enumerate(sorted_centrality, 1):
        pdf.chapter_body(f"{idx}. {name} (centralité : {cent:.4f})")

    # Générer et insérer un graphique
    bar_chart_path = 'data/community_sizes.png'
    generate_community_bar_chart(G, bar_chart_path)

    pdf.chapter_title("Graphique des tailles de communautés")
    pdf.image(bar_chart_path, x=10, w=190)

    pdf.output(output_pdf_path)
    print(f"✅ Rapport généré ici : {output_pdf_path}")