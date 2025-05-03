import pandas as pd
import networkx as nx
from networkx.algorithms import community

def generate_strategic_contacts_excel(G, output_path='data/contacts_strategiques.xlsx'):
    """
    Exporte les contacts stratégiques vers un fichier Excel.
    Args:
        G (networkx.Graph): Graphe LinkedIn.
        output_path (str): chemin du fichier Excel.
    """
    # Calcul de la centralité de degré
    centrality = nx.degree_centrality(G)
    nx.set_node_attributes(G, centrality, 'centrality')

    # Détection des communautés
    communities_generator = community.greedy_modularity_communities(G)
    community_dict = {}

    # Assigner un identifiant de communauté à chaque nœud
    for idx, comm in enumerate(communities_generator):
        for node in comm:
            community_dict[node] = idx  # Attribuer l'identifiant de la communauté

    # Ajouter les communautés au graphe
    nx.set_node_attributes(G, community_dict, 'community')

    # Vérifier que les attributs sont bien ajoutés (pour le débogage)
    for node in list(G.nodes)[:5]:  # Vérifier les 5 premiers nœuds
        print(f"Node: {node}, Community: {G.nodes[node].get('community', 'No community')}, Centrality: {G.nodes[node].get('centrality', 'No centrality')}")

    # Préparer les données à exporter
    data = []
    for node, attr in G.nodes(data=True):
        data.append({
            'Nom': node,
            'Entreprise': attr.get('company', ''),
            'Poste': attr.get('position', ''),
            'Date de connexion': attr.get('connected_on', ''),
            'Communauté': attr.get('community', ''),
            'Centralité': attr.get('centrality', 0),
        })

    # Créer le DataFrame
    df = pd.DataFrame(data)

    # Vérifier les données avant export
    print(df[['Nom', 'Communauté', 'Centralité']].head())

    # Trier par centralité décroissante
    df = df.sort_values(by='Centralité', ascending=False)

    # Exporter vers un fichier Excel
    df.to_excel(output_path, index=False)
    print(f"✅ Fichier Excel généré ici : {output_path}")
