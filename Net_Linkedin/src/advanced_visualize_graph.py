import matplotlib.pyplot as plt
import networkx as nx
import seaborn as sns
from networkx.algorithms import community
import numpy as np
import os

def visualize_advanced_network(G, save_path=None):
    """
    Visualise le réseau avec NetworkX et optionnellement sauvegarde l'image.
    
    Args:
        G (networkx.Graph): Graphe du réseau.
        save_path (str, optional): Chemin pour sauvegarder l'image. 
                                   Si None, ne sauvegarde pas.
    """
    plt.figure(figsize=(14, 10))

    pos = nx.spring_layout(G, seed=42)
    communities = nx.get_node_attributes(G, 'community')

    # Définir des couleurs par communauté
    if communities:
        unique_coms = list(set(communities.values()))
        color_map = [plt.cm.tab10(communities[node] % 10) for node in G.nodes()]
    else:
        color_map = 'skyblue'

    # Dessiner les nœuds avec les couleurs par communauté
    nx.draw_networkx_nodes(G, pos, node_size=60, node_color=color_map, alpha=0.8)
    
    # Dessiner les arêtes, en distinguant les connexions "temps" (même entreprise + année/mois)
    edges = G.edges()
    time_edges = [(u, v) for u, v, data in G.edges(data=True) if data.get('relationship') == 'coworker_same_time']
    other_edges = list(set(edges) - set(time_edges))

    # Dessiner les arêtes basées sur la même entreprise, année et mois avec une couleur différente
    nx.draw_networkx_edges(G, pos, edgelist=time_edges, edge_color='red', width=1.5, alpha=0.7)
    nx.draw_networkx_edges(G, pos, edgelist=other_edges, edge_color='skyblue', width=0.5, alpha=0.4)

    plt.title("Visualisation Avancée du Réseau LinkedIn", fontsize=16)
    plt.axis('off')
    plt.tight_layout()

    if save_path:
        # Créer le dossier s'il n'existe pas
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        plt.savefig(save_path, format='png', dpi=300)
        print(f"✅ Graphe sauvegardé ici : {save_path}")

    plt.show()

def plot_heatmap_of_network(G: nx.Graph):
    """
    Génère une carte de chaleur basée sur la matrice d'adjacence du graphe,
    en triant les nœuds par communauté.
    
    Args:
        G (nx.Graph): Le graphe NetworkX.
    """
    # Détection des communautés
    communities_list = community.greedy_modularity_communities(G)
    
    # Créer une liste ordonnée de nœuds par communauté
    ordered_nodes = []
    for comm in communities_list:
        ordered_nodes.extend(list(comm))
    
    # Générer la matrice d'adjacence triée
    adj_matrix = nx.to_numpy_array(G, nodelist=ordered_nodes)

    # Affichage de la carte de chaleur
    plt.figure(figsize=(12, 10))
    sns.heatmap(adj_matrix, cmap='YlGnBu', cbar=True,
                xticklabels=ordered_nodes, yticklabels=ordered_nodes)
    plt.title('Carte de Chaleur du Réseau LinkedIn (par Communautés)')
    plt.xlabel('Nœuds')
    plt.ylabel('Nœuds')
    plt.xticks(rotation=90)
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.show()
