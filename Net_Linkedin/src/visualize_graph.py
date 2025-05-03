import matplotlib.pyplot as plt
import networkx as nx

def visualize_network(G):
    """
    Affiche le graphe du réseau LinkedIn.
    Args:
        G (networkx.Graph): le graphe.
    """
    plt.figure(figsize=(14, 14))
    pos = nx.spring_layout(G, seed=42)

    # Dessiner les nœuds
    nx.draw_networkx_nodes(G, pos, node_color='skyblue', node_size=500)

    # Dessiner les arêtes
    nx.draw_networkx_edges(G, pos, alpha=0.5)

    # Labels : Nom + Entreprise
    labels = {node: f"{node}\n{data['company']}" for node, data in G.nodes(data=True)}
    nx.draw_networkx_labels(G, pos, labels=labels, font_size=8)

    plt.title("Carte de mon réseau LinkedIn", fontsize=20)
    plt.axis('off')
    plt.show()
