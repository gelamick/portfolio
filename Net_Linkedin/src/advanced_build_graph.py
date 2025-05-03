# src/advanced_build_graph.py

import networkx as nx
import pandas as pd
from networkx.algorithms import community
import streamlit as st

def build_advanced_network(df: pd.DataFrame) -> tuple:
    """
    Construit un graphe avancé du réseau LinkedIn avec détection de communautés.

    Args:
        df (pd.DataFrame): DataFrame contenant les connexions LinkedIn.

    Returns:
        tuple: (graphe NetworkX, liste de communautés)
    """
    G = nx.Graph()

    # --- Ajouter les nœuds ---
    for _, row in df.iterrows():
        full_name = f"{row['First Name']} {row['Last Name']}"
        G.add_node(full_name,
                   company=row.get('Company', ''),
                   position=row.get('Position', ''),
                   email=row.get('Email Address', ''),
                   url=row.get('URL', ''),
                   connected_on=row.get('Connected On', ''))

    # --- Créer des liens artificiels entre collègues ---
    company_groups = df.groupby('Company')
    for company, group in company_groups:
        names = [f"{row['First Name']} {row['Last Name']}" for _, row in group.iterrows()]
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                G.add_edge(names[i], names[j], relationship='coworker')

    # --- Info ---
    st.write(f"Nombre d'entreprises uniques : {len(company_groups)}")

    # --- Détection des communautés ---
    raw_communities = community.greedy_modularity_communities(G)
    sorted_communities = sorted(raw_communities, key=lambda c: len(c), reverse=True)

    st.write("Top 5 des plus grandes communautés :")
    for i, comm in enumerate(sorted_communities[:5]):
        with st.expander(f"Communauté {i + 1} ({len(comm)} personnes)"):
            st.write(sorted(comm))

    # --- Calcul de la centralité sur la plus grande composante connexe ---
    if not nx.is_connected(G):
        largest_cc = max(nx.connected_components(G), key=len)
        G_sub = G.subgraph(largest_cc).copy()
        st.warning("Le graphe est fragmenté. Centralité calculée uniquement sur la plus grande composante.")
    else:
        G_sub = G

    centrality = nx.betweenness_centrality(G_sub)

    # --- Ajout de la centralité à tous les nœuds ---
    for node in G.nodes():
        G.nodes[node]["centrality"] = centrality.get(node, 0.0)

    # --- Détection des super-connecteurs ---
    top_super_connectors = detect_super_connectors(G, top_n=10)
    st.subheader("🔝 Top 10 Super-Connecteurs")
    for idx, (name, degree) in enumerate(top_super_connectors, 1):
        st.markdown(f"{idx}. **{name}** – {degree} connexions")

    return G, sorted_communities


def detect_super_connectors(G: nx.Graph, top_n=10) -> list:
    """
    Retourne les nœuds avec le plus grand nombre de connexions.

    Args:
        G (nx.Graph): Graphe NetworkX.
        top_n (int): Nombre de super-connecteurs à retourner.

    Returns:
        List[Tuple[str, int]]: Liste triée de (nom, nombre de connexions)
    """
    degrees = dict(G.degree())
    sorted_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)
    return sorted_nodes[:top_n]