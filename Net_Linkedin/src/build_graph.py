import networkx as nx
import pandas as pd
from networkx.algorithms import community
import streamlit as st

def build_advanced_network(df: pd.DataFrame) -> tuple:
    """
    Construit un graphe avancé du réseau LinkedIn avec détection de communautés et connexions basées sur l'entreprise, l'année et le mois de connexion.
    
    Args:
        df (pd.DataFrame): Le DataFrame contenant les connexions LinkedIn.
    
    Returns:
        tuple: Un tuple contenant (G, communautés), où G est un graphe NetworkX et communautés est une liste de communautés détectées.
    """
    G = nx.Graph()

    # Ajouter les nœuds avec les informations nécessaires
    for idx, row in df.iterrows():
        full_name = f"{row['First Name']} {row['Last Name']}"
        connected_on = row.get('Connected On', '')
        year_month = None
        
        # Extraire l'année et le mois de la date de connexion si disponible
        if connected_on:
            try:
                connected_on = pd.to_datetime(connected_on)
                year_month = (connected_on.year, connected_on.month)
            except Exception as e:
                st.warning(f"Erreur de conversion de la date pour {full_name}: {e}")
        
        G.add_node(full_name, 
                   company=row['Company'], 
                   position=row['Position'], 
                   email=row.get('Email Address', ''), 
                   url=row.get('URL', ''),
                   connected_on=connected_on,
                   year_month=year_month  # Ajouter l'année et le mois de connexion
                   )

    # Ajouter des liens basés sur la même entreprise, la même année et le même mois
    for _, group in df.groupby('Company'):
        for _, subgroup in group.groupby('year_month'):  # Grouper par année et mois
            names = [f"{row['First Name']} {row['Last Name']}" for _, row in subgroup.iterrows()]
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    G.add_edge(names[i], names[j], relationship='coworker_same_time')

    # Afficher le nombre d'entreprises uniques
    st.write(f"Nombre d'entreprises uniques : {len(df['Company'].unique())}")

    # Filtrer et afficher uniquement les top 5 communautés
    st.write("Top 5 des communautés les plus grandes :")
    communities = community.greedy_modularity_communities(G)
    sorted_communities = sorted(communities, key=lambda c: len(c), reverse=True)[:5]
    
    for i, comm in enumerate(sorted_communities):
        with st.expander(f"Communauté {i + 1} ({len(comm)} personnes)"):
            st.write(list(comm))

    # Centralité : sur la plus grande composante connexe
    if not nx.is_connected(G):
        largest_cc = max(nx.connected_components(G), key=len)
        G_sub = G.subgraph(largest_cc).copy()
        st.warning("Le graphe est fragmenté. La centralité est calculée sur la plus grande composante connexe.")
    else:
        G_sub = G

    centrality = nx.betweenness_centrality(G_sub)

    # Enrichir les nœuds avec la centralité
    for node, cent in centrality.items():
        G.nodes[node]["centrality"] = cent

    # Pour les autres nœuds hors composante, centralité = 0.0
    for node in G.nodes():
        if "centrality" not in G.nodes[node]:
            G.nodes[node]["centrality"] = 0.0

    # Détecter les super-connecteurs (top 10 des plus connectés)
    top_super_connectors = detect_super_connectors(G, top_n=10)
    st.write("Top 10 des super-connecteurs :")
    for idx, (name, degree) in enumerate(top_super_connectors, 1):
        st.write(f"{idx}. {name} - {degree} connexions")

    # Retourner le graphe et les communautés
    return G, communities

def detect_super_connectors(G: nx.Graph, top_n=10) -> list:
    """
    Détecte les super-connecteurs (nœuds avec le plus de connexions).

    Args:
        G (nx.Graph): Le graphe NetworkX.
        top_n (int): Nombre de super-connecteurs à retourner.

    Returns:
        List of tuples (nom, nombre_de_connexions)
    """
    degree_dict = dict(G.degree())
    sorted_connectors = sorted(degree_dict.items(), key=lambda x: x[1], reverse=True)
    return sorted_connectors[:top_n]