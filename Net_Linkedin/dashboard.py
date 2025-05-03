# dashboard.py
import streamlit as st
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

from src.load_data import load_connections
from src.advanced_build_graph import build_advanced_network
from src.generate_report import generate_report
from src.generate_excel import generate_strategic_contacts_excel

# --- Charger les données ---
st.title("🕸️ Mon Réseau LinkedIn - Dashboard")

data_path = 'data/connections.csv'
df = load_connections(data_path)

# --- Aperçu des données ---
st.subheader("🔎 Aperçu des données brutes")
st.write(df.head())
st.write(f"Nombre initial de données : {len(df)}")

# --- Filtres interactifs améliorés ---
st.sidebar.header("🎯 Filtres Avancés")
df['Connected On'] = pd.to_datetime(df['Connected On'], errors='coerce')

position_options = sorted(df['Position'].dropna().unique())
company_options = sorted(df['Company'].dropna().unique())
min_date = df['Connected On'].min()
max_date = df['Connected On'].max()

selected_positions = st.sidebar.multiselect("💼 Filtrer par titre (position)", position_options)
selected_companies = st.sidebar.multiselect("🏢 Filtrer par entreprise", company_options)
selected_date_range = st.sidebar.date_input("📅 Plage de dates de connexion", [min_date, max_date])

if selected_positions:
    df = df[df['Position'].isin(selected_positions)]
if selected_companies:
    df = df[df['Company'].isin(selected_companies)]
if selected_date_range and len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    df = df[df['Connected On'].between(start_date, end_date)]

# --- Afficher un message si vide ---
if df.empty:
    st.warning("Aucune donnée pour les filtres sélectionnés.")
else:
    st.success(f"{len(df)} contacts trouvés après application des filtres.")
    if len(df) < 2:
        st.warning("Pas assez de données pour construire un graphe.")
    else:
        G, communities = build_advanced_network(df)

        # --- Vérification de la connectivité du graphe ---
        connected_components = list(nx.connected_components(G))
        st.write(f"Nombre de composants connexes : {len(connected_components)}")
        
        if len(connected_components) > 1:
            largest_component = max(connected_components, key=len)
            G = G.subgraph(largest_component)
            st.write("Sous-graphe de la plus grande composante connexe utilisé.")

        # --- Informations sur le graphe ---
        st.subheader("📊 Informations sur le Graphe")
        st.write(f"Nombre de nœuds : {G.number_of_nodes()}")
        st.write(f"Nombre d'arêtes : {G.number_of_edges()}")
        st.write(f"Nombre de communautés détectées : {len(communities)}")

        # --- Visualisation du graphe ---
        st.subheader("🖼️ Visualisation du Réseau par Communautés")
        if len(communities) > 0:
            colors = cm.rainbow(np.linspace(0, 1, len(communities)))
            node_color_map = {node: color for color, comm in zip(colors, communities) for node in comm}
            node_colors = [node_color_map.get(node, (0.5, 0.5, 0.5)) for node in G.nodes()]
            pos = nx.spring_layout(G, k=0.2, iterations=50)

            # Créer un mapping anonymisé
            node_list = list(G.nodes())
            anonymized_labels = {node: f"Node {i + 1}" for i, node in enumerate(node_list)}

            time_edges = [(u, v) for u, v, data in G.edges(data=True) if data.get('relationship') == 'coworker_same_time']
            other_edges = list(set(G.edges()) - set(time_edges))

            fig, ax = plt.subplots(figsize=(16, 14))
            nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=300, alpha=0.7)
            nx.draw_networkx_labels(G, pos, labels=anonymized_labels, font_size=8, font_color='black')  # réduis à 8 pour caser les 2 lignes
            nx.draw_networkx_edges(G, pos, edgelist=time_edges, edge_color='red', width=1.5, alpha=0.7)
            nx.draw_networkx_edges(G, pos, edgelist=other_edges, edge_color='skyblue', width=0.5, alpha=0.4)
            st.pyplot(fig)
        else:
            st.warning("Aucune communauté trouvée dans le graphe.")

        # --- Calcul des centralités ---
        st.subheader("🔑 Connexions Influentes")
        degree_centrality = nx.degree_centrality(G)
        betweenness_centrality = nx.betweenness_centrality(G)

        degree_centrality_df = pd.DataFrame(degree_centrality.values(), index=degree_centrality.keys(), columns=["Degree Centrality"])
        betweenness_centrality_df = pd.DataFrame(betweenness_centrality.values(), index=betweenness_centrality.keys(), columns=["Betweenness Centrality"])

        st.write("Distribution de la centralité de degré :")
        st.write(degree_centrality_df.describe())

        st.write("Distribution de la centralité de betweenness :")
        st.write(betweenness_centrality_df.describe())

        centrality_df = pd.DataFrame({
            'Degree Centrality': degree_centrality.values(),
            'Betweenness Centrality': betweenness_centrality.values()
        }, index=degree_centrality.keys())

        sorted_centrality = centrality_df.sort_values(by='Betweenness Centrality', ascending=False)
        top_influencers = sorted_centrality.head(10)
        top_influencers = top_influencers.dropna(subset=['Betweenness Centrality'])

        if not top_influencers.empty:
            st.write("Top 10 des super-connecteurs et leur centralité de betweenness :")
            st.write(top_influencers)
            top_influencers_plot = top_influencers[['Betweenness Centrality']] \
                                .sort_values(by='Betweenness Centrality', ascending=True) \
                                .plot(kind='barh', figsize=(10, 6), color='orange')
            st.pyplot(top_influencers_plot.figure)
        else:
            st.warning("Aucun super-connecteur trouvé.")

        # --- Visualisation des connexions influentes ---
        st.subheader("🖼️ Sous-Graphe des Connexions Influentes")
        influencer_nodes = top_influencers.index.tolist()
        H = G.subgraph(influencer_nodes)

        fig, ax = plt.subplots(figsize=(10, 8))
        nx.draw(H, ax=ax, with_labels=True, node_color='orange', node_size=1000, font_size=10)
        st.pyplot(fig)

        # --- Génération de rapport PDF et fichier Excel ---
        st.subheader("📁 Exportations")
        col1, col2 = st.columns(2)

        with col1:
            if st.button("📄 Générer Rapport PDF"):
                generate_report(G, output_pdf_path='data/dashboard_rapport.pdf')
                st.success("Rapport PDF généré : `dashboard_rapport.pdf`")

        with col2:
            if st.button("📊 Générer Excel Contacts Stratégiques"):
                generate_strategic_contacts_excel(G, output_path='data/dashboard_contacts.xlsx')
                st.success("Fichier Excel généré : `dashboard_contacts.xlsx`")
