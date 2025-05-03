from pyvis.network import Network

def visualize_network_html(G, output_path="network.html"):
    """
    Génére une visualisation interactive HTML.
    Args:
        G (networkx.Graph): Le graphe à afficher.
        output_path (str): Chemin du fichier HTML de sortie.
    """
    net = Network(notebook=False, width="100%", height="800px", bgcolor="#222222", font_color="white")
    net.from_nx(G)
    net.show(output_path)
