from typing import List
import networkx as nx
import numpy as np
import pandas as pd


def get_cluster_year_connected_component_table(intersection_matching: pd.DataFrame) -> pd.DataFrame:
    matching_graph = nx.Graph()
    edges = [(f"{row['y1']}_{row['id1']}", f"{row['y2']}_{row['id2']}") for i, row in intersection_matching.iterrows()]
    matching_graph.add_edges_from(edges)
    connected_components = list(nx.connected_components(matching_graph))
    return _extract_as_dataframe(connected_components)


def _extract_as_dataframe(connected_components: List[List[str]]) -> pd.DataFrame:
    data = []
    for i, component in enumerate(connected_components):
        for node in component:
            year, cluster_id = (int(x) for x in node.split('_'))
            data.append({'component_id': i, 'year': year, 'cluster_id': cluster_id})

    data = pd.DataFrame(data)
    return data