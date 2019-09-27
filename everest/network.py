import json, base64
import pandas as pd
from everest import utils
import networkx as nx
import matplotlib.pyplot as plt

class Networks:
    def __init__(self,pg_engine):
        self.link_type = 'all_ahrefs_domains' #main_domains, #links
        self.removed_nodes = []
        print("Pulling domains from db")
        self.pg_engine = pg_engine
        self.pull_network_domains()
        print("Creating network graph")
        self.reset_network_graph()
        print("Network data ready")

    def pull_network_domains(self):
        sql_query = """SELECT sd.domain,
                            d.domain_rating,
                            d.organic_traffic,
                            sd.live_backlinks,
                            sd.total_backlinks,
                            d.first_seen,
                            d.last_updated,
                            sd.last_successful_scrape_date,
                            sd.links,
                            sd.links_ahrefs_domains, 
                            sd.links_main_domains,
                            sd.lang,
                            sd.html
                        FROM domains AS d
                            LEFT JOIN scraped_domains AS sd
                            ON d.referring_domain = sd.domain"""

        network_domains = utils.frame_from_pg(sql_query,self.pg_engine)
        self.all_ahrefs_domains = network_domains.copy()
        network_domains = network_domains[~network_domains['links'].isnull()]
        network_domains = network_domains.apply(utils.decode_links,axis=1)
        self.network_domains = network_domains

    def reset_network_graph(self):
        if self.link_type == 'all_ahrefs_domains':
            links_column = 'links_ahrefs_domains'
        elif self.link_type == 'all_domains':
            links_column = 'links_main_domains'
        elif self.link_type == 'all_links':
            links_column = 'links'
        else:
            raise ValueError("network link_type must be 'all_ahrefs_domains', 'all_domains', or 'all_links'.")

        G = nx.DiGraph()
        for index in self.network_domains.index:
            domain_one = self.network_domains.loc[index,'domain']
            link_list = self.network_domains.loc[index,links_column]
            if link_list and isinstance(link_list,list):
                for domain_two in link_list:
                    if domain_two != domain_one:
                        G.add_edge(domain_one,domain_two)
        self.network_graph = G
        self.update_network_info()
        self.removed_nodes = []

    def remove_node(self,node):
        # G = self.network_graph
        # G.remove_node(node)
        self.network_graph.remove_node(node)
        self.removed_nodes.append(node)
        self.update_network_info()

    def update_network_info(self):
        G = self.network_graph
        components = sorted(list(nx.weakly_connected_components(G)),key=len,reverse=True)
        self.components = components

        domain_info = pd.DataFrame([
            {'domain': node, 
            'all_links': G.degree[node],
            'in_links': G.in_degree[node],
            'out_links': G.out_degree[node],
            } 
            for node in G
            ]).sort_values('all_links',ascending=False).set_index('domain')
        domain_info['component'] = None

        component_info = []
        for index, component in enumerate(components):
            for domain in component:
                domain_info.loc[domain,'component'] = index
            c_info = {
                'component': index,
                'size': G.subgraph(component).number_of_nodes(),
                'links': G.subgraph(component).number_of_edges(),
                'centroid': max(list(G.degree(component)),key=lambda item:item[1])[0],
                'centroid_links': max(list(G.degree(component)),key=lambda item:item[1])[1],
            }
            c_info['density'] = c_info['links']/c_info['size']
            c_info['star'] = c_info['links'] ==  c_info['centroid_links']
            component_info.append(c_info)
        component_info = pd.DataFrame(component_info).set_index('component')

        domain_info = domain_info.join(
            self.all_ahrefs_domains.set_index('domain')[['domain_rating','organic_traffic']],
            how='left'
        )

        component_info = component_info.join(
            domain_info.groupby('component').mean().rename(columns={
                'domain_rating': 'mean_domain_rating',
                'organic_traffic': 'mean_organic_traffic'
            })[['mean_domain_rating','mean_organic_traffic']]
        ).join(
            domain_info.groupby('component').max().rename(columns={
                'domain_rating': 'max_domain_rating',
                'organic_traffic': 'max_organic_traffic'
            })[['max_domain_rating','max_organic_traffic']]
        )

        self.domain_info = domain_info[['component','all_links','in_links', 'out_links', 
                                        'domain_rating','organic_traffic']]
        self.component_info = component_info[['size','links','density','star','centroid', 'centroid_links',
                                        'mean_domain_rating', 'mean_organic_traffic', 'max_domain_rating',
                                        'max_organic_traffic']]

    def set_link_type(self,link_type):
        if link_type in ['all_ahrefs_domains','all_domains','all_links']:
            self.link_type = link_type
            self.reset_network_graph()
        else:
            raise ValueError("network link_type must be 'all_ahrefs_domains', 'all_domains', or 'all_links'.")

    def get_component(self,component):
        return self.domain_info[self.domain_info['component']==component]

    def get_component_by_domain(self,domain):
        return self.get_component(self.domain_info.loc[domain,'component'])

    def draw_component(self,component,labels=True):
        G = self.network_graph
        components = self.components 
        plt.axis('off')
        plt.rcParams['figure.dpi'] = 100
        plt.rcParams['figure.figsize'] = [8, 8]
        nx.draw_networkx(G.subgraph(components[component]),
                        with_labels=True,
                        node_size = 20,
                        alpha = 0.5,
                        edge_color = 'grey',
                        font_size = 10
                        )

    def draw_component_by_domain(self,domain):
        return self.draw_component(self.domain_info.loc[domain,'component'])


            


