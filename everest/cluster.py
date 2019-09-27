import base64
import pandas as pd
from everest import utils
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import pairwise_distances
import hdbscan

def recluster(pg_engine, min_cluster_size = 10):
    print('Pulling page data from db')
    sql_query = """SELECT domain, main, lang 
                     FROM scraped_domains 
                    WHERE last_successful_scrape_date IS NOT NULL
                          AND main IS NOT NULL"""
                    
    clusters = utils.frame_from_pg(sql_query,pg_engine)

    clusters['main'] = clusters['main'].apply(base64.b64decode)
    clusters['main'] = clusters['main'].str.decode('utf-8')

    hdb = hdbscan.HDBSCAN(min_cluster_size = min_cluster_size)

    clusters['cluster'] = None
    clusters['mean_distance'] = None

    languages = set([x for x in clusters['lang'] if len(x) < 10])

    for language in languages:
        language_data = clusters[clusters['lang']==language].copy()
        language_bodies = language_data['main'] 
        try:    
            tfv = TfidfVectorizer(min_df=2)
            X_train_tf = tfv.fit_transform(language_bodies)

            hdb.fit(X_train_tf)
            language_data['cluster'] = hdb.labels_
            language_data['cluster'] = language_data['cluster'].astype(str)
            language_data['cluster'] = language + language_data['cluster']
            
            cluster_names = [cluster for cluster in language_data['cluster'].unique()
                    if cluster
                    and not '-' in cluster]
            for cluster_name in cluster_names:
                lab_tf = tfv.transform(language_data[language_data['cluster']==cluster_name]['main'])
                dist = pairwise_distances(lab_tf)
                mean_distance = dist.mean()
                language_data.loc[language_data['cluster']==cluster_name,'mean_distance'] = mean_distance
            
            clusters.update(language_data[['cluster','mean_distance']])
            print("Clustering complete for language code {}".format(language))
        except Exception as e:
            print("Clustering failed for language code {} with error: {}".format(language,e))
            language_data['cluster'] = language + '-clustering-failed'
            clusters.update(language_data[['cluster']])
    clusters = clusters.drop(columns=['main'])
    utils.frame_to_pg(pg_engine,clusters,'clusters',['domain'])

class Clusters:
    def __init__(self,pg_engine):
        self.pg_engine = pg_engine
        self.pull_cluster_domains()
        self.pull_cluster_backlinks()
        self.set_new_date_threshold()
        self.set_cluster_info()
        print("Cluster data ready")
            
    def pull_cluster_domains(self):
        print("Pulling cluster domains from db")
        sql_query = """SELECT c.*, 
                              sd.last_successful_scrape_date, 
                              sd.live_backlinks, 
                              sd.total_backlinks,
                              d.domain_rating,
                              d.organic_traffic,
                              d.first_seen,
                              d.last_updated
                         FROM clusters AS c 
                              LEFT JOIN scraped_domains AS sd 
                              ON c.domain = sd.domain
                              LEFT JOIN domains AS d 
                              ON c.domain = d.referring_domain
                        WHERE cluster IS NOT NULL AND cluster NOT LIKE '%-%'"""
        cluster_domains = utils.frame_from_pg(sql_query,self.pg_engine)
        self.cluster_domains = cluster_domains.set_index('domain')

    def pull_cluster_backlinks(self):
        print("Pulling cluster backlinks from db")
        sql_query = """SELECT c.cluster, b.*
                 FROM clusters AS c 
                      LEFT JOIN url_main_domain_matches AS udm
                        ON c.domain = udm.matching_ahrefs_domain
                      LEFT JOIN backlinks AS b
                        ON b.url_main = udm.url_main
                WHERE cluster IS NOT NULL AND cluster NOT LIKE '%-%'"""
        cluster_backlinks = utils.frame_from_pg(sql_query,self.pg_engine)
        self.cluster_backlinks = cluster_backlinks.set_index('referring_page_url')

    def set_cluster_info(self):
        cs = pd.DataFrame(
            self.cluster_domains.groupby(['cluster']).size()
                    ).rename(columns={0: 'num_domains'})
        cs['language'] = cs.index.str[:2]
        cs = cs[['language','num_domains']]
        totals = self.cluster_domains.groupby(['cluster']).sum()[
            ['new','live_backlinks','total_backlinks','organic_traffic']
        ].rename(columns = {'new': 'num_new_domains',
                           'live_backlinks': 'total_live_backlinks',
                           'organic_traffic': 'total_organic_traffic'})
        totals['num_new_domains'] = totals['num_new_domains'].astype(int)
        maxes = self.cluster_domains.groupby(['cluster']).max()[
            ['live_backlinks','total_backlinks','domain_rating','organic_traffic']
        ].rename(
            columns = {
                'live_backlinks': 'max_live_backlinks',
                'total_backlinks': 'max_backlinks',
                'domain_rating': 'max_domain_rating',
                'organic_traffic': 'max_organic_traffic'
            }
        )
        means = self.cluster_domains.groupby(['cluster']).median()[
            ['mean_distance','live_backlinks','total_backlinks','domain_rating','organic_traffic']
        ].rename(
            columns = {
                'live_backlinks': 'mean_live_backlinks',
                'total_backlinks': 'mean_backlinks',
                'domain_rating': 'mean_domain_rating',
                'organic_traffic': 'mean_organic_traffic'
            }
        )
        cs = pd.merge(
            cs,
            totals,
            how = 'left',
            left_index = True,
            right_index = True
        )
        cs = pd.merge(
            cs,
            means,
            how = 'left',
            left_index = True,
            right_index = True
        )
        cs = pd.merge(
            cs,
            maxes,
            how = 'left',
            left_index = True,
            right_index = True
        )
        cs = cs.sort_values(['language','num_domains','mean_distance'],ascending=[True,False,True])
        self.cluster_info = cs
        
    def set_new_date_threshold(self,date_string=None):
        if not date_string:
            self.new_date_threshold = min(
                self.cluster_domains['last_updated'].min(),
                self.cluster_domains['last_updated'].min()
            )
        else:
            self.new_date_threshold = date_string
        self.cluster_domains['new'] = self.cluster_domains['last_updated'] >= self.new_date_threshold
        self.cluster_backlinks['new'] = self.cluster_backlinks['last_updated'] >= self.new_date_threshold
        print('Date threshold updated')
        
    def get_cluster_domains(self,cluster):
        return self.cluster_domains[self.cluster_domains['cluster']==cluster]

    def get_cluster_backlinks(self,cluster):
        return self.cluster_backlinks[self.cluster_backlinks['cluster']==cluster]

    def get_cluster_domains_by_domain(self,domain):
        return self.get_cluster_domains(self.cluster_domains.loc[domain,'cluster'])

    def get_cluster_backlinks_by_url(self,url):
        return self.get_cluster_backlinks(self.cluster_backlinks.loc[url,'cluster'])