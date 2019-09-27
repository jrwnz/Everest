import re
import datetime
import pandas as pd
from everest import utils
from everest import utils

def clean_link_strings(link_string):
    return link_string.replace("\\","\\\\").replace('"','\"')

def rename_columns(df):
    df.rename(
        columns={
         x: (x.lower()
             .replace('desc','')
             .replace('(','')
             .replace(')','')
             .replace('%','')
             .strip()
             .replace(' ','_'))
         for x in df.columns},
        inplace=True)

def get_link_type(row):
    # This function has been modified to remove code sensitive to the client
    try:
        link_type = 'unrecognized'
        if isinstance(row['link_anchor'],str):
            link_type = 'Has anchor'
        else:
            linke_type = 'No anchor'
        return link_type
    except Exception as e:
        return('Error finding link type - {}'.format(e))


def update_domain_matches(pg_engine):
    sql_query = 'SELECT DISTINCT referring_domain FROM domains'
    domains = utils.frame_from_pg(sql_query, pg_engine)
    all_domains = set(domains['referring_domain'])

    sql_query = 'SELECT DISTINCT referring_page_url FROM backlinks'
    backlink_urls = utils.frame_from_pg(sql_query, pg_engine)

    (
        backlink_urls['url_https'],
        backlink_urls['url_www'],
        backlink_urls['url_main'], 
        backlink_urls['url_path'],
        backlink_urls['url_params'],
        backlink_urls['error_in_split_url']
    ) = zip(
        *backlink_urls['referring_page_url'].apply(utils.split_url)
    )

    url_main_domain_matches = backlink_urls[['url_main']].drop_duplicates()
    (
        url_main_domain_matches['matching_ahrefs_domain'], 
        url_main_domain_matches['error_in_get_matching_domain']
    ) = zip(
        *url_main_domain_matches['url_main'].apply(utils.get_matching_domain,args=[all_domains])
    )
    url_main_domain_matches = url_main_domain_matches[
        ['url_main','matching_ahrefs_domain','error_in_get_matching_domain']
    ]

    utils.frame_to_pg(pg_engine,
                url_main_domain_matches,
                'url_main_domain_matches',
                ['url_main']
            )


def update_domains_to_scrape(pg_engine):
    sql_query = """SELECT referring_page_url, backlink_status, first_seen FROM backlinks WHERE link_type IN ('email','formcode')"""
    backlinks_forms = utils.frame_from_pg(sql_query,pg_engine)
    sql_query = """SELECT * FROM url_main_domain_matches"""
    url_main_domain_matches = utils.frame_from_pg(sql_query,pg_engine)

    (
    backlinks_forms['url_https'],
    backlinks_forms['url_www'],
    backlinks_forms['url_main'], 
    backlinks_forms['url_path'],
    backlinks_forms['url_params'],
    backlinks_forms['error_in_split_url']
    ) = zip(
        *backlinks_forms['referring_page_url'].apply(utils.split_url)
    )

    backlinks_forms = pd.merge(
        backlinks_forms,
        url_main_domain_matches,
        how='left',
        on='url_main'
    )

    backlinks_forms['total_backlinks'] = 1
    backlinks_forms['live_backlinks'] = 1*backlinks_forms['backlink_status'].isnull()
    backlinks_forms['url_www'] = 1*(backlinks_forms['url_www'] == 'www.')
    backlinks_forms['url_https'] = 1*(backlinks_forms['url_https'] == 'https://')

    backlinks_domains = pd.merge(
        backlinks_forms[['matching_ahrefs_domain','total_backlinks','live_backlinks','url_www','url_https']].groupby('matching_ahrefs_domain').sum(),
        backlinks_forms[['matching_ahrefs_domain','first_seen']].groupby('matching_ahrefs_domain').min().reset_index(),
        on='matching_ahrefs_domain',
        how='inner'
    ).rename(columns={'matching_ahrefs_domain': 'domain'})

    utils.update_pg_table(pg_engine,
                    backlinks_domains,
                    'scraped_domains',
                    ['domain'],
                    update_cols = ['total_backlinks','live_backlinks','url_www','url_https']
                    )

def upload_ahrefs_data(
    backlinks_file,
    domains_file,
    pg_engine
    ):
    print('Loading from CSV')
    backlinks = pd.read_csv(backlinks_file,
                            encoding='utf-16',
                            sep='\t')
    domains = pd.read_csv(domains_file,
                            encoding='utf-16',
                            sep='\t')

    print('Cleaning and processing tables')
    rename_columns(backlinks)
    rename_columns(domains)

    backlinks['referring_page_url'] = backlinks['referring_page_url'].apply(utils.clean_link_strings)
    backlinks['link_url'] = backlinks['link_url'].apply(utils.clean_link_strings)

    backlinks['first_seen'] = pd.to_datetime(backlinks['first_seen'])
    backlinks['last_check'] = pd.to_datetime(backlinks['last_check'])
    backlinks['day_lost'] = pd.to_datetime(backlinks['day_lost'])
    backlinks['live'] = backlinks['backlink_status'].isnull()
    backlinks['link_type'] = backlinks.apply(get_link_type,axis=1)

    domains['first_seen'] = pd.to_datetime(domains['first_seen'])

    update_time = datetime.datetime.now()
    backlinks['last_updated'] = update_time
    domains['last_updated'] = update_time

    backlinks.drop(columns=['#'],inplace=True)
    domains.drop(columns=['#'],inplace=True)

    (
        backlinks['url_https'],
        backlinks['url_www'],
        backlinks['url_main'], 
        backlinks['url_path'],
        backlinks['url_params'],
        backlinks['error_in_split_url']
    ) = zip(
        *backlinks['referring_page_url'].apply(utils.split_url)
    )
    backlinks.drop(columns=['url_https',
                            'url_www',
                            'url_path',
                            'url_params',
                            'error_in_split_url'],inplace=True)

    print('Uploading backlinks to postgres')
    utils.update_pg_table(pg_engine,
        backlinks,
        'backlinks',
        ['referring_page_url', 'link_url', 'first_seen'],
        'all'
    )
    print('Uploading domains to postgres')
    utils.update_pg_table(pg_engine,
        domains,
        'domains',
        ['referring_domain'],
        'all'
    )

    print('Updating url/domain matches')
    update_domain_matches(pg_engine)

    print('Updating domains to scrape')
    update_domains_to_scrape(pg_engine)

    print('------------------------------------')
    print('Upload of new crawler data complete.')