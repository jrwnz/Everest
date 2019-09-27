import re, json, base64
import datetime, time
import pandas as pd
import numpy as np
import requests

from langdetect import detect
from bs4 import BeautifulSoup
from bs4.element import Comment

from everest import utils

def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def text_from_html(soup):
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    string = u" ".join(t.strip() for t in visible_texts)
    string = string.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    string = re.sub(' +',' ',string).strip()
    return string

def get_page_text(soup,domain):
    try:
        title = soup.find('title')
        if title:
            title = title.text
            title = re.sub('\s+',' ',title)
#         body = soup.find('body')
#         if body:
#             body = text_from_html(body)
#         headers = soup.find_all('header')
#         header = ' '.join([text_from_html(header) for header in headers]) if headers else None
#         footers = soup.find_all('footer')
#         footer = ' '.join([text_from_html(footer) for footer in footers]) if footers else None
        mains = soup.find_all('main')
        if mains:
            main = ' '.join([text_from_html(main) for main in mains])
        else:
            main = soup.find('body')
            if main:
                if main.header:
                    main.header.decompose()
                if main.footer:
                    main.footer.decompose()
                main = text_from_html(main)
        a_tags = soup.findAll('a', attrs={'href': re.compile("^https?://")})
        links = [a_tag.get('href') for a_tag in a_tags if domain not in a_tag.get('href')]
        try:
            lang = detect(main)
        except:
            lang = "Can't detect language - no text, or langdetect not installed"
#         return title, body, header, footer, main, links, lang, None
        return title, main, links, lang, None
    except Exception as e:
        return None, None, None, None, str(e)
#         return None, None, None, None, None, None, None, str(e)


def scrape_domain(row,all_ahrefs_domains,time_limit):       
    domain = row['domain']
    urls_to_try = []
    protocols = []
    if row['url_https'] > 0:
        protocols.append('https://')
    if row['url_https'] < row['total_backlinks']:
        protocols.append('http://')
    if row['url_www'] > 0:
        for protocol in protocols:
            urls_to_try.append('{}www.{}'.format(protocol,domain))
    if row['url_www'] < row['total_backlinks']:
        for protocol in protocols:
            urls_to_try.append('{}{}'.format(protocol,domain))
                        
    r = None
    for url in urls_to_try:      
        try:
            dtime = datetime.datetime.now()
            row['last_scrape_date'] = dtime
            r = requests.get(url,timeout=time_limit)
            row['last_scrape_request_error'] = None
            row['last_scrape_code'] = r.status_code
#             if r.status_code == 200:
#                 with open('Scraping/Scraped Domains/More/{}.pkl'.format(row.name),'wb') as f:
#                     pickle.dump(r,f)
#                 break
        except Exception as e:
            row['last_scrape_code'] = None
            row['last_scrape_request_error'] = str(e)
    
    if r and r.status_code == 200:
        try:
            soup = BeautifulSoup(r.content)
            title, main, links, lang, last_scrape_parse_error = get_page_text(soup, domain)
#             title, body, header, footer, main, links, lang, last_scrape_parse_error = get_page_text(soup, domain)
            row['last_scrape_parse_error'] = last_scrape_parse_error
            
            if links:
                links_main_domains = list(set([utils.get_url_main(a_tag) for a_tag in links]))
            else:
                links_main_domains = []
                links = []
            if links_main_domains:
                links_ahrefs_domains = list(set([utils.get_matching_domain(a_tag,all_ahrefs_domains)[0] for a_tag in links_main_domains]))
                links_ahrefs_domains = [x for x in links_ahrefs_domains if x]
            else:
                links_ahrefs_domains = []
#             row['title'] = base64.b64encode(title).decode('utf-8')
#             row['body'] = body
#             row['header'] = header
#             row['footer'] = footer
            row['lang'] = lang
            row['last_successful_scrape_date'] = dtime
            row['html'] = base64.b64encode(r.content).decode('utf-8')
            row['main'] = base64.b64encode(main).decode('utf-8')
            row['links'] = base64.b64encode(json.dumps(links).encode('utf-8')).decode('utf-8')
            row['links_main_domains'] = base64.b64encode(json.dumps(links_main_domains).encode('utf-8')).decode('utf-8')
            row['links_ahrefs_domains'] = base64.b64encode(json.dumps(links_ahrefs_domains).encode('utf-8')).decode('utf-8')
        except Exception as e:
            row['last_scrape_parse_error'] = str(e)
    return row

def scrape_batch(pg_engine,time_limit,batch_size,scrape_condition):
    all_ahrefs_domains = utils.get_all_ahrefs_domains(pg_engine)

    sql_query = """SELECT * 
                 FROM scraped_domains 
                {}
             ORDER BY first_seen DESC""".format(scrape_condition)
    scraped_domains = utils.frame_from_pg(sql_query,pg_engine)

    if scraped_domains.shape[0] == 0:
        print('Nothing to scrape')
        return

    start = time.time()
    completed = 0
    total = scraped_domains.shape[0]

    while scraped_domains.shape[0] > 0:
        immediate = scraped_domains.head(batch_size)
        scraped_domains = scraped_domains[~scraped_domains.index.isin(immediate.index)]

        immediate = immediate.apply(scrape_domain,axis=1,args=(all_ahrefs_domains,time_limit,))

        # for col in ['title','main']:
        #     immediate[col] = immediate[col].astype(str)
        #     immediate[col] = immediate[col].str.replace("\\","\\\\").str.replace('"','\"')

        immediate_success = immediate[immediate['last_scrape_date']==immediate['last_successful_scrape_date']]
        immediate_failure = immediate[immediate['last_scrape_date']!=immediate['last_successful_scrape_date']]

        try:
            utils.update_pg_table(pg_engine,
                immediate_success,
                'scraped_domains',
                ['domain'],
                update_cols = 'all'
                )

            utils.update_pg_table(pg_engine,
                immediate_failure,
                'scraped_domains',
                ['domain'],
                update_cols = ['domain','last_scrape_code','last_scrape_date','last_scrape_parse_error','last_scrape_request_error']
                )

            completed += immediate.shape[0]
        except Exception as e:
            if batch_size == 1:
                immediate['soup'] = None
                immediate['title'] = None
                immediate['main'] = None
                immediate['links'] = None
                immediate['links_main_domains'] = None
                immediate['links_ahrefs_domains'] = None
                immediate['last_scrape_parse_error'] = 'Scraped nonutf8 characters failed pg upload.'
                first_ind = immediate.index[0]
                if not immediate.loc[first_ind,'last_scrape_code']:
                    immediate.loc[first_ind,'last_scrape_code'] = 0
                    immediate['last_scrape_code'] = immediate['last_scrape_code'].astype(int)
                    immediate.loc[first_ind,'last_scrape_code'] = np.nan
                utils.update_pg_table(pg_engine,
                    immediate,
                    'scraped_domains',
                    ['domain'],
                    update_cols = ['domain','last_scrape_code','last_scrape_date','last_scrape_parse_error','last_scrape_request_error']
                )

        print('Scraped {} of {} domains. {} remaining. Elapsed time: {}'.format(
            completed,total,scraped_domains.shape[0],time.time()-start))
    
def scrape(pg_engine,time_limit=2,scrape_condition='WHERE last_scrape_date IS NULL'):
    print('-----------Scraping First Pass----------')
    scrape_batch(pg_engine,time_limit,100,scrape_condition)
    print('-----------Scraping Second Pass----------')
    scrape_batch(pg_engine,time_limit,10,scrape_condition)
    print('-----------Scraping Third Pass----------')
    scrape_batch(pg_engine,time_limit,1,scrape_condition)
    print('---------Scraping Complete----------')
