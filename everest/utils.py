import io, re, json, base64
import pandas as pd
import psycopg2
import sqlalchemy as sql

def clean_link_strings(link_string):
    return link_string.replace("\\","\\\\").replace('"','\"')

def get_matching_domain(url, all_domains):
    try:
        while '.' in url:
            if url in all_domains:
                return url, None
            else:
                url = url[url.find('.')+1:]
        return None, None
    except Exception as e:
        return None, e

def get_url_main(url):
    try:
        url_split = re.search('^(https?://)(www.)?([^/]*)\/?(.*)',url)
        url_main = url_split.group(3)
    except:
        url_main = None
    return url_main

def get_all_ahrefs_domains(pg_engine):
    all_ahrefs_domains_df = utils.frame_from_pg("""SELECT referring_domain FROM domains""",pg_engine)
    return set(all_ahrefs_domains_df['referring_domain'])

def split_url(url):
    try:
        if url.startswith('\"'):
            url = url[1:]
            if url.endswith('\"'):
                url = url[:-1]
        url = url.strip()
        url_split = re.search('(https?://)(wwww?\d?.)?([^/?]*)\/?([^?]*)\??(.*)',url)
        url_https = url_split.group(1).strip() if url_split.group(1) else None
        url_www = url_split.group(2).strip() if url_split.group(2) else None
        url_main = url_split.group(3).strip() if url_split.group(3) else None
        url_path = url_split.group(4).strip() if url_split.group(4) else None
        url_params = url_split.group(5).strip() if url_split.group(5) else None
        return url_https, url_www, url_main, url_path, url_params, None
    except Exception as e:
        return None, None, None, None, None, e    

def decode_base64(text):
    text = base64.b64decode(text).decode('utf-8')
    return text

def decode_links(row):
    for col in ['links','links_main_domains','links_ahrefs_domains']:
        row[col] = json.loads(decode_base64(row[col]))
    return row

def frame_to_pg(engine,df,table_name, primary_keys):    
    df.head(0).to_sql(table_name, engine, if_exists='replace',index=False)
    
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', encoding='utf-8', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, table_name, null="") # null values become ''
    cur.execute("""ALTER TABLE {} ADD PRIMARY KEY ({});""".format(table_name,", ".join(primary_keys)))
    conn.commit()
    conn.close()

def update_pg_table(engine,df,table_name,primary_keys,update_cols=None):
    frame_to_pg(engine,df,table_name+'_temp', primary_keys)

    conn = engine.connect()
    if update_cols == 'all':
        update_cols = [x for x in df.columns]
    if update_cols:
        if not isinstance(update_cols,list):
            print("Error: update_cols must be a list of columns, None, or 'all'")
            return
        update_cols = [x for x in update_cols if x not in primary_keys]
        col_string = ', '.join(['{} = t2.{}'.format(x,x) for x in update_cols])
        key_string = ' AND '.join(['t.{} = t2.{}'.format(x,x) for x in primary_keys])

        update_string = """UPDATE {} AS t SET {} FROM {}_temp AS t2 WHERE {}""".format(
            table_name,col_string,table_name,key_string
        )
        conn.execute(update_string)
    all_cols = [x for x in df.columns]
    col_string = ', '.join([x for x in all_cols])
    conn.execute("""INSERT INTO {} ({}) SELECT {} FROM {}_temp ON CONFLICT DO NOTHING;""".format(
                                                        table_name,col_string,col_string,table_name)
                )
    conn.execute("""DROP TABLE {}_temp;""".format(table_name))
    conn.close()    

def frame_from_pg(query, engine):
    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
       query=query, head="HEADER"
    )
    conn = engine.raw_connection()
    cur = conn.cursor()
    store = io.StringIO()
    cur.copy_expert(copy_sql, store)
    store.seek(0)
    df = pd.read_csv(store)
    return df