import sqlite3
import csv 
import datetime


database_file = 'site-sonar-db.db'
sql_file = 'db.sql'


ADD_SITE = 'INSERT INTO SITES (site_name,normalized_name,num_nodes,remarks) \
      VALUES ( ?, ?, ?, ? )'
GET_SITES = 'SELECT * FROM sites'
GET_SITENAMES = 'SELECT site_name FROM sites'
GET_SITEID_BY_NAME = 'SELECT site_id FROM sites where site_id = ?'
ADD_JOBS = 'INSERT INTO JOBS (job_id,site_id,timestamp,status) VALUES (?, ?, ?, ?)'

# Utils
def normalize_ce_name(target_ce):
    return target_ce.replace("::", "_").lower()[len("alice_"):]

# Connection Functions
def get_connection(database_file, detect_types= sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES):
    try:
        conn = sqlite3.connect(database_file)
        print ('Opened database successfully')
        return conn
    except sqlite3.Error as error:
        print("Error while working with SQLite", error)

def initialize_db(database_file):
    conn = get_connection(database_file)
    with open (sql_file,'r') as f:
        sql = f.read()
        conn.executescript(sql)
        conn.commit()

# Querying Functions
def get_sites():
    conn = get_connection(database_file)
    cursor = conn.execute(GET_SITES)
    sites = []
    for row in cursor:
        site_id = row[0]
        site_name = row[1]
        normalized_name = row[2]
        num_nodes = row[3]
        remarks = row[4]
        site = {
            'site_id': site_id,
            'site_name': site_name,
            'normalized_name': normalized_name,
            'num_nodes': num_nodes,
            'remarks': remarks
            }
        sites.append(site)
    return sites

def add_sites_from_csv(csv_filename):
    conn = get_connection(database_file)
    site_tuples = []
    sitenames = get_sitenames()
    with open(csv_filename, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in csv_reader:
            details = row[0].split(',')
            current_site_name = details[0]
            is_exists = check_sitename_exists(sitenames, current_site_name)
            if (is_exists == True):
                print (current_site_name + ' already exists in the database. Please use the update function. Skipping...')
            else:
                num_nodes = details[1]
                if not num_nodes:
                    num_nodes = -1
                else:
                    num_nodes = int(num_nodes)
                normalized_name = normalize_ce_name(current_site_name)
                site_tuples.append((current_site_name, normalized_name, num_nodes, ''))
                print ('Adding ' + current_site_name + ' to the database')
    conn.executemany(ADD_SITE, site_tuples)
    conn.commit()
    print ("Total number of rows added :", conn.total_changes)
                

def check_sitename_exists(sitenames,current_site_name):

    if not sitenames:
        return False
    else:
        if current_site_name in sitenames:
            return True
        else:
            return False

def get_sitenames():
    conn = get_connection(database_file)
    sitenames=[]
    cursor = conn.execute(GET_SITENAMES)
    for row in cursor:
        sitenames.append(row[0])
    return sitenames

# Not tested
def get_siteid_by_name(site_name):
    conn = get_connection(database_file)
    cursor = conn.execute(GET_SITEID_BY_NAME,site_name)

def add_job_batch(jobs,site_id):
    timestamp = datetime.datetime.now()
    job_tuples = []
    for job_id in jobs:
        job_tuples.append((job_id, site_id, timestamp, 'STARTED'))
    conn = get_connection(database_file)
    conn.executemany(ADD_JOBS, job_tuples)
    conn.commit()


if __name__ == "__main__":
    conn = get_connection(database_file)
    add_sites_from_csv('test_ce_list.csv')
