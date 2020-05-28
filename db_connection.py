import sqlite3
import csv 

# >>> sqlite3 site-sonar-db.db < db.sql


database_file = 'site-sonar-db.db'



ADD_SITE = 'INSERT INTO SITES (site_name,normalized_name,num_nodes,remarks) \
      VALUES ( ?, ?, ?, ? )'
GET_SITES = 'SELECT * FROM sites'
GET_SITENAMES = 'SELECT site_name FROM sites'

def normalize_ce_name(target_ce):
    return target_ce.replace("::", "_").lower()[len("alice_"):]

def get_connection(database_file):
    conn = sqlite3.connect(database_file)
    print ('Opened database successfully')
    return conn

def get_sites(conn):
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

def add_site_from_csv(conn, csv_filename):
    site_tuples = []
    sitenames = get_sitenames(conn)
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

def get_sitenames(conn):
    sitenames=[]
    cursor = conn.execute(GET_SITENAMES)
    for row in cursor:
        sitenames.append(row[0])
    return sitenames


