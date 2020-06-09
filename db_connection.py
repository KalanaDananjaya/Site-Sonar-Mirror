import sqlite3
import csv 
import datetime
import logging

from config import SQL_FILE,DATABASE_FILE, JOB_STATES
from sql_queries import *


# Utils
def normalize_ce_name(target_ce):
    return target_ce.replace("::", "_").lower()[len("alice_"):]

# Connection Functions
def get_connection(DATABASE_FILE, detect_types= sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES):
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        return conn
    except sqlite3.Error as error:
        logging.exception("Error while connecting to database: %s", error)

def initialize_db(DATABASE_FILE):
    conn = get_connection(DATABASE_FILE)
    with open (SQL_FILE,'r') as f:
        sql = f.read()
        conn.executescript(sql)
        conn.commit()

# Site Related Functions
def get_sites():
    conn = get_connection(DATABASE_FILE)
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

def get_site_ids():
    conn = get_connection(DATABASE_FILE)
    cursor = conn.execute(GET_SITE_IDS)
    ids = []
    for row in cursor:
        site_id = row[0]
        ids.append(site_id)
    return ids

def add_sites_from_csv(csv_filename):
    conn = get_connection(DATABASE_FILE)
    site_tuples = []
    sitenames = get_sitenames()
    with open(csv_filename, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in csv_reader:
            details = row[0].split(',')
            current_site_name = details[0]
            is_exists = check_sitename_exists(sitenames, current_site_name)
            if (is_exists == True):
                logging.warning('%s already exists in the database. Please use the update function. Skipping...',current_site_name)
            else:
                num_nodes = details[1]
                if not num_nodes:
                    num_nodes = -1
                else:
                    num_nodes = int(num_nodes)
                normalized_name = normalize_ce_name(current_site_name)
                site_tuples.append((current_site_name, normalized_name, num_nodes, ''))
                logging.info('Adding %s to the database', current_site_name)
    conn.executemany(ADD_SITE, site_tuples)
    conn.commit()
    logging.debug("Total number of sites added : %s", conn.total_changes)
                

def check_sitename_exists(sitenames,current_site_name):
    # Return current_site_name exists in sitenames, return True
    if not sitenames:
        return False
    else:
        if current_site_name in sitenames:
            return True
        else:
            return False

def get_sitenames():
    conn = get_connection(DATABASE_FILE)
    sitenames=[]
    cursor = conn.execute(GET_SITENAMES)
    for row in cursor:
        sitenames.append(row[0])
    return sitenames

def get_siteid_by_normalized_name(normalized_name):
    logging.debug('Searching id for %s...', normalized_name)
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(GET_SITEID_BY_NORMALIZED_NAME,[normalized_name])
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)
    rows = cursor.fetchall()
    for row in rows:
        site_id = row[0]
        logging.debug ('Site_id of %s: %s',normalized_name, site_id)
        return site_id

# Not tested
def get_siteid_by_name(site_name):
    conn = get_connection(DATABASE_FILE)
    cursor = conn.execute(GET_SITEID_BY_NAME,[site_name])

# Node related functions
def get_nodeid_by_node_name(site_id,node_name):
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(GET_NODEID_BY_NODE_NAME,[site_id, node_name])
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)
    rows = cursor.fetchall()
    result_len = len(rows)
    if result_len == 0:
        logging.debug('Node does not exist')
        return False
    else:
        for row in rows:
            node_id = row[0]
            return node_id

def add_node(site_id,node_name):
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(ADD_NODE,[site_id, node_name])
        conn.commit()
        node_id  = cursor.lastrowid
        return node_id
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

# Parsing related functions
def add_parsed_output(site_id,node_id,parsed_result):
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(ADD_PARSED_OUTPUT,[site_id, node_id,parsed_result])
        conn.commit()
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def add_parsed_output_by_names(normalized_site_name,node_name,parsed_result):
    site_id = get_siteid_by_normalized_name(normalized_site_name)
    node_id = get_nodeid_by_node_name(site_id, node_name)
    if not node_id:
        node_id = add_node(site_id,node_name)
    logging.debug ('Node id of %s in site_id %s: %s', node_name, site_id, node_id)
    add_parsed_output(site_id,node_id,parsed_result)
    return True

def delete_parsed_outputs():
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(DELETE_PARSED_OUTPUTS)
        conn.commit()
        logging.debug('Deleted parsed outputs successfully')
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

# Processing state related functions
def delete_processed_states():
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(DELETE_PROCESSING_STATE)
        conn.commit()
        logging.debug('Deleted processed states successfully')
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def initialize_processing_state():
    delete_processed_states()
    site_ids = get_site_ids()
    timestamp = datetime.datetime.now()
    state_tuple = []
    for site_id in site_ids:
        state_tuple.append((site_id,timestamp,'WAITING'))
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.executemany(INITIALIZE_PROCESSING_STATE,state_tuple)
        conn.commit()
        logging.debug('Initialized processing states successfully')
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def update_processing_state(site_id,state):
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(UPDATE_PROCESSING_STATE,[state,site_id])
        conn.commit()
        logging.debug('Updated processing state successfully')
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def update_processing_state_by_sitename(normalized_site_name,state):
    site_id = get_siteid_by_normalized_name(normalized_site_name)
    update_processing_state(site_id,state)

def get_processing_state_siteids_by_state(state):
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(GET_PROCESSING_STATE_SITEIDS_BY_STATE,[state])
        site_ids = []
        for row in cursor:
            site_ids.append(row[0])
        return site_ids
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

# Job related functions
def add_job_batch(jobs,site_id):
    timestamp = datetime.datetime.now()
    job_tuples = []
    for job_id in jobs:
        job_tuples.append((job_id, site_id, timestamp, 'STARTED','W'))
    conn = get_connection(DATABASE_FILE)
    try:
        conn.executemany(ADD_JOBS, job_tuples)
        conn.commit()
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def get_all_jobs_by_site_id(site_id):
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(GET_JOBS_BY_SITEID,[site_id])
        jobs = []
        for row in cursor:
            job_id = row[0]
            site_id = row[1]
            timestamp = row[2]
            abstract_state = row[3]
            state = row[4]
            site = {
                'job_id': job_id,
                'site_id': site_id,
                'timestamp': timestamp,
                'abstract_state': abstract_state,
                'state': state
                }
            jobs.append(job)  
        return jobs
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def get_jobs_by_siteid_and_abs_state(site_id,abstract_state):
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(GET_INCOMPLETE_JOBS_BY_SITEID,[site_id,abstract_state])
        jobs = []
        for row in cursor:
            job_id = row[0]
            site_id = row[1]
            timestamp = row[2]
            abstract_state = row[3]
            state = row[4]
            job = {
                'job_id': job_id,
                'site_id': site_id,
                'timestamp': timestamp,
                'abstract_state': abstract_state,
                'state': state
                }
            jobs.append(job)  
        return jobs
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def update_job_states(job_id,abstract_state,state):
    timestamp = datetime.datetime.now()
    conn = get_connection(DATABASE_FILE)
    try:
        conn.execute(UPDATE_JOB, [timestamp,abstract_state,state,job_id])
        conn.commit()
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

#def get_incomplete_jobs_by_siteid(site_id)