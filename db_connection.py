import sqlite3
import csv 
import datetime
import logging
import os

from config import SQL_FILE,DATABASE_FILE
from sql_queries import *


# Utils
def normalize_ce_name(target_ce):
    return target_ce.replace("::", "_").lower()[len("alice_"):]

# Connection Functions
def get_connection(database_file, detect_types= sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES):
    """
    Create database connection

    Args:
        database_file (str): Name of the SQLite DB file
        detect_types : Used to store datetime objects. Defaults to sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES.

    Returns:
        SQLite Connection object: Database connection
    """
    try:
        conn = sqlite3.connect(database_file)
        return conn
    except sqlite3.Error as error:
        logging.exception("Error while connecting to database: %s", error)

def initialize_db():
    """
    Initialize database using SQL File

    Args:
        DATABASE_FILE (str): Name of the SQLite DB file, set from config
        SQL_FILE(str): Set from config
    """
    conn = get_connection(database_file)
    with open (SQL_FILE,'r') as f:
        sql = f.read()
        try:
            conn.executescript(sql)
            conn.commit()
        except sqlite3.Error as error:
            logging.exception("Error while connecting to database: %s", error)

def clear_db():
    """
    Delete the database file if exists
    """
    if os.path.exists(DATABASE_FILE): 
        os.remove(DATABASE_FILE) 

def clear_tables():
    """
    Clear Nodes, Jobs, Processing_state, Parsed_outputs tables
    """
    conn = get_connection(DATABASE_FILE)
    try:
        conn.execute(DELETE_NODES)
        conn.execute(DELETE_JOBS)
        conn.execute(DELETE_PROCESSING_STATE)
        conn.execute(DELETE_PARSED_OUTPUTS)
        conn.commit()
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)


# Site Related Functions
def get_sites():
    """
    Get all Grid sites

    Returns:
        sites(dict): All site data
    """
    conn = get_connection(DATABASE_FILE)
    cursor = conn.execute(GET_SITES)
    sites = []
    for row in cursor:
        site_id = row[0]
        site_name = row[1]
        normalized_name = row[2]
        num_nodes = row[3]
        last_update = row[4]
        site = {
            'site_id': site_id,
            'site_name': site_name,
            'normalized_name': normalized_name,
            'num_nodes': num_nodes,
            'last_update': last_update
            }
        sites.append(site)
    return sites

def get_site_ids():
    """
    Get Site IDs

    Returns:
        Site IDs(list)
    """
    conn = get_connection(DATABASE_FILE)
    cursor = conn.execute(GET_SITE_IDS)
    ids = []
    for row in cursor:
        site_id = row[0]
        ids.append(site_id)
    return ids

def add_sites_from_csv(csv_filename):
    """
    Add Grid sites from CSV file

    Args:
        csv_filename (str): Name of the CSV file (Format: sitename,num_nodes)
    """
    conn = get_connection(DATABASE_FILE)
    current_time = datetime.datetime.now()
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
                site_tuples.append((current_site_name, normalized_name, num_nodes, current_time))
                logging.debug('Adding %s to the database', current_site_name)
    conn.executemany(ADD_SITE, site_tuples)
    conn.commit()
    logging.debug("Total number of sites added : %s", conn.total_changes)

def update_site_last_update_time(site_id):
    """
    Update last update time of the site

    Args:
        site_id (int): Site ID
    """
    current_time = datetime.datetime.now()
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(UPDATE_LAST_SITE_UPDATE_TIME,[current_time,site_id])
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)
                

def check_sitename_exists(sitenames,current_site_name):
    """
    Check whether current_site_name exists in sitenames

    Args:
        sitenames (list): List of available sites in the DB
        current_site_name (str): Name of the new site

    Returns:
        Bool: True if current_site_name exists in sitenames
    """
    if not sitenames:
        return False
    else:
        if current_site_name in sitenames:
            return True
        else:
            return False

def get_sitenames():
    """
    Get all names of sites

    Returns:
        sitenames
    """
    conn = get_connection(DATABASE_FILE)
    sitenames=[]
    cursor = conn.execute(GET_SITENAMES)
    for row in cursor:
        sitenames.append(row[0])
    return sitenames

def get_num_nodes_in_site(site_id):
    """
    Get the number of nodes in site

    Args:
        site_id (int])

    Returns:
        Number of nodes
    """
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(GET_NUM_NODES_IN_SITE,[site_id])
        for row in cursor:
            return row[0]
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def get_siteid_by_normalized_name(normalized_name):
    """
    Get Site ID by the normalized name

    Args:
        normalized_name (str)

    Returns:
        site_id
    """
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

def get_normalized_name_by_siteid(site_id):
    """
    Get normalized name by Site ID

    Args:
        site_id (int)

    Returns:
        normalized_name(str)
    """
    logging.debug('Searching normalized name for site with id: %s...', str(site_id))
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(GET_NORMALIZED_NAME_BY_SITE_ID,[site_id])
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)
    rows = cursor.fetchall()
    for row in rows:
        normalized_name = row[0]
        logging.debug ('Normalized name of site %s: %s',str(site_id),normalized_name)
        return normalized_name


# Node related functions
def get_nodeid_by_node_name(site_id,node_name):
    """
    Get Node Id by Node Name

    Args:
        site_id (int)
        node_name (str): Name of the node

    Returns:
        node_id (int)
    """
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
    """
    Add node to the database

    Args:
        site_id (int)
        node_name (str): Name of the node

    Returns:
        node_id (int): Generated Node ID
    """
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
    """
    Add parsed output to the database

    Args:
        site_id (int)
        node_id (int)
        parsed_result (json string): Parsed output dict converted to JSON string
    """
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(ADD_PARSED_OUTPUT,[site_id, node_id,parsed_result])
        conn.commit()
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def add_parsed_output_by_names(normalized_site_name,node_name,parsed_result):
    """
    Add parsed output to the database if the a duplicate result has not been added already

    Args:
        normalized_site_name (str)
        node_name ([str)
        parsed_result (json string): Parsed output dict converted to JSON string

    Returns:
        Bool: True if succesful
    """
    site_id = get_siteid_by_normalized_name(normalized_site_name)
    node_id = get_nodeid_by_node_name(site_id, node_name)
    if not node_id:
        node_id = add_node(site_id,node_name)
        logging.debug ('Assigned Node Id %s to %s of site %s',node_id,node_name,site_id)
        add_parsed_output(site_id,node_id,parsed_result)
    return True

def get_parsed_output_by_siteid(site_id):
    """
    Get all parsed outputs of the site nodes

    Args:
        site_id (int)

    Returns:
        site_outputs (dict): All parsed outputs of nodes (Format: {node_id:ouput})
    """
    try:
        conn = get_connection(DATABASE_FILE)
        site_outputs = {}
        cursor = conn.execute(GET_PARSED_OUTPUT_BY_SITEID,[site_id])
        for row in cursor:
            node_id = row[1]
            output = row[2]
            site_outputs.update({node_id:output})
        return site_outputs
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def delete_parsed_outputs():
    """
    Clear Parsed_outputs table 
    """
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(DELETE_PARSED_OUTPUTS)
        conn.commit()
        logging.debug('Deleted parsed outputs successfully')
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

# Processing state related functions
def delete_processed_states():
    """
    Clear processing_states table
    """
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(DELETE_PROCESSING_STATE)
        conn.commit()
        logging.debug('Deleted processed states successfully')
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def initialize_processing_state():
    """
    Initialize processing_states table
    """
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
    """
    Update the current processing state of the site

    Args:
        site_id (int)
        state (enum): ('WAITING','COMPLETE','ERRONEOUS','PARSED')
    """
    conn = get_connection(DATABASE_FILE)
    timestamp = datetime.datetime.now()
    try:
        cursor = conn.execute(UPDATE_PROCESSING_STATE,[state,timestamp,site_id])
        conn.commit()
        logging.debug('Updated processing state of site %s to %s successfully',site_id,state)
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def update_processing_state_by_sitename(normalized_site_name,state):
    """
    Update the current processing state of the site by site name

    Args:
        normalized_site_name (str)
        state (enum): ('WAITING','COMPLETE','ERRONEOUS','PARSED')
    """
    site_id = get_siteid_by_normalized_name(normalized_site_name)
    update_processing_state(site_id,state)

def get_processing_state_siteids_by_state(state):
    """
    Get Site IDs with the given state

    Args:
        state (enum): ('WAITING','COMPLETE','ERRONEOUS','PARSED')

    Returns:
        site_ids (list): IDs of sites in the given state
    """
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
    """
    Add jobs of same site batch wise to the database

    Args:
        jobs (list): List of Job IDs
        site_id (int)
    """
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
    """
    Get all jobs of given site

    Args:
        site_id (int)

    Returns:
        jobs (dict): Details of all jobs in the given site
    """
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

def get_all_job_ids_by_abs_state(abstract_state):
    """
    Get all jobs in the database with given abstract state

    Args:
        abstract_state (enum): ('STARTED','ERROR','STALLED','FINISHED')

    Returns:
        job_ids(list): Job IDs of jobs  in given abstract state
    """
    conn = get_connection(DATABASE_FILE)
    try:
        cursor = conn.execute(GET_ALL_JOBS_BY_STATE,[abstract_state])
        job_ids = []
        for row in cursor:
            job_ids.append(row[0])
        return job_ids
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)

def get_jobs_by_siteid_and_abs_state(site_id,abstract_state):
    """
    Get Job ID details of given site with given abstract state

    Args:
        site_id (int)
        abstract_state (enum): ('STARTED','ERROR','STALLED','FINISHED')

    Returns:
        jobs (dict): All job details of given site with given abstract state
    """
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
    """
    Update state of the job

    Args:
        job_id (int)
        abstract_state (enum): ('STARTED','ERROR','STALLED','FINISHED')
        state (enum): JAliEN state of the Job -
        ('K','R','ST','D','DW','W','OW','I','S','SP','SV','SVD','ANY','ASG','AST','FM','IDL','INT','M',
        'SW','ST','TST','EA','EE','EI','EIB','EM','ERE','ES','ESV','EV','EVN','EVT','ESP','EW','EVE',
        'FF','Z','XP','UP','F','INC')
    """
    timestamp = datetime.datetime.now()
    conn = get_connection(DATABASE_FILE)
    try:
        conn.execute(UPDATE_JOB, [timestamp,abstract_state,state,job_id])
        conn.commit()
    except sqlite3.Error as error:
        logging.exception("Error while executing the query: %s", error)
