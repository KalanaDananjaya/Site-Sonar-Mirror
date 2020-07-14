import csv 
import datetime
import time
import logging
import os

from config import SQL_FILE,DB_HOST,DB_USER,DB_PWD,DB_DATABASE
from sql_queries import *

import mysql.connector

# Utils
def normalize_ce_name(target_ce):
    return target_ce.replace("::", "_").lower()[len("alice_"):]

# Connection Functions

def get_connection(auto_commit=True):
    try:
        connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PWD,
        database=DB_DATABASE,
        autocommit=auto_commit
        )
        cursor = connection.cursor()
        return cursor,connection
    except mysql.connector.Error as error:
        logging.error("Error while connecting to MySQL", error)



def clear_tables(all=False):
    """
    Clear Nodes, Jobs, Processing_state, Parsed_outputs tables
    """
    cursor,conn = get_connection(auto_commit=False)
    try:
        if all:
            cursor.execute(DELETE_SITES)
            cursor.execute(DELETE_RUN)
        cursor.execute(DELETE_NODES)
        cursor.execute(DELETE_JOBS)
        cursor.execute(DELETE_PROCESSING_STATE)
        cursor.execute(DELETE_PARAMETERS)
        conn.commit()
        logging.debug('Database tables cleared')
    except mysql.connector.Error as error:
            logging.error("Failed to clear database: {}".format(error))
            conn.rollback()
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()

def increment_run_id():
    cursor,conn = get_connection()
    try:
        cursor.execute(INCREMENT_RUN_ID,['STARTED'])
        logging.debug('Run Id incremented')
    except mysql.connector.Error as error:
        logging.error("Failed to increment run id: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()

def start_new_run():
    cursor,conn = get_connection()
    run_id = get_run_id()
    run_exists = check_run_exists(run_id)
    if not run_exists:
        increment_run_id()
    else:
        logging.error("Cannot start a new run as the last run is still running")

def check_run_exists(run_id):
    cursor,conn = get_connection()
    flag = True
    try:
        cursor.execute(CHECK_RUN_STATE,[run_id])
        state = cursor.fetchone()[0]
        if state == 'STARTED':
            logging.debug('Last run is still running')
            flag = True
        else:
            logging.debug('No currently executing runs')
            flag = False
    except mysql.connector.Error as error:
        logging.error("Failed to increment run id: {}".format(error))
        flag = False
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()
        return flag

def change_run_state(state):
    run_id = get_run_id()
    cursor,conn = get_connection()
    try:
        cursor.execute(ABORT_RUN,[state,run_id])
        logging.debug('Run aborted')
    except mysql.connector.Error as error:
            logging.error("Failed to abort the run: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()

def get_run_id():
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_LAST_RUN_ID)
        run_id = cursor.fetchone()
        return run_id[0]
    except mysql.connector.Error as error:
            logging.error("Failed to get last run id: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()

# Site Related Functions
def get_sites():
    """
    Get all Grid sites

    Returns:
        sites(dict): All site data
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_SITES)
        results = cursor.fetchall()
        sites = []
        for row in results:
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
    except mysql.connector.Error as error:
            logging.error("Failed to retrieve sites: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()

def get_site_ids():
    """
    Get Site IDs

    Returns:
        Site IDs(list)
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(GET_SITE_IDS)
        results = cursor.fetchall()
        ids = []
        for row in results:
            site_id = row[0]
            ids.append(site_id)
        return ids
    except mysql.connector.Error as error:
            logging.debug("Failed to get site ids: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()

def add_sites_from_csv(csv_filename):
    """
    Add Grid sites from CSV file

    Args:
        csv_filename (str): Name of the CSV file (Format: sitename,num_nodes)
    """

    cursor,conn = get_connection()
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
                site_tuples.append((current_site_name, normalized_name, num_nodes))
                logging.debug('Adding %s to the database', current_site_name)
    try:
        cursor.executemany(ADD_SITE, site_tuples)
        conn.commit()
        logging.debug("Total number of sites added : %s", cursor.rowcount)
    except mysql.connector.Error as error:
        logging.error("Failed to add sites to database: {}".format(error))
        conn.rollback()
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()
            logging.debug("connection is closed")

def update_site_last_update_time(site_id):
    """
    Update last update time of the site

    Args:
        site_id (int): Site ID
    """
    cursor, conn = get_connection()
    try:
        cursor.execute(UPDATE_LAST_SITE_UPDATE_TIME,[site_id])
    except mysql.connector.Error as error:
        logging.error("Failed to update site last update times: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()
                

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
    cursor, conn = get_connection()
    sitenames=[]
    try:
        cursor.execute(GET_SITENAMES)
        results = cursor.fetchall()
        for row in results:
            sitenames.append(row[0])
        return sitenames
    except mysql.connector.Error as error:
        logging.error("Failed to get sitenames: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()


def update_processing_state(state,initialize=True):
    """
    Update processing_states table

    Return True if succesfull
    """
    success_flag = False
    run_id = get_run_id()
    cursor, conn = get_connection(auto_commit=False)
    try:
        if initialize:
            site_ids = get_site_ids()
            state_tuple = []
            for site_id in site_ids:
                state_tuple.append((site_id,run_id,state))
            cursor.executemany(INITIALIZE_PROCESSING_STATE,state_tuple)
        else:
            site_ids = get_sites_by_processing_state('WAITING')
            state_tuple = []
            for site_id in site_ids:
                state_tuple.append((state, site_id,run_id))
            cursor.executemany(UPDATE_PROCESSING_STATE,state_tuple)
        conn.commit()
        logging.debug('Update processing states to %s successfully',state)
        success_flag = True
    except mysql.connector.Error as error:
        logging.error("Failed to update processing states: {}".format(error))
        success_flag = False
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()
        return success_flag

def get_sites_by_processing_state(state):
    run_id = get_run_id()
    site_ids = []
    cursor, conn = get_connection()
    try: 
        cursor.execute(GET_SITE_IDS_BY_PROCESSING_STATE,(state,run_id))
        results = cursor.fetchall()
        for row in results:
            site_ids.append(row[0])
    except mysql.connector.Error as error:
        logging.error("Failed to fetch processing states: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()
        return site_ids
        
# Job related functions

def add_job(job_id,site_id):
    """
    Add jobs to the database

    Args:
        job_id (str): Job ID
        site_id (int)
    """
    cursor, conn = get_connection()
    try:
        run_id = get_run_id()
        cursor.execute(ADD_JOB, (job_id,run_id, site_id, 'STARTED'))
        logging.debug('Job %s added to database succesfully',job_id.strip())
    except mysql.connector.Error as error:
        logging.error("Failed to add job: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()

def get_all_job_ids_by_state(state):
    """
    Get all jobs in the database with given state

    Args:
        state (enum): ('STARTED','STALLED','COMPLETED','KILLED')

    Returns:
        job_ids(list): Job IDs of jobs in given  state
    """
    cursor, conn = get_connection()
    try:
        run_id = get_run_id()
        cursor.execute(GET_ALL_JOB_IDS_BY_STATE,[state,run_id])
        results = cursor.fetchall()
        job_ids = []
        for row in results:
            job_ids.append(row[0])
        return job_ids
    except mysql.connector.Error as error:
        logging.error("Failed to get jobs by state: {}".format(error))
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()

def update_job_state_by_job_id(job_id,state):
    """
    Update state of the job

    Args:
        job_id (int): Single id or id list
        state (enum): ('STARTED','ERROR','STALLED','COMPLETED','KILLED')
    """
    cursor, conn = get_connection(auto_commit=False)
    run_id = get_run_id()
    job_tuple = []
    if type(job_id) == str:
        job_tuple.append((state,job_id,run_id))
    elif type(job_id) == list: 
        for id in job_id:
            job_tuple.append((state,id,run_id))
    try:
        cursor.executemany(UPDATE_JOB_STATE_BY_JOBID, job_tuple)
        conn.commit()
    except mysql.connector.Error as error:
        logging.error("Failed to update job state: {}".format(error))
        conn.rollback()
    finally:
        if(conn.is_connected()):
            cursor.close()
            conn.close()




# def initialize_db():
#     """
#     Initialize database using SQL File

#     Args:
#         SQL_FILE(str): Set from config
#     """
#     cursor,conn = get_connection(auto_commit=False)
#     with open (SQL_FILE,'r') as f:
#         sql_lines = f.read().split(';')
#         sql = f.read()
#         try:
#             cursor.execute(sql)
#             conn.commit()
#             logging.info('Database initialized succesfully')
#         except mysql.connector.Error as error:
#             logging.error("Failed to create tables in MySQL: {}".format(error))
#             conn.rollback()
#         finally:
#             if(conn.is_connected()):
#                 cursor.close()
#                 conn.close()

# def get_num_nodes_in_site(site_id):
#     """
#     Get the number of nodes in site

#     Args:
#         site_id (int])

#     Returns:
#         Number of nodes
#     """
#     conn = get_connection(DATABASE_FILE)
#     try:
#         cursor = conn.execute(GET_NUM_NODES_IN_SITE,[site_id])
#         for row in cursor:
#             return row[0]
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)

# def get_siteid_by_normalized_name(normalized_name):
#     """
#     Get Site ID by the normalized name

#     Args:
#         normalized_name (str)

#     Returns:
#         site_id
#     """
#     logging.debug('Searching id for %s...', normalized_name)
#     conn = get_connection(DATABASE_FILE)
#     try:
#         cursor = conn.execute(GET_SITEID_BY_NORMALIZED_NAME,[normalized_name])
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)
#     rows = cursor.fetchall()
#     for row in rows:
#         site_id = row[0]
#         logging.debug ('Site_id of %s: %s',normalized_name, site_id)
#         return site_id

# def get_normalized_name_by_siteid(site_id):
#     """
#     Get normalized name by Site ID

#     Args:
#         site_id (int)

#     Returns:
#         normalized_name(str)
#     """
#     logging.debug('Searching normalized name for site with id: %s...', str(site_id))
#     conn = get_connection(DATABASE_FILE)
#     try:
#         cursor = conn.execute(GET_NORMALIZED_NAME_BY_SITE_ID,[site_id])
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)
#     rows = cursor.fetchall()
#     for row in rows:
#         normalized_name = row[0]
#         logging.debug ('Normalized name of site %s: %s',str(site_id),normalized_name)
#         return normalized_name


# Node related functions
# def get_nodeid_by_node_name(site_id,node_name):
#     """
#     Get Node Id by Node Name

#     Args:
#         site_id (int)
#         node_name (str): Name of the node

#     Returns:
#         node_id (int)
#     """
#     conn = get_connection(DATABASE_FILE)
#     try:
#         cursor = conn.execute(GET_NODEID_BY_NODE_NAME,[site_id, node_name])
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)
#     rows = cursor.fetchall()
#     result_len = len(rows)
#     if result_len == 0:
#         logging.debug('Node does not exist')
#         return False
#     else:
#         for row in rows:
#             node_id = row[0]
#             return node_id

# def add_node(site_id,node_name):
#     """
#     Add node to the database

#     Args:
#         site_id (int)
#         node_name (str): Name of the node

#     Returns:
#         node_id (int): Generated Node ID
#     """
#     conn = get_connection(DATABASE_FILE)
#     try:
#         cursor = conn.execute(ADD_NODE,[site_id, node_name])
#         conn.commit()
#         node_id  = cursor.lastrowid
#         return node_id
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)

# Parsing related functions
# def add_parsed_output(site_id,node_id,parsed_result):
#     """
#     Add parsed output to the database

#     Args:
#         site_id (int)
#         node_id (int)
#         parsed_result (json string): Parsed output dict converted to JSON string
#     """
#     conn = get_connection(DATABASE_FILE)
#     try:
#         cursor = conn.execute(ADD_PARSED_OUTPUT,[site_id, node_id,parsed_result])
#         conn.commit()
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)

# def add_parsed_output_by_names(normalized_site_name,node_name,parsed_result):
#     """
#     Add parsed output to the database if the a duplicate result has not been added already

#     Args:
#         normalized_site_name (str)
#         node_name ([str)
#         parsed_result (json string): Parsed output dict converted to JSON string

#     Returns:
#         Bool: True if succesful
#     """
#     site_id = get_siteid_by_normalized_name(normalized_site_name)
#     node_id = get_nodeid_by_node_name(site_id, node_name)
#     if not node_id:
#         node_id = add_node(site_id,node_name)
#         logging.debug ('Assigned Node Id %s to %s of site %s',node_id,node_name,site_id)
#         add_parsed_output(site_id,node_id,parsed_result)
#     return True

# def get_parsed_output_by_siteid(site_id):
#     """
#     Get all parsed outputs of the site nodes

#     Args:
#         site_id (int)

#     Returns:
#         site_outputs (dict): All parsed outputs of nodes (Format: {node_id:ouput})
#     """
#     try:
#         conn = get_connection(DATABASE_FILE)
#         site_outputs = {}
#         cursor = conn.execute(GET_PARSED_OUTPUT_BY_SITEID,[site_id])
#         for row in cursor:
#             node_id = row[1]
#             output = row[2]
#             site_outputs.update({node_id:output})
#         return site_outputs
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)

# def delete_parsed_outputs():
#     """
#     Clear Parsed_outputs table 
#     """
#     conn = get_connection(DATABASE_FILE)
#     try:
#         cursor = conn.execute(DELETE_PARAMETERS)
#         conn.commit()
#         logging.debug('Deleted parsed outputs successfully')
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)

# Processing state related functions
# def delete_processed_states():
#     """
#     Clear processing_states table
#     """
#     cursor, conn = get_connection()
#     try:
#         cursor.execute(DELETE_PROCESSING_STATE)
#         logging.debug('Deleted processed states successfully')
#     except mysql.connector.Error as error:
#         logging.error("Failed to delete processed states: {}".format(error))
#     finally:
#         if(conn.is_connected()):
#             cursor.close()
#             conn.close()

# def update_processing_state(site_id,state):
#     """
#     Update the current processing state of the site

#     Args:
#         site_id (int)
#         state (enum): ('WAITING','COMPLETE','ERRONEOUS','PARSED')
#     """
#     conn = get_connection(DATABASE_FILE)
#     ts = time.time()
#     timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
#     try:
#         cursor = conn.execute(UPDATE_PROCESSING_STATE,[state,timestamp,site_id])
#         conn.commit()
#         logging.debug('Updated processing state of site %s to %s successfully',site_id,state)
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)

# def update_processing_state_by_sitename(normalized_site_name,state):
#     """
#     Update the current processing state of the site by site name

#     Args:
#         normalized_site_name (str)
#         state (enum): ('WAITING','COMPLETE','ERRONEOUS','PARSED')
#     """
#     site_id = get_siteid_by_normalized_name(normalized_site_name)
#     update_processing_state(site_id,state)

# def get_processing_state_siteids_by_state(state):
#     """
#     Get Site IDs with the given state

#     Args:
#         state (enum): ('WAITING','COMPLETE','ERRONEOUS','PARSED')

#     Returns:
#         site_ids (list): IDs of sites in the given state
#     """
#     conn = get_connection(DATABASE_FILE)
#     try:
#         cursor = conn.execute(GET_PROCESSING_STATE_SITEIDS_BY_STATE,[state])
#         site_ids = []
#         for row in cursor:
#             site_ids.append(row[0])
#         return site_ids
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)




# def get_all_jobs_by_site_id(site_id):
#         cursor = conn.execute(GET_JOBS_BY_SITEID,[site_id])
#         jobs = []
#         for row in cursor:
#             job_id = row[0]
#             site_id = row[1]
#             timestamp = row[2]
#             abstract_state = row[3]
#             state = row[4]
#             job = {
#                 'job_id': job_id,
#                 'site_id': site_id,
#                 'timestamp': timestamp,
#                 'abstract_state': abstract_state,
#                 'state': state
#                 }
#             jobs.append(job)  
#         return jobs
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)



# def get_jobs_by_siteid_and_abs_state(site_id,abstract_state):
#     """
#     Get Job ID details of given site with given abstract state

#     Args:
#         site_id (int)
#         abstract_state (enum): ('STARTED','ERROR','STALLED','FINISHED')

#     Returns:
#         jobs (dict): All job details of given site with given abstract state
#     """
#     conn = get_connection(DATABASE_FILE)
#     try:
#         cursor = conn.execute(GET_INCOMPLETE_JOBS_BY_SITEID,[site_id,abstract_state])
#         jobs = []
#         for row in cursor:
#             job_id = row[0]
#             site_id = row[1]
#             timestamp = row[2]
#             abstract_state = row[3]
#             state = row[4]
#             job = {
#                 'job_id': job_id,
#                 'site_id': site_id,
#                 'timestamp': timestamp,
#                 'abstract_state': abstract_state,
#                 'state': state
#                 }
#             jobs.append(job)  
#         return jobs
#     except sqlite3.Error as error:
#         logging.exception("Error while executing the query: %s", error)

