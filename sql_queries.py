# ADD_SITE = 'INSERT INTO sites (site_name,normalized_name,num_nodes,last_update) \
#       VALUES ( %s, %s, %s, %s )'
ADD_SITE = 'INSERT INTO sites (site_name,normalized_name,num_nodes,last_update) \
      VALUES ( %s, %s, %s, NOW())'
GET_SITES = 'SELECT * FROM sites'
GET_SITENAMES = 'SELECT site_name FROM sites'
GET_SITE_IDS = 'SELECT site_id FROM sites'
# GET_SITEID_BY_NAME = 'SELECT site_id FROM sites WHERE site_id = (?)'
# GET_SITEID_BY_NORMALIZED_NAME = 'SELECT site_id FROM sites WHERE normalized_name = (?)'
# GET_NORMALIZED_NAME_BY_SITE_ID = 'SELECT normalized_name FROM sites WHERE site_id = (?)'
# GET_NUM_NODES_IN_SITE = 'SELECT num_nodes FROM sites WHERE site_id = (?)'
UPDATE_LAST_SITE_UPDATE_TIME = 'UPDATE sites set last_update = NOW() WHERE site_id = (%s)'
DELETE_SITES = 'TRUNCATE TABLE sites'

# ADD_NODE = 'INSERT INTO nodes (site_id,node_name) VALUES (?, ?)'
# GET_NODEID_BY_NODE_NAME = 'SELECT node_id FROM nodes WHERE site_id = (?) and node_name = (?)'
DELETE_NODES = 'TRUNCATE TABLE nodes'
 
ADD_JOB = 'INSERT INTO jobs (job_id,run_id,site_id,last_update,job_state) VALUES (%s,%s, %s, NOW(), %s)'
# GET_JOBS_BY_SITEID = 'SELECT * FROM jobs WHERE site_id = (?)'
GET_ALL_JOB_IDS_BY_STATE = 'SELECT job_id FROM jobs WHERE job_state = %s AND run_id = %s'
UPDATE_JOB_STATE_BY_JOBID = 'UPDATE jobs SET last_update=NOW(), job_state=%s WHERE job_id=%s AND run_id = %s'
DELETE_JOBS = 'TRUNCATE TABLE jobs'

INITIALIZE_PROCESSING_STATE = 'INSERT INTO processing_state (site_id,run_id,last_update, state) VALUES (%s, %s, NOW(), %s)'
# GET_PROCESSING_STATE = 'SELECT * FROM processing_state WHERE site_id = (?)'
# GET_PROCESSING_STATE_SITEIDS_BY_STATE = 'SELECT site_id FROM processing_state WHERE state = (?)'
# UPDATE_PROCESSING_STATE = 'UPDATE processing_state SET state = (?),timestamp = (?) WHERE site_id = (?)'
DELETE_PROCESSING_STATE = 'TRUNCATE TABLE processing_state'

# ADD_PARSED_OUTPUT = 'INSERT INTO parsed_outputs (site_id,node_id,parsed_result) VALUES (?, ?, ?)'
# GET_PARSED_OUTPUT_BY_SITEID = 'SELECT * FROM parsed_outputs WHERE site_id = (?)'
DELETE_PARAMETERS = 'TRUNCATE TABLE parameters'

INCREMENT_RUN_ID = 'INSERT INTO run (last_update) values(NOW())'
GET_LAST_RUN_ID = 'SELECT run_id FROM run ORDER BY run_id DESC LIMIT 1'
DELETE_RUN = 'TRUNCATE TABLE run'
