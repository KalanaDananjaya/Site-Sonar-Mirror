ADD_SITE = 'INSERT INTO sites (site_name,normalized_name,num_nodes,last_update) \
      VALUES ( %s, %s, %s, NOW())'
GET_SITES = 'SELECT * FROM sites'
GET_SITENAMES = 'SELECT site_name FROM sites'
GET_SITE_IDS = 'SELECT site_id FROM sites'
UPDATE_LAST_SITE_UPDATE_TIME = 'UPDATE sites SET last_update = NOW() WHERE site_id = (%s)'
DELETE_SITES = 'TRUNCATE TABLE sites'

DELETE_NODES = 'TRUNCATE TABLE nodes'

ADD_JOB = 'INSERT INTO jobs (job_id,run_id,site_id,started_at,last_update,job_state) VALUES (%s,%s, %s, NOW(), NOW(), %s)'
GET_ALL_JOB_IDS_BY_STATE = 'SELECT job_id FROM jobs WHERE job_state = %s AND run_id = %s'
UPDATE_JOB_STATE_BY_JOBID = 'UPDATE jobs SET last_update=NOW(), job_state=%s WHERE job_id=%s AND run_id = %s'
DELETE_JOBS = 'TRUNCATE TABLE jobs'

INITIALIZE_PROCESSING_STATE = 'INSERT INTO processing_state (site_id,run_id,started_at,last_update, state) VALUES (%s, %s, NOW(), NOW(), %s)'
UPDATE_PROCESSING_STATE = 'UPDATE processing_state SET last_update=NOW(),state =%s WHERE site_id=%s AND run_id=%s'
GET_SITE_IDS_BY_PROCESSING_STATE = 'SELECT site_id FROM processing_state WHERE (state=%s) AND (run_id=%s)'

DELETE_PROCESSING_STATE = 'TRUNCATE TABLE processing_state'

DELETE_PARAMETERS = 'TRUNCATE TABLE parameters'

INCREMENT_RUN_ID = 'INSERT INTO run (started_at,last_update,state) values(NOW(), NOW(),%s)'
GET_LAST_RUN_ID = 'SELECT run_id FROM run ORDER BY run_id DESC LIMIT 1'
CHECK_RUN_STATE = 'SELECT state FROM run WHERE run_id=%s'
ABORT_RUN = 'UPDATE run SET state=%s,last_update=NOW() WHERE run_id=%s'
DELETE_RUN = 'TRUNCATE TABLE run'

ADD_KEYS = 'INSERT INTO job_keys (run_id,key_list) VALUES (%s,%s)'
DELETE_KEYS = 'TRUNCATE TABLE job_keys'
