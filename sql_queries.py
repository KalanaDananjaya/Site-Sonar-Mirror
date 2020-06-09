ADD_SITE = 'INSERT INTO SITES (site_name,normalized_name,num_nodes,remarks) \
      VALUES ( ?, ?, ?, ? )'
GET_SITES = 'SELECT * FROM sites'
GET_SITENAMES = 'SELECT site_name FROM sites'
GET_SITE_IDS = 'SELECT site_id FROM sites'
GET_SITEID_BY_NAME = 'SELECT site_id FROM sites where site_id = (?)'
GET_SITEID_BY_NORMALIZED_NAME = 'SELECT site_id FROM sites where normalized_name = (?)'

ADD_NODE = 'INSERT INTO nodes (site_id,node_name) VALUES (?, ?)'
GET_NODEID_BY_NODE_NAME = 'SELECT node_id FROM nodes WHERE site_id = (?) and node_name = (?)'
 
ADD_JOBS = 'INSERT INTO jobs (job_id,site_id,timestamp,abstract_state,state) VALUES (?, ?, ?, ?, ?)'
GET_JOBS_BY_SITEID = 'SELECT * FROM jobs WHERE site_id = (?)'
GET_INCOMPLETE_JOBS_BY_SITEID = 'SELECT * FROM jobs WHERE site_id = (?) AND abstract_state = (?)'
UPDATE_JOB = 'UPDATE jobs SET timestamp=(?),abstract_state=(?),state=(?) WHERE job_id=(?)'

INITIALIZE_PROCESSING_STATE = 'INSERT INTO processing_state (site_id,timestamp,state) VALUES (?, ?, ?)'
GET_PROCESSING_STATE = 'SELECT * FROM processing_state WHERE site_id = (?)'
UPDATE_PROCESSING_STATE = 'UPDATE processing_state SET state = (?) WHERE site_id = (?)'
DELETE_PROCESSING_STATE = 'DELETE FROM processing_state'
GET_PROCESSING_STATE_SITEIDS_BY_STATE = 'SELECT site_id FROM processing_state WHERE state = (?)'

ADD_PARSED_OUTPUT = 'INSERT INTO parsed_outputs (site_id,node_id,parsed_result) VALUES (?, ?, ?)'
DELETE_PARSED_OUTPUTS = 'DELETE FROM parsed_outputs'