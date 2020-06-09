DROP TABLE IF EXISTS sites;
CREATE TABLE sites
(site_id INTEGER PRIMARY KEY AUTOINCREMENT,
site_name text NOT NULL,
normalized_name text NOT NULL,
num_nodes int NOT NULL,
remarks text);

DROP TABLE IF EXISTS nodes;
CREATE TABLE nodes
(node_id INTEGER PRIMARY KEY AUTOINCREMENT,
site_id int NOT NULL,
node_name text NOT NULL);

DROP TABLE IF EXISTS jobs;
CREATE TABLE jobs
(job_id int NOT NULL PRIMARY KEY,
site_id int NOT NULL,
timestamp int NOT NULL,
abstract_state text CHECK(abstract_state IN ('STARTED','FINISHED') ) NOT NULL,
state text CHECK(state IN ('K','R','ST','D','DW','W','OW','I','S','SP','SV','SVD','ANY','ASG','AST','FM','IDL','INT','M','SW','ST','TST','EA','EE','EI','EIB','EM','ERE','ES','ESV','EV','EVN','EVT','ESP','EW','EVE','FF','Z','XP','UP','F','INC') ) NOT NULL);

DROP TABLE IF EXISTS parsed_outputs;
CREATE TABLE parsed_outputs
(site_id int NOT NULL,
node_id int NOT NULL,
parsed_result text,
PRIMARY KEY (site_id,node_id));

DROP TABLE IF EXISTS processing_state;
CREATE TABLE processing_state
(site_id int NOT NULL PRIMARY KEY,
timestamp int NOT NULL,
state text CHECK(state IN ('WAITING','COMPLETE','PARSED','PROCESSED') ) NOT NULL);