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
name text NOT NULL);

DROP TABLE IF EXISTS jobs;
CREATE TABLE jobs
(job_id int NOT NULL PRIMARY KEY,
site_id int NOT NULL,
timestamp int NOT NULL,
status text CHECK( status IN ('STARTED','FINISHED') ) NOT NULL);