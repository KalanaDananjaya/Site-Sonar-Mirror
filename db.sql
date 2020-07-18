DROP TABLE IF EXISTS sites;
CREATE TABLE sites
(site_id INTEGER PRIMARY KEY AUTO_INCREMENT,
site_name varchar(50) NOT NULL,
normalized_name varchar(50) NOT NULL,
num_nodes int NOT NULL,
last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);

DROP TABLE IF EXISTS run;
CREATE TABLE run
(run_id int PRIMARY KEY AUTO_INCREMENT,
started_at TIMESTAMP NOT NULL,
last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
state ENUM ('STARTED','COMPLETED','TIMED_OUT','ABORTED')
);

DROP TABLE IF EXISTS processing_state;
CREATE TABLE processing_state
(site_id int NOT NULL,
run_id int NOT NULL,
started_at TIMESTAMP NOT NULL,
last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
state ENUM ('WAITING','COMPLETED','STALLED','ABORTED')  NOT NULL,
running_job_count int,
completed_job_count int,
killed_job_count int,
PRIMARY KEY(site_id,run_id),
FOREIGN KEY (site_id) references sites(site_id),
FOREIGN KEY (run_id) references run(run_id)); 

DROP TABLE IF EXISTS nodes;
CREATE TABLE nodes
(node_id INTEGER PRIMARY KEY AUTO_INCREMENT,
run_id int NOT NULL,
site_id int NOT NULL,
node_name varchar(50) NOT NULL,
FOREIGN KEY (site_id) references sites(site_id),
FOREIGN KEY (run_id) references run(run_id));

DROP TABLE IF EXISTS jobs;
CREATE TABLE jobs
(job_id int NOT NULL PRIMARY KEY,
run_id int NOT NULL,
site_id int NOT NULL,
started_at TIMESTAMP NOT NULL,
last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
job_state ENUM ('STARTED','STALLED','COMPLETED','KILLED')  NOT NULL,
FOREIGN KEY (site_id) references sites(site_id),
FOREIGN KEY (run_id) references run(run_id)); 

DROP TABLE IF EXISTS parameters;
CREATE TABLE parameters
(job_id int NOT NULL, 
run_id int NOT NULL,
site_id int NOT NULL,
node_id int NOT NULL,
paramName varchar(500) NOT NULL,
paramValue varchar(500) NOT NULL,
last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (job_id,paramName),
FOREIGN KEY (site_id) references sites(site_id),
FOREIGN KEY (run_id) references run(run_id),
FOREIGN KEY (job_id) references jobs(job_id));


