# User specific configs
GRID_USER_HOME = '/alice/cern.ch/user/k/kwijethu'   # Path to user's Grid Home directory
JOB_TEMPLATE_NAME = 'job_template.jdl'              # Name of the Job template listed in JDL/ folder
JOB_FACTOR = 2                                      # Factor of jobs to submit relative to the number of nodes in the Grid site
RESULTS_DOWNLOAD_FOLDER = 'outputs'                     # Relative to root directory of the repository

# Database configs
DATABASE_FILE = 'site-sonar-db.db' 
SQL_FILE = 'db.sql'

# General configs
LOG_FILE = 'site-sonar.log'
OUTPUT_FOLDER = 'outputs'
GRID_SITE_SONAR_HOME = 'site-sonar'
GRID_SITE_SONAR_OUTPUT_DIR = 'site-sonar/outputs'

# Time intervals in seconds
SLEEP_BETWEEN_MONITOR_PINGS = 3600 # Set to 3600 in production environment
SLEEP_BETWEEN_MONITOR_AND_PARSER = 3600 # Set to 3600 in production environment
SLEEP_BETWEEN_PARSER_PINGS = 3600 # Set to 3600 in production environment

# Timeouts in hours
JOB_WAITING_TIMEOUT = 6 # Set to 6 in production environment

# Grid sites lists
TEST_SITES_CSV_FILE = 'test_ce_list.csv'
FULL_SITES_CSV_FILE = 'updated_ce_list.csv'
SHORT_SITES_CSV_FILE = 'ce_short_list.csv'
MEDIUM_SITES_CSV_FILE = 'medium_ce_list.csv'
SITES_CSV_FILE = TEST_SITES_CSV_FILE # Set to FULL_SITES_CSV_FILE in production env

# Job states
JOB_GENERAL_STATES = ['K','R','ST','D','DW','W','OW','I','S','SP','SV','SVD','ANY','ASG','AST','FM','IDL','INT','M','SW','ST','TST']
JOB_ERROR_STATES = ['EA','EE','EI','EIB','EM','ERE','ES','ESV','EV','EVN','EVT','ESP','EW','EVE','FF','Z','XP','UP','F','INC']
JOB_RUNNING_STATES = ['R','ST','W','OW','I','S','SP','SV','SVD','ASG','AST','FM','IDL','INT','M','SW','ST','TST']
JOB_WAITING_STATES = ['W','OW']
JOB_STATES = JOB_ERROR_STATES + JOB_GENERAL_STATES
