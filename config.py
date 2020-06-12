# User Configs
GRID_USER_HOME = '/alice/cern.ch/user/k/kwijethu'
JOB_TEMPLATE_NAME = 'job_template.jdl'

# Database configs
DATABASE_FILE = 'site-sonar-db.db'
SQL_FILE = 'db.sql'

# General configs
LOG_FILE = 'site-sonar.log'

# Grid sites lists
TEST_SITES_CSV_FILE = 'test_ce_list.csv'
FULL_SITES_CSV_FILE = 'updated_ce_list.csv'
SHORT_SITES_CSV_FILE = 'ce_short_list.csv'
MEDIUM_SITES_CSV_FILE = 'medium_ce_list.csv'
SITES_CSV_FILE = SHORT_SITES_CSV_FILE

# Job states
JOB_GENERAL_STATES = ['K','R','ST','D','DW','W','OW','I','S','SP','SV','SVD','ANY','ASG','AST','FM','IDL','INT','M','SW','ST','TST']
JOB_ERROR_STATES = ['EA','EE','EI','EIB','EM','ERE','ES','ESV','EV','EVN','EVT','ESP','EW','EVE','FF','Z','XP','UP','F','INC']
JOB_RUNNING_STATES = ['R','ST','W','OW','I','S','SP','SV','SVD','ASG','AST','FM','IDL','INT','M','SW','ST','TST']
JOB_WAITING_STATES = ['W','OW']
JOB_STATES = JOB_ERROR_STATES + JOB_GENERAL_STATES

# Time intervals in seconds
SLEEP_BETWEEN_SUBMIT_AND_MONITOR = 30
SLEEP_BETWEEN_MONITOR_PINGS = 30

# Timeouts in hours
JOB_WAITING_TIMEOUT = 0.15