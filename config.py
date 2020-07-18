# User specific configs
GRID_USER_HOME      = '/alice/cern.ch/user/k/kwijethu'  # Path to user's Grid Home directory
GRID_SITE_SONAR_HOME= 'site-sonar'                      # Relative path from User's Grid Home to Site Sonar directory
JOB_TEMPLATE_NAME   = 'job_template.jdl'                # Name of the Job template listed in $GRID_SITE_SONAR_HOME/JDL/ folder
JOB_FACTOR          = 2                                 # Factor of jobs to submit relative to the number of nodes in the Grid site
GRID_SITE_SONAR_OUTPUT_DIR  = 'site-sonar/outputs'      # Relative path from User's Grid Home to results output directory

# Database configs 
DB_HOST     = 'remotemysql.com'
DB_USER     = 'D8h8sG8cqZ'
DB_PWD      = 'avMnBqDCal'
DB_DATABASE = 'D8h8sG8cqZ'

# General configs
LOG_FILE                    = 'site-sonar.log'
OUTPUT_FOLDER               = 'outputs'

# Grid sites lists
TEST_SITES_CSV_FILE     = 'test_ce_list.csv'
FULL_SITES_CSV_FILE     = 'updated_ce_list.csv'
SHORT_SITES_CSV_FILE    = 'ce_short_list.csv'
MEDIUM_SITES_CSV_FILE   = 'medium_ce_list.csv'

SITES_CSV_FILE = TEST_SITES_CSV_FILE # Set to FULL_SITES_CSV_FILE in production env

