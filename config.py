# User specific configs
# Path to user's Grid Home directory
GRID_USER_HOME = '/alice/cern.ch/user/k/kwijethu'
# Relative path from User's Grid Home to Site Sonar directory
GRID_SITE_SONAR_HOME = 'site-sonar'
# Name of the Job template listed in $GRID_SITE_SONAR_HOME/JDL/ folder
JOB_TEMPLATE_NAME = 'job_template.jdl'
# Factor of jobs to submit relative to the number of nodes in the Grid site
JOB_FACTOR = 2
# Relative path from User's Grid Home to results output directory
GRID_SITE_SONAR_OUTPUT_DIR = 'site-sonar/outputs'

# Database configs
DB_HOST = ''
DB_USER = ''
DB_PWD = ''
DB_DATABASE = ''

# General configs
LOG_FILE = 'site-sonar.log'
OUTPUT_FOLDER = 'outputs'

# Site data
SITES_CSV_FILE = 'ce_list.csv'

# Search Backend configs
BACKEND_URL = 'http://<ip_of_the_backend>'
