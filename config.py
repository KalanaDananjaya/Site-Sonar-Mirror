# User specific configs
GRID_USER_HOME      = '/alice/cern.ch/user/k/kwijethu'  # Path to user's Grid Home directory
GRID_SITE_SONAR_HOME= 'site-sonar'                      # Relative path from User's Grid Home to Site Sonar directory
JOB_TEMPLATE_NAME   = 'job_template.jdl'                # Name of the Job template listed in $GRID_SITE_SONAR_HOME/JDL/ folder
JOB_FACTOR          = 2                                 # Factor of jobs to submit relative to the number of nodes in the Grid site
GRID_SITE_SONAR_OUTPUT_DIR  = 'site-sonar/outputs'      # Relative path from User's Grid Home to results output directory

# Database configs 
DB_HOST     = ''
DB_USER     = ''
DB_PWD      = ''
DB_DATABASE = ''

# General configs
LOG_FILE                    = 'site-sonar.log'
OUTPUT_FOLDER               = 'outputs'

# Site data
SITES_CSV_FILE = 'ce_list.csv'

# Search Backend configs
BACKEND_URL = 'http://18.220.133.83:5000'

