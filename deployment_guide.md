# Deployment Guide

## Setting up the Database

- Create a new MySQL database
- Create a user with all privileges accessible from any IP(Say db-writer) and another user with read only privileges accessible only from localhost(Say db-reader)
- Source https://gitlab.cern.ch/kwijethu/site-sonar/-/blob/master/db.sql to initialize the database tables

## Setting up the Site Sonar CLI Tool

- Clone the repo [Site Sonar](https://gitlab.cern.ch/kwijethu/site-sonar)
- Create a folder named `site-sonar`in the Grid user home
- Create a folder name `outputs` inside that folder
- Set the following configs

```
# User specific configs
GRID_USER_HOME      = '/alice/cern.ch/user/k/kwijethu'  # Path to user's Grid Home directory
GRID_SITE_SONAR_HOME= 'site-sonar'                      # Relative path from User's Grid Home to Site Sonar directory
JOB_TEMPLATE_NAME   = 'job_template.jdl'                # Name of the Job template listed in $GRID_SITE_SONAR_HOME/JDL/ folder
JOB_FACTOR          = 2                                 # Factor of jobs to submit relative to the number of nodes in the Grid site
GRID_SITE_SONAR_OUTPUT_DIR  = 'site-sonar/outputs'      # Relative path from User's Grid Home to results output directory

# Database configs
DB_HOST     = '' # IP of the database host
DB_USER     = '' # Username of the db-writer
DB_PWD      = '' # Password of the db-writer
DB_DATABASE = '' # DB name
```

- Run `./site-sonar.py init` to populate the tables(Running this command clears the existing data in the tables. Therefore run this command only once at the beginning of a new deployment.)
- Run `./site-sonar.py stage` to upload the job files to the Grid
- Run `./site-sonar.py reset` to create a new environment (Do not run this if `init` command was run before this)
- Once the **Site Sonar ML client** is running, run `nohup ./site-sonar.py submit &` to start a new run.

More information on handling a run is available in the [README](https://gitlab.cern.ch/kwijethu/site-sonar/-/blob/master/README.md)

## Setting up Site Sonar ML Client

- Clone the repo [Site Sonar ML Client](https://gitlab.cern.ch/kwijethu/site-sonar-ml-client)
- Follow the [README](https://gitlab.cern.ch/kwijethu/site-sonar-ml-client/-/blob/master/README.md) to start the client

## Building the Site Sonar Website Frontend

- Clone the repo [Site Sonar Frontend](https://gitlab.cern.ch/kwijethu/site-sonar-frontend)
- Run `npm install` to install the dependencies
- Set the environment variable `REACT_APP_BACKEND_URL=<backend_url>` in the `.env` file
- Run `npm run build` to build the website
- Copy the build directory into the **Site Sonar Website Backend** root directory

## Deploying the Site Sonar Website Backend

- Clone the repo [Site Sonar Backend](https://gitlab.cern.ch/kwijethu/site-sonar-backend) to the same host as the DB
- Set the following configurations in the config.py

```
# Database configs
DB_HOST     = 'localhost'
DB_USER     = '' # Username of the db-reader
DB_PWD      = '' # Password of the db-reader
DB_DATABASE = '' # Database name
```

- Deploy the server using `gunicorn`
  `gunicorn3 -w 3 --bind 0.0.0.0:80 backend:app`

## Querying Collected Results

### Using Site Sonar CLI Tool

- Set the `backend URL` in config.py

```
# Search Backend configs
BACKEND_URL = '' # URL to the Backend
```

- Use `summary` and `search` commands to query the results.

### Using the Website

- Visit `backend URL` in your browser
- Use the form given to query the results ( The website will show the sites supporting Singularity at the beginning)
