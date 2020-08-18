# Site Sonar Job Submission Tool
This repository contains the source code for a Site Sonar Job Submission Tool. This tool is used to automate the submission of jobs to the WLCG to analyse the Grid site information.

## Prerequisites:
Please make sure following prerequisites are met before starting the program.
* Start a MySQL instance and source the `db.sql` SQL file to create the database
* Update the database configurations and grid configurations in `config.py`
* Update the job script with your desired commands 
* Enter the `xjalienfs` environment
* Start Site Sonar MonAlisa client

## Updating the Job Script
* The job script is available at `scripts/stat.py`
* Make sure you change only the `monitors` variable in the job script
* The `monitors` is a python dictionary of the following format
> monitors = { identifier : command, identifier : command }
> The identifier will be used as the key when searching the collected information

## Running the Program
If you are running the tool on a local instance, initialized the database using `python3 site-sonar.py init`
> This command is intended to be used only for development purposes. It will be removed in the future. Run only once.

Use the command `python3 site-sonar.py submit` to start job submission 

> The job results will be added to the database by Site Sonar MonAlisa client.

> A full grid run will take around 36 hours. Therefore use `nohup <command> &` to run the `submit` command

## Aborting a Run

If you want to abort the current run use the command `python3 site-sonar.py abort`
> If the tool is running in the background, make sure to bring it to the foreground before using the above command

> If you abort a run, its data will not be available for searching

If you want to abort the run while preserving the collected results, use `--clean` flag. `clean` flag kills the remaining jobs and update the processing states of the sites and the run.
> `python3 site-sonar.py abort --clean`

If the run has completed and you want to kill the remaining jobs, use `--finish` flag. `finish` flag kills the remaining jobs without changing any processing states.
> `python3 site-sonar.py abort --finish`

## Finishing a Run
The run will be marked as `COMPLETED` if any of the following conditions are met.
* All the Grid Sites are marked as `STALLED` or `COMPLETED`
> A Grid site will be marked as `COMPLETED` if 90% of the jobs submitted to it have finished the excution.
> A Grid site will be marked as `STALLED` if none of its jobs have run since 12 hours from the last job execution.

* The run will be marked as `TIMED_OUT` if none of the jobs have completed since 24 hours from the last job execution

A `COMPLETED` or `TIMED_OUT` run is considered as a finished run and its data will be available for search in the website.

* There is a possibility that the run won't meet either of the above conditions. The run will have to be manually marked as `TIMED_OUT` in such a case. Use the command `python3 site-sonar.py abort --clean` for this.
> The remaining jobs will be killed when `abort` command is used

## Starting a New Run
* To start a new run, start a fresh environment by using the command `python3 site-sonar.py reset`
* Submit the jobs using `python3 site-sonar.py submit`

## Obtaining a Run Analysis

* To get a summary of all the runs, use `python3 site-sonar.py summary`
* To get a summary of a specific run, use `python3 site-sonar.py summary -r <run_id>`

## Query Results
You can either use the Site Sonar website or the CLI tool to query the collected results. Use the following command to search the results.
`./site-sonar.py search -r <run_id> -q A="<identifier>: <value>" -eq "<relationship among queries>"`
* To add multiple queries use the format `<next_english_letter>="<identifier>: <value>"`
* Define the relationship as a boolean expression `A & ( B | ~C)`
(Only the english letter in order the symbols [`&`,`|`,`~`,`(`,`)`] are allowed)
> ./site-sonar.py search -r 1 -q A="Singularity: SUPPORTED" -q B="Max Namespaces:15000" -eq "A | B"
