# Site-Sonar

A CLI tool to Retrieve the information of Grid nodes and analyze them

Use the following command to run the tool

nohup python3 site-sonar.py bg -s &

python3 site-sonar.py init
python3 site-sonar.py reset
python3 site-sonar.py stage
python3 site-sonar.py submit
python3 site-sonar.py monitor
python3 site-sonar.py fetch
python3 site-sonar.py parse
python3 site-sonar.py kill -a

python3 site-sonar.py search 'key:value'
python3 site-sonar.py search -q 'Underlay: enable underlay = yes' -sid 1