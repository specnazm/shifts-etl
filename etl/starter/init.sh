pip3 install requests
cd home/app
/spark/bin/spark-submit  --driver-class-path /home/app/postgresql-42.4.0.jar  jobs/etl_job.py