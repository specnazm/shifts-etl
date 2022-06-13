# Shifts ETL

Extract shift data from shifts api, transforms it and load to PosgtreSQL.

## Prerequirements
docker-compose and docker


### Start

``` 
  $ git clone https://github.com/specnazm/shifts-etl
  $ cd shifts-etl
  $ docker-compose up -d
```
* Docker-compose will start shifts_api, postgresql and spark cluster. 
* After startup, job is submitted to spark master node from init.sh
* After job is done, tests will be run also inside init.sh
* Jobs take some time to be done. For insights about job status run :

``` 
  $ docker-compose logs --follow spark-master
```
* For first task logs :Extracting data, Transforming and Saving data will be displayed more than once (it's ok :) until all data is collected
* For second task logs are:  Started task : MEAN BREAK TIME, Started task : PAID BREAKS COUNT etc.



* When test are done last line should be:
```
Ran 5 tests in ts
OK
```
* Results can be checked in pgadmin tables
