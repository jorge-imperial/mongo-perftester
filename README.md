# mongodb performance tester
A python script to create workload on a MongoDB database.


This script will start a defined number of processes that can run a defined workload for a MongoDB database.


The main use-cases

1 - You want to know what are the limits on a given MongoDB environment: how many operations (find(), update(), inserts(), deletes(), aggregate()) before the response degrades.

2 - You want to reproduce a given work load to investigate behaviors, root-cause-analysis.


Usage:

```
usage: load-test.py [-h] [--uri URI] [--processes PROCESSES]
                    [--logfile LOGFILE] [--seconds SECONDS]
                    [--verbose VERBOSE] [--queries QUERIES]
                    [--updates UPDATES] [--inserts INSERTS]
                    [--aggregations AGGREGATIONS] [--deletes DELETES]

optional arguments:
  -h, --help            show this help message and exit
  --uri URI             MongoDB URI. Default mongodb://127.0.0.1:27017/admin
  --processes PROCESSES
                        Number of processes to run. Default 2
  --logfile LOGFILE     log file to write to
  --seconds SECONDS     Number of seconds to run (default 300)
  --verbose VERBOSE     verbosity. Default False
  --queries QUERIES     Number of queries to execute per process
  --updates UPDATES     Number of updates to execute per process
  --inserts INSERTS     Number of insert operations to execute per process
  --aggregations AGGREGATIONS
                        Number of aggregation operations to execute per
                        process
  --deletes DELETES     Number of delete operations to execute per process

```

As an example, the command line

_python3 load-test.py  --processes 3 --queries 5 --updates 3 --verbose True --uri "mongodb://user:P4ssw0rd@narval:27017,narval:27018,narval:27019/admin?replicaSet=rs1"_

will start three processes that connect to the replica set _rs1_ running on the host Narval:27017, Narval:27018, and Narval:27019. It will authenticate as user/P4ssw0rd.

Each of the processes will run* 5 queries and 3 updates per second.

The operations must be specified in the script, by modifying these methods:

```
def run_queries(n, client):
    # This is a sample query. You can write your own!
    age = randint(18, 65)
    collection = client.test['onemill']
    collection.find({'age': age})
    # Any relevant debug output can be written to the logs.
    #logging.debug('Proc{} query'.format(n))

def run_updates(n, client):
    age = randint(18, 65)
    collection = client.test['onemill']
    collection.update_one({'age': age}, {'$set': {'m': randint(0, 1000)}})
    #logging.debug('Proc{} update'.format(n))

def run_deletes(n):
    logging.debug('Proc{} delete'.format(n))

def run_inserts(n):
    logging.debug('Proc{} insert'.format(n))

def run_aggregations(n):
    logging.debug('Proc{0} aggregation'.format(n))

```

*Try to run.
