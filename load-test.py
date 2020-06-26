import argparse
import logging
from datetime import timedelta, datetime
from multiprocessing import Pool
from random import randint
from time import sleep
import json

import pymongo


def run_queries(n, client, db, coll, query):
    # This is a sample query. You can write your own!
    c = client[db][coll]
    c.find(query)
    # Any relevant debug output can be written to the logs.
    #logging.debug('Proc{} query'.format(n))


def run_updates(n, client, db, coll, update):
    doc = update['doc']
    select = update['filter']
    c = client[db][coll]
    c.update(select, doc)
    #logging.debug('Proc{} update'.format(n))


def run_deletes(n, client, db, coll, delete):
    logging.debug('Proc{} delete'.format(n))


def run_inserts(n, client, db, coll, insert):
    c = client[db][coll]
    c.update(insert)
    logging.debug('Proc{} insert'.format(n))


def run_aggregations(n, client, db, coll, agg):
    logging.debug('Proc{0} aggregation'.format(n))


# -----------------------------------------------
# These are global variables used by all processes.
# TODO: pass them directly to loop.
mongo_uri = None
max_qry = 0
max_upd = 0
max_ins = 0
max_agr = 0
max_del = 0
seconds_to_run = 0

query_doc = None
query_coll = None
query_db = None

update_doc = None
update_filter = None
update_coll = None
update_db = None

delete_doc = None
delete_coll = None
delete_db = None

insert_doc = None
insert_coll = None
insert_db = None

aggregation_pipe = None
aggregation_coll = None
aggregation_db = None


def millis_interval(start, end):
    """start and end are datetime instances"""
    diff = end - start
    millis = diff.days * 24 * 60 * 60 * 1000
    millis += diff.seconds * 1000
    millis += diff.microseconds / 1000
    return millis


def test_db(proc_number):
    global mongo_uri
    global max_qry, max_upd, max_ins, max_agr, max_del
    global seconds_to_run

    # For time tracking
    process_start = datetime.now()

    ops_left = max_qry + max_upd + max_ins + max_agr + max_del
    logging.info('Process {} will run {} operations per second'.format(proc_number, ops_left))

    # Here we have safely created the mongoClient object in its own process.
    client = pymongo.MongoClient(mongo_uri)

    #
    process_end = process_start + timedelta(seconds=seconds_to_run)
    while datetime.now() < process_end:
        period_start = datetime.now()
        ops_left = max_qry + max_upd + max_ins + max_agr + max_del

        # Initially we support queries, updates, deletes, inserts and aggregations,
        # but we could extend this to other commands.
        operations = [
            {'name': 'query', 'counter': 0, 'max': max_qry, 'run': run_queries,
             'db': query_db, 'coll': query_coll, 'op': query_doc},
            {'name': 'update', 'counter': 0, 'max': max_upd, 'run': run_updates,
             'db': update_db, 'coll': update_coll, 'op': {'doc': update_doc, 'filter': update_filter}},
            {'name': 'delete', 'counter': 0, 'max': max_del, 'run': run_deletes,
             'db': delete_db, 'coll': delete_coll, 'op': delete_doc},
            {'name': 'insert', 'counter': 0, 'max': max_ins, 'run': run_inserts,
             'db': insert_db, 'coll': insert_coll, 'op': insert_doc},
            {'name': 'aggregate', 'counter': 0, 'max': max_agr, 'run': run_aggregations,
             'db': aggregation_db, 'coll': aggregation_coll, 'op': aggregation_pipe}
        ]

        logging.debug('Process {} will run {} operations per second'.format(proc_number, ops_left))

        # start operations this period
        while ops_left > 0:
            # Select one operation type at random
            which_op = randint(0, len(operations)-1)

            # Are we done with this kind or operations?
            op = operations[which_op]
            if op['counter'] >= op['max']:
                continue  # next op

            # execute
            op['run'](proc_number, client, op['db'], op['coll'], op['op'])
            op['counter'] += 1

            # count ops left
            ops_left = 0
            for op in operations:
                ops_left += op['max']-op['counter']

        # Sleep until end period?
        time_used = datetime.now() - period_start
        time_used_millis = ((time_used.seconds * 1000.0) + (time_used.microseconds / 1000.0))
        if time_used_millis > 1000:
            logging.debug("Proc {}: Operations took more than one second to run {} milliseconds."
                          .format(proc_number, time_used_millis))
        else:
            time_left_millis = 1000.0 - time_used_millis
            logging.debug('Process {} sleeping {} ms'.format(proc_number, time_left_millis))
            if time_left_millis > 25:
                sleep(time_left_millis/1000.0)

    # Like nice clients, we close our connections before leaving.
    client.close()

    return datetime.now() - process_start


def print_results(results, n_threads):
    total_query_time = sum([run for run in results], timedelta())
    avg_query_time = total_query_time / n_threads
    max_query_time = max([run for run in results])
    min_query_time = min([run for run in results])
    logging.info('{} Processes running for a total time of {} max = {} avg = {} min = {}'.format(
        n_threads,
        total_query_time,
        max_query_time,
        avg_query_time,
        min_query_time,
    ))


def perf_test(arguments):
    global max_qry, max_upd, max_ins, max_agr, max_del
    global mongo_uri
    global seconds_to_run
    global query_doc, query_coll, query_db
    global update_filter, update_doc, update_coll, update_db
    global delete_doc, delete_coll, delete_db
    global insert_doc, insert_coll, insert_db
    global aggregation_pipe, aggregation_coll, aggregation_db

    mongo_uri = arguments['uri']
    seconds_to_run = arguments['seconds']
    n_threads = arguments['processes']
    max_agr = arguments['aggregations']
    max_del = arguments['deletes']
    max_ins = arguments['inserts']
    max_upd = arguments['updates']
    max_qry = arguments['queries']

    try:
        query_doc = json.loads(arguments['query_doc'])
        query_coll = arguments['query_coll']
        query_db = arguments['query_db']

        update_doc = json.loads(arguments['update_doc'])
        update_filter = json.loads(arguments['update_filter'])
        update_coll = arguments['update_coll']
        update_db = arguments['update_db']

        delete_doc = json.loads(arguments['delete_doc'])
        delete_coll = arguments['delete_coll']
        delete_db = arguments['delete_db']

        insert_doc = json.loads(arguments['insert_doc'])
        insert_coll = arguments['insert_coll']
        insert_db = arguments['insert_db']
    except json.JSONDecodeError as e:
        logging.critical('Error decoding JSON document: ' + e.msg)
        return

    pool = Pool(n_threads)
    logging.info('Running {} process(es) for {} seconds'.format(n_threads, seconds_to_run))

    results = pool.map(test_db, range(n_threads))
    pool.close()
    pool.join()

    print_results(results, n_threads)


if __name__ == "__main__":

    ap = argparse.ArgumentParser()

    ap.add_argument('--uri', required=False,
                    help='MongoDB URI. Default mongodb://127.0.0.1:27017/admin', type=str, default='mongodb://127.0.0.1:27017/admin')
    ap.add_argument('--processes', required=False,
                    help='Number of processes to run. Default 2', type=int, default=2)
    ap.add_argument('--logfile', required=False,
                    help='log file to write to', type=str)
    ap.add_argument('--seconds', required=False,
                    help='Number of seconds to run (default 300)', type=int, default=300)
    ap.add_argument('--verbose', required=False,
                    help='verbosity. Default False', type=bool, default=False)
    ap.add_argument('--queries', required=False,
                    help='Number of queries to execute per process', type=int, default=0)
    ap.add_argument('--updates', required=False,
                    help='Number of updates to execute per process', type=int, default=0)
    ap.add_argument('--inserts', required=False,
                    help='Number of insert operations to execute per process', type=int, default=0)
    ap.add_argument('--aggregations', required=False,
                    help='Number of aggregation operations to execute per process', type=int, default=0)
    ap.add_argument('--deletes', required=False,
                    help='Number of delete operations to execute per process', type=int, default=0)

    ap.add_argument('--query_doc', required=False,  help='Query', type=str, default='{}')
    ap.add_argument('--query_db', required=False,  help='Query database', type=str, default='test')
    ap.add_argument('--query_coll', required=False, help='Query collection', type=str, default='foo')

    ap.add_argument('--update_filter', required=False, help='Update filter', type=str, default='{}')
    ap.add_argument('--update_doc', required=False, help='Update document', type=str, default='{}')
    ap.add_argument('--update_db', required=False, help='Update database', type=str, default='test')
    ap.add_argument('--update_coll', required=False, help='Update collection', type=str, default='foo')

    ap.add_argument('--insert_doc', required=False, help='Insert doc', type=str, default='{}')
    ap.add_argument('--insert_db', required=False, help='Insert database', type=str, default='test')
    ap.add_argument('--insert_coll', required=False, help='Insert collection', type=str, default='foo')

    ap.add_argument('--delete_doc', required=False, help='Delete doc', type=str, default='{}')
    ap.add_argument('--delete_db', required=False, help='Delete database', type=str, default='test')
    ap.add_argument('--delete_coll', required=False, help='Delete collection', type=str, default='foo')

    ap.add_argument('--aggregation_doc', required=False, help='Aggregation pipeline', type=str, default='{}')
    ap.add_argument('--aggregation', required=False, help='Aggregation database', type=str, default='test')
    ap.add_argument('--aggregation_coll', required=False, help='Aggregation collection', type=str, default='foo')

    args = vars(ap.parse_args())

    if args['verbose']:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    if 'logfile' in args:
        logging.basicConfig(filename=args['logfile'], format='%(asctime)s %(message)s', level=log_level)
    else:
        logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)

    perf_test(args)
