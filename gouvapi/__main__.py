#!/usr/env/bin python

from gouvapi import fetch_job, process_job
from gouvapi import _kpis as kpis
from gouvapi.api import API

from multiprocessing.dummy import Pool
import csv
import os


def fetch(output, increment=10, threads=2):
    '''
    Run fetch jobs in multithreading mode.
        outputfile    Output file name.
        increment     Datasets fetched by each job
        threads       Threads to run
    '''

    # Init output file
    with open(output, 'w') as fileh:
        fileh.write('dataset|id|url\n')
        fileh.close()

    # Get a single dataset page with one element.
    # Only used to compute jobs to run.
    api = API()
    first_dataset = api.datasets(page_size=1, page=1).get()

    # Compute nomber of pages to fetch according to increment and number of
    # datasets in fetch.
    pages_to_fetch = first_dataset['total'] / float(increment)
    if int(pages_to_fetch) < pages_to_fetch:
        pages_to_fetch += 1
    pages_to_fetch = int(pages_to_fetch)

    # Init jobs queue.
    jobs = [[x, increment, output, kpis] for x in range(0, pages_to_fetch)]

    # Run threads
    pool = Pool(threads)
    pool.map(fetch_job, jobs)

    pool.close()
    pool.join()


def process(input, output, threads=16):
    with open(output, 'w') as fileh:
        fileh.write('[\n')
        fileh.close()

    with open(input, 'r') as fileh:
        inputcsv = csv.DictReader(fileh, delimiter='|')

        jobs = [[row, output, kpis] for row in inputcsv]

        pool = Pool(threads)
        pool.map(process_job, jobs)
        pool.close()
        pool.join()


    with open(output, 'rb+') as fileh:
        fileh.seek(-2, os.SEEK_END)
        fileh.truncate()
        fileh.write(bytes('\n]'.encode('utf-8')))


if __name__ == '__main__':

    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Fetch and process CSV resources from data.gouv.fr')
    parser.add_argument('action', type=str, choices=['fetch', 'process'], help='Action to perform, one only.')
    parser.add_argument('--output', '-o', type=str, required=True, help='required')
    parser.add_argument('--input', '-i', type=str)
    parser.add_argument('--threads', '-t', type=int)
    parser.add_argument('--increment', '-I', type=int, help='action fetch: pages size for api calls')

    args = parser.parse_args()

    if args.action == 'process' and args.input is None:
        parser.print_help()
        print('{}: error: action \'process\' requires parameter \'--input\''.format(
            os.path.basename(sys.argv[0])
        ))
        exit(2)

    args = {key:getattr(args, key) for key in vars(args) if getattr(args, key) is not None}
    if args['action'] == 'fetch':
        args.pop('action')
        fetch(**args)
    else:
        args.pop('action')
        process(**args)
    exit()

