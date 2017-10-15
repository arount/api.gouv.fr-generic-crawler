#!/usr/env/bin python

'''
Exposes all components and some quick access to
main actions (fetch and process).
'''

from gouvapi.api import *
from gouvapi.resource import *
from gouvapi.logger import FetchLogger, ProcessLogger
import gouvapi._kpis as kpis

from termcolor import colored
import json
import time
import requests


def process_resource(resource):
    '''
    Open and process resource.
    '''
    with resource():
        data = resource.json()
    return data


def process_job(job):
    '''
    Processing resource job, designed to be used within multithreading,
    but can be used alone as well.
        job    Job description as dict, except indexes/values:
                   0       Row, a list from input csv file.
                   1       Output file path.
    '''
    logger = ProcessLogger()
    # Parse job into variables
    row = job[0]
    output_path = job[1]

    # Get jobid (jobid) and increment global jobids jobid.
    jobid = kpis.counter
    kpis.counter += 1

    logger.up(jobid=jobid, kpis=kpis, id=row['id'])

    # Try to fetch resource
    resource = CSVResource(**row)
    try:
        json_data = json.dumps(process_resource(resource))
    except (ResourceException, CSVResourceException) as e:
        kpis.error += 1
        logger.error(jobid=jobid, kpis=kpis, id=row['id'])
    else:
        kpis.success += 1
        logger.success(jobid=jobid, kpis=kpis, id=row['id'])

        # Write job's output in file
        with open(output_path, 'a+') as fileh:
            fileh.write('{},\n'.format(json_data))
            fileh.close()

    logger.down(jobid=jobid, kpis=kpis, id=row['id'])
    return


def fetch_job(job):
    '''
    API resources fetching job.
    Call API page and filter / extract resources.
    '''
    # Parse job to variables
    page = job[0]
    increment = job[1]
    outputfile = job[2]

    logger = FetchLogger()

    # Set jobid
    jobid = kpis.counter
    kpis.counter += 1

    logger.up(jobid=jobid, kpis=kpis, id='{}-{}'.format(page, increment))

    # Init API instance and get api's page to fetch
    api = API()
    try:
        datasets = api.datasets(page_size=increment, page=page).get()['data']
    # Sometimes network error can happen, let sleep job for 0.2 seconds and try again
    except (requests.ConnectionError, QueryException, json.decoder.JSONDecodeError):
        time.sleep(0.2)
        try:
            datasets = api.datasets(page_size=increment, page=page).get()['data']
        # If even after retry it failed again log job in error.
        except (requests.ConnectionError, QueryException, json.decoder.JSONDecodeError):
            logger.crash(jobid=jobid, kpis=kpis, id='{}-{}'.format(page, increment))
            return

    fetch(datasets, outputfile, logger, jobid=jobid)

    logger.down(jobid=jobid, kpis=kpis, id='{}-{}'.format(page, increment))


def fetch(datasets, outputfile, logger, jobid=None):
    '''
    Fetch resources within a datasets.
    Filter resources to keep only csv, handle most network/parsing behaviors.
    '''
    # Loop in fetched datasets
    for item in datasets:
        kpis.fetched += 1

        # Some datasets expose several time the same resource
        # this list will be used to avoid to write same line twice or more.
        fetched_resources = []
        for resource in item['resources']:
            kpis.resources += 1

            # Even when asking for csv all mimetypes does not fit.
            # Some of none text/csv mime are surelly well formated csv files, but because this
            # app is a test we will skip them for now.
            if resource['mime'] != 'text/csv':
                kpis.nocsv += 1
                kpis.error += 1
                logger.nocsv(jobid=jobid, kpis=kpis, id=resource['id'])
                continue

            # Same url was already fetched once.
            if resource['url'] in fetched_resources:
                kpis.duplicate += 1
                kpis.error += 1
                logger.duplicate(jobid=jobid, kpis=kpis, id=resource['id'])
                continue

            fetched_resources.append(resource['url'])

            # Write data in file
            with open(outputfile, 'a+') as fh:
                fh.write('{}|{}|{}\n'.format(
                    item['id'],
                    resource['id'],
                    resource['url']
                ))
                fh.close()
                kpis.success += 1

                logger.success(jobid=jobid, kpis=kpis, id=resource['id'])


