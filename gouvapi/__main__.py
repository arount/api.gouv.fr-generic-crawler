from gouvapi.datasets import Datasets
from gouvapi.api import Query

if __name__ == '__main__':

    print('Starting to fetch datasets')
    for item in Datasets():
        print('  fetching {}'.format(item['uri']))
        query = Query.from_uri(item['uri'])

        for resource in query.get()['resources']:
            print('    fetching resource {}'.format(resource['url']))
