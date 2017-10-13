#!/usr/env/bin python

'''
Exposes Datasets class, an iterator allowing to fetch all datasets uris.
It's only a more convenient way to fetch datasest from data.gouv.fr API,
it abstract all API navigation behavior to only gives data.
'''

from gouvapi.api import API


class Datasets(object):
    '''
    Simple iterator fetching datasets uris and metadata.
    '''

    def __init__(self, increment = 20, start = 1):
        '''
        Init Datasets iterator

            increment    Size of elements fetched by page.
            start        Starting position
        '''

        self.page = start - 1
        self.position = 0

        self._datasets = {'data': []}
        self._increment = increment
        self._api = API()


    def __iter__(self):
        return self


    def __next__(self):
        '''
        Iterator method, call API at first call and when current page's items are all
        fetched.

        Would stop when API does not returns any data (more sure and pythonic than testing
        current position vs datasets size).
        '''

        # Try to get next position item
        # If IndexError raised, a call to API is required.
        try:
            item = self._datasets['data'][self.position]
        except IndexError:
            # Update positions to fetch API
            self.page += 1
            self.position = 0

            # Get data from API
            self._datasets = self._api.datasets(
                page_size=self._increment,
                page=self.page
            ).get()

            # IndexError raised here means all possible data are
            # fetched.
            try:
                item = self._datasets['data'][self.position]
            except IndexError:
                raise StopIteration()

        self.position += 1
        return item


