#!/usr/env/bin python

'''
data.gouv.fr API client for Python
(This is not really a client API, but a quick implementation of /dataset endpoint)
'''

import requests
import urllib.parse


class API(object):
    '''
    Main API entry point.
    Expose easy access to handled resources - nothing more.
    '''

    baseurl = 'https://www.data.gouv.fr/api/1/'


    def datasets(self, output='csv', page_size=20, page=1):
        '''
        Quick access to '/datasets' API resources.

            output       'format' parameter excepted by API
            page_size    Size of result list
            page         Page to fetch
        '''
        return self.query('datasets', format=output, page_size=page_size, page=page)


    def query(self, resource, **kwargs):
        '''
        Generate Query instance to call API.

            resource    Resource endpoint to reach
                        Can be a path to resource (example 'foo/bar/baz")
            kwargs      GET arguments to pass
        '''
        url = urllib.parse.urljoin(self.baseurl, resource)
        query = Query(url, **kwargs)
        return query


class Query(object):
    '''
    Query aims to send queries to API, handle errors and expose user-friendly
    methods to navigate through API's endpoints.

    Not that Query class handles JSON API **only**, resources (like CSV files)
    are not part of API and would be not handled here.
    '''


    def __init__(self, url, **params):
        '''
        Init Query's instance.

            url       URL to fetch
            params    HTTP parameters to pass
        '''

        self.response = None
        self.status_code = None
        self.data = None

        self.url = url
        self.params = params


    @classmethod
    def from_uri(cls, uri):
        '''
        Kind of factory taking URI (uri) to parse it in url / parameters.
        Returns Query instance.

            uri    URI to parse into Query instance
        '''
        parts = urllib.parse.urlparse(uri)
        url = '{}://{}{}'.format(
            parts.scheme,
            parts.netloc,
            parts.path
        )
        params = dict(urllib.parse.parse_qsl(parts.query))
        return cls(url, **params)


    @property
    def uri(self):
        '''
        Compute final URI (URL + params) and return it as string.
        '''
        if len(self.params) > 0:
            params = urllib.parse.urlencode(self.params)
            return '?'.join([self.url, params])
        else:
            return self.url


    def get(self):
        '''
        Alias to self.send() + self.parse()
        '''
        self.send()
        return self.parse()


    def send(self, method='GET'):
        '''
        Send query.
        Will raise QueryException if http status code isn't 200

            method    HTTP method to use
        '''
        self.response = getattr(requests, method.lower())(self.uri)
        # Alias to `status_code` from raw response to Query instance
        self.status_code = self.response.status_code

        if self.status_code != 200:
            raise QueryException('Query {} returned HTTP status code: {}'.format(
                self, self.status_code
            ))

        return self.response


    def parse(self):
        '''
        Parse given self.response.
        Will raise ValueError if self.response is None
        '''
        if self.response is None:
            raise ValueError('Query.parse() can not parse None response.')

        self.data = self.response.json()

        return self.data


    def nextpage(self):
        '''
        Create Query instance pointing to next API page.
        '''
        try:
            next_uri = self.data['next_page']
        except KeyError:
            raise QueryNavigationException(
                'Query {} does not provide next_page endpoint.'
            )
        return self.__class__.from_uri(next_uri)


    def __repr__(self):
        return '<Query {} [{}]>'.format(self.uri, self.status_code)


class QueryException(Exception):
    '''
    Default Query exception class.
    When something wrong happend while fetching API.
    '''
    pass


class QueryNavigationException(Exception):
    '''
    This exception aims to be raised only for navigation issue.
    It does not means something wrong happend while fetching results,
    but navigation is not possible anymore.

    Example: You are on the last page of a resource and try to navigate to next page.
    '''
    pass

