#!/usr/env/bin python

import urllib.request
import chardet
import csv
import io

class CSVResource(object):
    '''
    Handle all CSV resources behavior.
    Download in memory content, reads it and parse headers.
    Can handle heterogeneous encoding, delimiters and newlines
    '''

    # csv.Sniffer() should be able to find itself delimiter to use, but failed a lot.
    # This heuristic delimiters list will help to find the right delimiter to use.
    _heuristic_delimiters = [',', ';', '|']
    # Keys to exposes in self.json()
    _json_keys = ['headers', 'charset', 'url', 'firstline', 'delimiter', 'quotechar']


    def __init__(self, url, **meta):
        '''
        Initialize resource.
            url     Resource's URL
            meta    Meta data, could be anything
        '''
        # Set all keys used in self.json() to None. It avoid crashes if trying to serialize
        # not opened instance.
        attrs = {key: None for key in self._json_keys}
        # All meta are directly setted as instance's attribute too.
        attrs.update(meta)
        for key in attrs:
            setattr(self, key, attrs[key])

        # Save meta keys for more conveniant usage of self.json()
        self._meta = list(meta.keys())
        self.url = url


    def __enter__(self):
        '''
        Open URL and parse CSV.
        '''
        self.open()
        self.parse()
        return self


    def __call__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


    def open(self):
        '''
        Open resource's URL.
        Raise ResourceException if not accessible.
        Handle newlines and charset.
        '''
        # Try to open url
        try:
            self.response = urllib.request.urlopen(self.url)
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            raise ResourceException(str(e))

        # Because this class aims to expose only csv headers and not values
        # it would be waste of time to read all document.
        # Only first line is read to win memory / cpu and time.
        try:
            # Combine readline() and splitline() to handle universal newlines
            firstline = self.response.readline().splitlines()[0]
        except IndexError as e:
            raise CSVResourceException(str(e))

        # Find charset of document.
        self.charset = chardet.detect(firstline)['encoding']
        # Sometimes charset is not found, it mainly happen when document is not text/csv
        # but gz file.
        if self.charset is None:
            raise CSVResourceException('Can not find charset for {}'.format(self.url))
        try:
            self.firstline = firstline.decode(self.charset)
        except UnicodeDecodeError as e:
            raise CSVResourceException(str(e))


    def parse(self):
        '''
        Parse CSV heards and dialect from csv.Sniffer()

        Headers are heuristically found to provide better results than csv.Sniffer()
        especially on heterogeneous resources.
        '''
        # Sniff quotechar and delimiter
        self.dialect = csv.Sniffer().sniff(self.firstline)
        self.dialect.delimiter = self._find_delimiter()

        # Alias for more conveniant use of self.json()
        self.delimiter = self.dialect.delimiter
        self.quotechar = self.dialect.quotechar

        # Finally reads first line as csv
        try:
            _headers = csv.reader(
                io.StringIO(self.firstline),
                dialect=self.dialect
            )
        except TypeError as e:
            raise CSVResourceException(str(e))

        # Parse headers into list
        try:
            _headers = list(_headers)[0]
        except csv.Error as e:
            raise CSVResourceException(str(e))

        # Be sure to clean quotechars (sometimes ones left)
        headers = []
        for header in _headers:
            headers.append(header.replace(self.dialect.quotechar, ''))

        self.headers = headers
        return self.headers


    def _find_delimiter(self):
        '''
        Find 'best' delimiter.
        It basically find the delimiter with more splits in first line
        within self._heuristic_delimiters + csv.Sniffer()'s one
        '''
        delimiters = self._heuristic_delimiters
        if self.dialect.delimiter not in delimiters:
            delimiters.append(self.dialect.delimiter)

        # Try to find best delimiter of heuristics and sniffed ones
        heuristics = {}
        for delimiter in delimiters:
            heuristics[len(self.firstline.split(delimiter))] = delimiter

        return heuristics[max(heuristics.keys())]


    def json(self):
        '''
        Serialize instance into dict.
        '''
        output = {}
        for key in self._json_keys + self._meta:
            output[key] = getattr(self, key)
        return output



class ResourceException(Exception):
    pass

class CSVResourceException(ResourceException):
    pass
