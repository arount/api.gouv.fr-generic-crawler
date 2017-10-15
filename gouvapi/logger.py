#!/usr/env/bin python

import parse
import sys
from termcolor import colored


class StdinLogger(object):

    icons = {
        'up': ('▲', 'blue'),
        'down': ('▼', 'magenta'),
        'error': ('■', 'red'),
        'crash': ('❌', 'red'),
        'success': ('■', 'green'),
        'nocsv': ('■', 'yellow'),
        'duplicate': ('■', 'grey')
    }
    colors = {
        'jobid': 'cyan',
        'success': 'green',
        'error': 'red',
        'nocsv': 'yellow'
    }

    def __init__(self, pattern):
        self.pattern = pattern


    def log(self, action, pattern = None, **kwargs):
        '''
        Log action into screen.
            action    Action to log, determine what icon to use
            kwargs    All needed args to format self.pattern
        '''
        # Get icon
        _args = {
            'icon': colored(
                self.icons[action][0],
                self.icons[action][1],
                attrs=['bold']
            )
        }
        _args.update(kwargs)

        # If 'kpis' key found alias all attributes in args
        args = _args.copy()
        if 'kpis' in _args.keys():
            for key in dir(_args['kpis']):
                if key[0:2] != '__':
                    args[key] = getattr(args['kpis'], key)
        args.pop('kpis')

        # Colorize args
        _args = args.copy()
        for name in _args:
            if name in self.colors:
                args[name] = colored(_args[name], self.colors[name])

        # Format and print
        if pattern is None:
            pattern = self.pattern
        print(pattern.format(**args))


    def up(self, **kwargs):
        self.log('up', **kwargs)

    def down(self, **kwargs):
        self.log('down', **kwargs)

    def error(self, **kwargs):
        self.log('error', **kwargs)

    def success(self, **kwargs):
        self.log('success', **kwargs)

    def nocsv(self, **kwargs):
        self.log('nocsv', **kwargs)

    def duplicate(self, **kwargs):
        self.log('duplicate', **kwargs)

    def crash(self, **kwargs):
        self.log('crash', **kwargs)


class FetchLogger(StdinLogger):

    pattern = '{icon} ({jobid}:{success}/{error}/{nocsv}/{duplicate}) {id}'

    def __init__(self):
        pass


class ProcessLogger(StdinLogger):

    pattern = '{icon} ({jobid}:{success}/{error}) {id}'

    def __init__(self):
        pass

