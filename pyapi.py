import requests
import os
import time
import datetime
import json
import logging

''' A simple and thin python library for APIs based on pyen
'''
logger = logging.getLogger('pyapi')

class PyapiConfigurationException(Exception): pass
class PyapiException(Exception):
    def __init__(self, http_status, code, msg):
        self.http_status = http_status
        self.code = code
        self.msg = msg
    def __str__(self):
        return u'http status: {0}, code:{1} - {2}'.format(self.http_status, self.code, self.msg)

class Pyapi(object):

    def __init__(self, site, api_key = None):
        """ Creates a new Pyapi

        Args:
            site:  str, the site name, currently the sites are:
                    'echonest'
                    'bandsintown'
            api_key: the site API key. If not set, look
                     for one in the site_API_KEY environment variable

        """
        
        sites = {'echonest': {'api_key': 'ECHONEST_API_KEY', 
                              'prefix': 'http://developer.echonest.com/api/v4/'},
                 'bandsintown': {'api_key': 'BANDSINTOWN_API_KEY', 
                              'prefix': 'http://api.bandsintown.com/'}}
        self.site = site
        if self.site not in sites:
            raise PyapiConfigurationException("Please call Pyapi('site'). "
                "Current valid sites are: {0}".format(sites.keys()))

        if not api_key:
            api_key = os.environ.get(sites[self.site]['api_key'])

        self.next_command_time = 0

        # These are things we can config
        self.api_key = api_key
        self.auto_throttle = True
        self.prefix = sites[site]['prefix']
        self.max_retries = 5

        if not self.api_key:
            raise PyapiConfigurationException("Can't find your API key anywhere")


    def _internal_call(self, verb, method, params):

        clean_params = dict(api_key = self.api_key)
        files = {}

        for k,v in params.items():
            if isinstance(v, bool):
                v = 'true' if v else 'false'
            elif hasattr(v, 'read') and hasattr(v, 'close'):
                files[k] = v
                continue
            elif ((method == 'catalog/update'
                    or method == 'tasteprofile/update') and k == 'data'
                    and not isinstance(v, str)):
                v = json.dumps(v)
            clean_params[k] = v

        if verb == 'GET':
            args = dict(params=clean_params)
        else:
            args = dict(data=clean_params)
            if files:
                args['files'] = files

        url = self.prefix + method

        max_tries = self.max_retries
        while max_tries > 0:
            self._apply_throttle()

            r = requests.request(verb, url, **args)
            print 'pyapi request url', r.url
            self._measure_throttle_time(r)

            if r.status_code == 429 and self.auto_throttle:
                max_tries -= 1
                logger.debug(u'RATE LIMITED, retrying ...:')
            else:
                break

        logger.debug(u'URL: {0}'.format(r.url))
        if r.status_code >= 400 and r.status_code < 500:
            logger.error(u'ERROR {0} {1}'.format(r.status_code, r.url))

        # logger.debug('HEADERS {0}'.format(repr(r.headers)))
        # logger.debug(u'RESP: {0}'.format(r.text))

        # print 'status code', r.status_code, r.text
        # we don't get valid json back on a 404, so, deal with 404 explicitly
        if r.status_code != 404:
            results =  r.json()
            if self.site == 'echonest':
                results = results['response']
                if results['status']['code'] != 0:
                    raise PyapiException(r.status_code, results['status']['code'],
                                         results['status']['message'])
        else:
            raise PyapiException(r.status_code, -1, u'the requested resource could not be found')

        r.raise_for_status()
        return results

    def _measure_throttle_time(self, r):
        if self.auto_throttle:
            if 'x-ratelimit-remaining' in r.headers:
                remaining = int(r.headers['x-ratelimit-remaining'])
                if remaining == 0:
                    date_string = r.headers['date']
                    date = self._parse_date(date_string)
                    delay_time = 60 - date.second
                    if delay_time > 0:
                        self.next_command_time = time.time() + delay_time

    def _apply_throttle(self):
        if self.auto_throttle:
            now = time.time()
            if self.next_command_time  > now:
                delay_time = self.next_command_time - now
                logger.debug(u'RATE LIMITED, delaying for {0}'.format(delay_time))
                time.sleep(delay_time)

    def _parse_date(self, date_string):
        # Mon, 21 Oct 2013 18:06:50 GMT
        # TODO:
        # need to be careful about locale here, the locale
        # will be Echo Nest locale, not local locale. So
        # this code will break in france
        #
        fmt = "%a, %d %b %Y %H:%M:%S %Z"
        date = datetime.datetime.strptime(date_string, fmt)
        return date

    def post(self, method, args = None, **kwargs):
        """ Calls the API via a POST

        Args:
            method: The API method name
            args: The API method arguments
        """
        if args:
            kwargs.update(args)
        return self._internal_call('POST', method, kwargs)

    def get(self, method, args = None, **kwargs):
        """ Calls the API via a GET

        Args:
            method: The API method name
            args: The method arguments
        """
        if args:
            kwargs.update(args)
        return self._internal_call('GET', method, kwargs)

