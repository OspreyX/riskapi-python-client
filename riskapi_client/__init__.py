"""
Fully featured python client library for RiskAPI

This library can be used to communicate programmatically with StatPro RiskAPI
from python, it can be embedded in any python 2.7 application and it does not
have external dependencies.


Usage

Instantiate a client object and call its method. Arguments are just python
objects, serialization and deserialization is performed transparently.
HTTP errors are wrapped in "HTTPError" exceptions and raised, with the
attribute "code" containing the HTTP status code and the exception
message containing the error returned by the server, if any.

Please refer to StatPro RiskAPI documentation for further informations.


Getting started

1. Create a file named .riskapi.conf in your home directory with your own
   credentials for the RiskAPI service, e.g.:

   [client]
   host=api.risk.statpro.com
   customer=internal
   user=<your user id>
   password=<your password>

2. Obtain a client object, the "connect" factory method is the easiest way:

   >>> conn = riskapi_client.connect()
   >>> conn
   <riskapi_client.RiskapiClient object at 0x7f14228f4bd0>

3. Get the list of available products, starting with "US" and limited to 2:

   >>> conn.products(search="US", limit=2)
   {u'count': 4963, u'data': [{u'reference_price': 56.48, u'code': u'US0003041052', u'product_type': 1, u'description': u'AAC Technologies Holdings Inc', u'last_update': u'2014-11-12', u'currency': u'USD', u'pricer': u''}, {u'reference_price': 115.337578118, u'code': u'US000324AA15', u'product_type': 7, u'description': u'AAF HOLDINGS 12.00% 01-Jul-2019 CALL', u'last_update': u'2014-11-11', u'currency': u'USD', u'pricer': u'callablecouponbond'}]}

4. Build a portfolio and add two holdings with the retrieved products plus a non-existing one:

   >>> pf = riskapi_client.Portfolio("EUR")
   >>> pf.add("US0003041052", quantity=13000)
   Holding('US0003041052', None, 13000, None, [], None, None)
   >>> pf.add("US000324AA15", quantity=10000)
   Holding('US000324AA15', None, 10000, None, [], None, None)
   >>> pf.add("XXX", quantity=13000)
   Holding('XXX', None, 13000, None, [], None, None)

5. Call some portfolio-level function:

   >>> conn.portfolio_info(pf)
   {u'errors': [[2, u'uncovered', u"Client code not found: u'XXX'", [u'XXX', None]]], u'results': {u'size': 2, u'exposure': 926549.3167053802, u'full_nominal_exposure': [[912219.776, u''], [14329.5407053803, u'']]}}

   >>> conn.risk(pf, [0.99])
   {u'errors': [[2, u'uncovered', u"Client code not found: u'XXX'", [u'XXX', None]]], u'results': [{u'potential_upside': 69280.4843514962, u'average_var': 60192.4303244384, u'expected_shortfall': 66361.2651884222, u'expected_upside': 78593.1966801616, u'lookback_days': 730, u'expected_return': 1075.6266813165, u'percentile': 0.99, u'frequency': 1, u'horizon': 1, u'average_potential_upside': 66453.7067345781, u'var': 60851.9174691098, u'expected_loss': 663.6126518842, u'diversification': 0.9999230482, u'volatility': 22179.0104368703}]}
"""

import os
import json
import urllib
import httplib
import warnings
import ConfigParser
import logging
import gzip
import time
import socket
from cStringIO import StringIO

msgpack = None
try:
    import msgpack
except ImportError:
    warnings.warn("msgpack module not installed - messagepack encoding disabled")


DEFAULT_HOST = "api.risk.statpro.com"
LOG = logging.getLogger('riskapi.client')

RISK_FUNCTIONS = [
    'average_potential_upside', 'average_var', 'diversification',
    'expected_loss', 'expected_return', 'expected_shortfall',
    'expected_upside', 'potential_upside', 'var', 'volatility',
]

DECOMPOSABLE_RISK_FUNCTIONS = [
    'expected_shortfall', 'expected_upside',
    'potential_upside', 'var', 'volatility',
]

class HTTPError(Exception):
    def __init__(self, code, msg=None):
        if msg is None:
            msg = httplib.responses[code]

        super(HTTPError, self).__init__(msg)

        self.code = code


class RiskapiClientError(Exception):
    pass


class HTTPClient(object):
    """a simple http client depending only on stdlib stuff"""

    block_size = 1024*8

    def __init__(self, scheme, host, port=None, auto_decode=True, retry=6):
        """
        initialize a new http client.
        """

        if scheme not in ('http', 'https'):
            raise RiskapiClientError("Invalid scheme '%s' (http or https expected)" % scheme)

        self.scheme = scheme
        self.host = host
        self.port = port

        self.auto_decode = auto_decode
        self.last_request = None

        self.retry = retry

        self.conn = self.connect()

    def close(self):
        self.conn.close()

    def connect(self):
        if self.scheme == 'http':
            cls = httplib.HTTPConnection
        else:
            cls = httplib.HTTPSConnection

        LOG.debug("Connectiong to %s:%s, %s", self.host, self.port, cls.__name__)

        conn = cls(self.host, self.port)
        conn.connect()
        return conn

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def reset(self):
        self.close()
        self.connect()

    def post(self, path, data, headers=None):
        return self._request(path, 'POST', data, headers)

    def get(self, path, params=None, headers=None):
        if params:
            url = "%s?%s" % (path, urllib.urlencode(params))
        else:
            url = path

        return self._request(url, 'GET', None, headers)

    def _decode(self, response):
        ct = response.getheader('Content-Type')
        ce = response.getheader('Content-Encoding')
        cl = response.getheader('Content-Length')

        if cl and not int(cl):
            LOG.debug("Empty response body")
            return ""

        if ce and ce == "gzip":
            # this reads the whole response in memory, but json.load() would do the same anyway...
            data = response.read()
            response = gzip.GzipFile(fileobj=StringIO(data))

        if ct and 'application/json' in ct:
            LOG.debug("decoding %s", ct)
            return json.load(response)
        elif ct and 'application/x-msgpack' in ct:
            if msgpack:
                LOG.debug("decoding %s", ct)
                return msgpack.unpack(response)
            else:
                LOG.debug("not decoding %s, decoder is unavailable", ct)
                return response.read()

        return response.read()

    def _request(self, url, method, body, headers):
        headers = headers or {}

        LOG.debug("Requesting %s %s, headers %s", method, url, headers)

        for retry in xrange(self.retry):
            try:
                self.conn.request(method, url, body, headers)

                response = self.conn.getresponse()

                LOG.debug("Response status %s, headers %s", response.status, response.getheaders())

                self.last_request = (method, url, response.status, response.getheaders())

                if self.auto_decode:
                    res_body = self._decode(response)
                else:
                    res_body = response.read()

                if response.status != httplib.OK:
                    raise HTTPError(response.status, res_body or None)

                return res_body
            except (socket.error, httplib.HTTPException) as e:
                LOG.debug("Error %s, retrying %s more times in %s seconds",
                          e, self.retry-retry, (2**retry)/10.0)
                time.sleep((2**retry)/10.0)
                self.reset()
                continue
        else:
            raise RiskapiClientError("%s %s failed after %s tries" % (method, url, self.retry))

    def fetch_paginated(self, url, page_size, extra_params, headers=None):
        # get the first page
        params = dict(start=0, limit=page_size)
        if extra_params:
            params.update(extra_params)
        data = self.get(url, params, headers)
        total_count = data['count']

        if total_count < page_size:
            return data['data']

        requests = (total_count / page_size) + 1

        results = data['data']
        for i in xrange(1, requests):
            params = dict(start=i * page_size, limit=page_size)
            if extra_params:
                params.update(extra_params)
            data = self.get(url, params, headers)
            results += data['data']
        return results


class Holding(object):
    def __init__(self, code, price=None, quantity=1, currency_exchange_value=None,
                 attributes=None, currency=None, price_factor=None):
        self.code = code
        self.price = price
        self.quantity = quantity
        self.currency_exchange_value = currency_exchange_value
        self.attributes = attributes or []
        self.currency = currency
        self.price_factor = price_factor

    def encode(self):
        """return the data structure expected by riskapi server, ready to be jsonized"""

        return [self.code, self.price, self.quantity, self.currency_exchange_value,
                self.attributes, self.currency, self.price_factor]

    def __repr__(self):
        return ("Holding(%(code)r, %(price)r, %(quantity)r, %(currency_exchange_value)r, "
                "%(attributes)r, %(currency)r, %(price_factor)r)") % vars(self)


class Portfolio(object):
    """RiskAPI portfolio"""

    Holding = Holding


    def __init__(self, currency, holdings=None, type_="quantities", outstanding=None, coverage_priority=None):
        self.holdings = holdings or []
        self.currency = currency
        self.type = type_
        self.outstanding = outstanding
        self.coverage_priority = coverage_priority

    def add(self, code, price=None, quantity=1, currency_exchange_value=None,
            attributes=None, currency=None, price_factor=None):
        """add an holding with the given properties to the portfolio"""

        holding = self.Holding(code, price, quantity, currency_exchange_value,
                               attributes, currency, price_factor)
        self.holdings.append(holding)
        return holding

    def encode(self):
        """return the data structure expected by riskapi server, ready to be jsonized"""

        holdings = [x.encode() for x in self.holdings]
        return [dict(currency=self.currency, type=self.type,
                     outstanding=self.outstanding,
                     coverage_priority=self.coverage_priority),
                holdings]

    def dump(self, file_name):
        """dump the portfolio to a json file"""

        with open(file_name, "wb") as ff:
            json.dump(self.encode(), ff)

    @classmethod
    def load(cls, file_name):
        """load a portfolio from a previously dumped json file"""

        with open(file_name, "rb") as ff:
            data = json.load(ff)

        holdings = [cls.Holding(*x) for x in data[1]]
        return cls(data[0]['currency'], holdings, data[0]['type'],
                   data[0]['outstanding'], data[0]['coverage_priority'])


class RiskapiClient(object):
    """
    HTTP client for StatPro web RiskAPI

    Refer to RiskAPI documentation for the meaning of the parameters and more details
    """

    API_BASE = "api"
    API_VERSION = "v1"

    FORMATS = dict(json="application/json")
    if msgpack is not None:
        FORMATS['msgpack'] = "application/x-msgpack"

    def __init__(self, host, customer=None, username=None, password=None, scheme="https",
                 keep_alive=True, request_format="json", response_format="json",
                 request_gzip=False, response_gzip=False):
        self.host = host
        self.customer = customer
        self.username = username
        self.password = password
        self.request_gzip = request_gzip
        self.response_gzip = response_gzip

        if ':' in host:
            name, port = host.split(':')
            port = int(port)
        else:
            name, port = host, None

        self.keep_alive = keep_alive

        if not request_format in self.FORMATS:
            raise RiskapiClientError("Invalid request format: should be one of %s" % self.FORMATS.keys())

        if not response_format in self.FORMATS:
            raise RiskapiClientError("Invalid response format: should be one of %s" % self.FORMATS.keys())

        self.request_format = request_format
        self.response_format = response_format

        self.webclient = HTTPClient(scheme, name, port)
        self._available_resources = self.webclient.get(self._url("system/resources"), headers=self._headers)

    def _url(self, resource):
        # generate the complete url for the given resource

        fragments = [self.API_BASE, self.API_VERSION, resource]
        if self.customer:
            fragments.insert(0, self.customer)

        return "/" + "/".join(fragments)

    @property
    def _headers(self):
        """return the http headers for a request"""

        headers = {}

        if self.keep_alive:
            headers['Connection'] = "Keep-Alive"
        else:
            headers['Connection'] = "Close"

        headers['Content-Type'] = self.FORMATS[self.request_format]
        headers['Accept'] = self.FORMATS[self.response_format]+",*/*"

        if self.username:
            auth = ("%s:%s" % (self.username, self.password)).encode('base64').strip()
            headers['Authorization'] = "Basic %s" % auth

        if self.request_gzip:
            headers['Content-Encoding'] = "gzip"

        if self.response_gzip:
            headers['Accept-Encoding'] = "gzip"

        return headers

    def _encode(self, data):
        if self.request_format == "json":
            data = json.dumps(data)
        elif self.request_format == "msgpack":
            data = msgpack.packb(data)
        else:
            raise RiskapiClientError("Invalid format: %s" % self.request_format)

        if self.request_gzip:
            writer = StringIO()
            gzip.GzipFile(fileobj=writer, mode="wb").write(data)
            data = writer.getvalue()

        return data

    def products(self, search=None, limit=None):
        """
        Available Products
        Return the list of the available products.
        Parameters:
            search
                a search term: if provided the search is limited to the
                products for which the code of the description starts with
                "search" (case insensitive)
            limit
                returns only up to "limit" results
        """

        params = {}
        if search is not None:
            params['query'] = search

        url = self._url("statics/products")

        if limit is not None:
            params['limit'] = limit

            return self.webclient.get(url, params, self._headers)['data']
        else:
            return self.webclient.fetch_paginated(self._url("statics/products"), 20000, params, self._headers)

    def product(self, code):
        """
        Product Details
        Return the product statics data and historical simulation scenarios
        """

        return self.webclient.get(self._url("statics/products/%s" % code), headers=self._headers)

    def available_stress_test_scenarios(self):
        """
        Available Stress Test Scenarios
        Return the list of the available stress test scenarios
        """

        return self.webclient.get(self._url("statics/stress-test"), headers=self._headers)

    def available_liquidity_risk_scenarios(self):
        """
        Available Liquidity Risk Scenarios
        Return the list of the available liquidity risk scenarios
        """

        return self.webclient.get(self._url("statics/liquidity-risk"), headers=self._headers)

    def portfolio_info(self, portfolio, fields=None):
        """
        Portfolio static infos
        Return a number of static informations about the given portfolio
        """

        data = self.webclient.post(
            self._url("statics/portfolio-info"),
            self._encode(dict(portfolio=portfolio.encode(), fields=fields)),
            self._headers)
        return data

    def data_info(self):
        """
        Dataset static infos
        Return a number of static informations about the latest loaded dataset
        """
        return self.webclient.get(self._url("statics/data-info"), headers=self._headers)

    def risk(self, portfolio, percentiles, functions=None,
             lookback_days=None, horizons=None, frequencies=None,
             exponential_decay=None):
        """
        Portfolio risk analysis
        Compute the given list of risk functions on the given portfolio with
        each combination of frequencies, horizons, percentiles and lookback_days.
        """

        if functions is None:
            functions = RISK_FUNCTIONS

        if lookback_days is None:
            lookback_days = [730]

        if horizons is None:
            horizons = [1]

        if frequencies is None:
            frequencies = [1]

        params = dict(lookback_days=lookback_days, percentiles=percentiles,
                      horizons=horizons, frequencies=frequencies,
                      portfolio=portfolio.encode(), functions=functions,
                      exponential_decay=exponential_decay)

        data = self.webclient.post(
            self._url("risk"), self._encode(params), self._headers)
        return data

    def stress_test(self, portfolio, codes=None):
        """
        Portfolio stress test analysis
        Return the cash loss or gain obtained by applying each requested
        stress test scenario on the given portfolio
        """

        data = self.webclient.post(
            self._url("stress-test"), self._encode(dict(portfolio=portfolio.encode(), stress_test_codes=codes)),
            self._headers)
        return data

    def liquidity_risk(self, portfolio):
        """
        Portfolio liquidity risk analysis
        Return the cash loss or gain obtained by appling each available
        liquidity risk scenario on the given portfolio
        """

        data = self.webclient.post(
            self._url("liquidity-risk"), self._encode(dict(portfolio=portfolio.encode())),
            self._headers)
        return data

    def risk_decomposition(self, portfolio, percentile, functions=None,
                           lookback_days=730, horizon=1, frequency=1, fields=None):
        """
        Portfolio risk decomposition
        Compute the risk decomposition of the given risk functions on the
        given portfolio, using the attributes lists from the portfolio holdings
        """

        if functions is None:
            functions = DECOMPOSABLE_RISK_FUNCTIONS

        params = dict(lookback_days=lookback_days, percentile=percentile,
                      horizon=horizon, frequency=frequency,
                      portfolio=portfolio.encode(), functions=functions, fields=fields)

        data = self.webclient.post(
            self._url("risk/decomposition"), self._encode(params), self._headers)
        return data

    def relative_risk_decomposition(self, portfolio, benchmark, percentile, functions=None,
                                    lookback_days=730, horizon=1, frequency=1, fields=None):
        """
        Portfolio relative risk decomposition
        Compute the risk decomposition of the given risk functions on the
        given portfolio, relative to the given benchmark
        """

        if functions is None:
            functions = DECOMPOSABLE_RISK_FUNCTIONS

        params = dict(lookback_days=lookback_days, percentile=percentile,
                      horizon=horizon, frequency=frequency,
                      portfolio=portfolio.encode(), benchmark=benchmark.encode(),
                      functions=functions, fields=fields)

        data = self.webclient.post(
            self._url("risk/decomposition/relative"), self._encode(params), self._headers)
        return data

    def multi_level_risk_decomposition(self, portfolio, percentile, functions=None,
                                       lookback_days=730, horizon=1, frequency=1, fields=None):
        """
        Portfolio multi-level risk decomposition
        Compute the multi-level risk decomposition of the given risk functions
        on the given portfolio, using the attributes lists from the portfolio
        holdings. It returns a hierarchy of risk figures according to the assets attributes
        """

        if functions is None:
            functions = DECOMPOSABLE_RISK_FUNCTIONS

        params = dict(lookback_days=lookback_days, percentile=percentile,
                      horizon=horizon, frequency=frequency,
                      portfolio=portfolio.encode(), functions=functions, fields=fields)

        data = self.webclient.post(
            self._url("risk/multi-level-decomposition"), self._encode(params), self._headers)
        return data

    def relative_multi_level_risk_decomposition(self, portfolio, benchmark, percentile, functions=None,
                                                lookback_days=730, horizon=1, frequency=1, fields=None):
        """
        Portfolio relative multi-level risk decomposition
        Compute the multi-level risk decomposition of the given risk functions
        on the given portfolio relative to the given benchmark using the attributes
        lists from the portfolio holdings. It returns a hierarchy of risk figures
        according to the assets attributes
        """

        if functions is None:
            functions = DECOMPOSABLE_RISK_FUNCTIONS

        params = dict(lookback_days=lookback_days, percentile=percentile,
                      horizon=horizon, frequency=frequency,
                      portfolio=portfolio.encode(), benchmark=benchmark.encode(),
                      functions=functions, fields=fields)

        data = self.webclient.post(
            self._url("risk/multi-level-decomposition/relative"), self._encode(params), self._headers)
        return data

    def stress_test_decomposition(self, portfolio, codes=None):
        """
        Portfolio stress test decomposition
        Measure the risk decomposition for the requested stress test scenarios
        on the given portfolio using the attributes lists from the portfolio holdings
        """

        data = self.webclient.post(
            self._url("stress-test/decomposition"),
            self._encode(dict(portfolio=portfolio.encode(), stress_test_codes=codes)),
            self._headers)
        return data

    def relative_stress_test_decomposition(self, portfolio, benchmark, codes=None):
        """
        Portfolio relative stress test decomposition
        Measure the risk decomposition for the requested stress test scenarios
        on the given portfolio relative to the given benchmark using the attributes
        lists from the portfolio holdings
        """

        data = self.webclient.post(
            self._url("stress-test/decomposition/relative"),
            self._encode(dict(portfolio=portfolio.encode(),
                              benchmark=benchmark.encode(),
                              stress_test_codes=codes)),
            self._headers)
        return data

    def multi_level_stress_test_decomposition(self, portfolio, codes=None):
        """
        Portfolio multi-level stress test decomposition
        Measure the multi-level risk decomposition for the requested
        stress test scenarios on the given portfolio using the
        attributes lists from the portfolio holdings
        """

        data = self.webclient.post(
            self._url("stress-test/multi-level-decomposition"),
            self._encode(dict(portfolio=portfolio.encode(), stress_test_codes=codes)),
            self._headers)
        return data

    def relative_multi_level_stress_test_decomposition(self, portfolio, benchmark, codes=None):
        """
        Portfolio relative multi-level stress test decomposition
        Measure the multi-level risk decomposition for the requested
        stress test scenarios on the given portfolio relative to the
        given benchmark using the attributes lists from the portfolio holdings
        """

        data = self.webclient.post(
            self._url("stress-test/multi-level-decomposition/relative"),
            self._encode(dict(portfolio=portfolio.encode(),
                              benchmark=benchmark.encode(),
                              stress_test_codes=codes)),
            self._headers)
        return data

    def liquidity_risk_decomposition(self, portfolio):
        """
        Portfolio liquidity risk decomposition
        Measure the risk decomposition for all the available liquidity
        scenarios on the given portfolio using the attributes lists
        from the portfolio holdings
        """

        data = self.webclient.post(
            self._url("liquidity-risk/decomposition"), self._encode(dict(portfolio=portfolio.encode())),
            self._headers)
        return data

    def multi_level_liquidity_risk_decomposition(self, portfolio):
        """
        Portfolio multi-level liquidity risk decomposition
        Measure the multi-level risk decomposition for all the available liquidity
        scenarios on the given portfolio using the attributes lists from the portfolio holdings
        """

        data = self.webclient.post(
            self._url("liquidity-risk/multi-level-decomposition"), self._encode(dict(portfolio=portfolio.encode())),
            self._headers)
        return data

    def aussie_bond_futures_NPV(self, code, price):
        """
        Aussie bond futures NPV
        Compute the NPV for an Aussie bond futures
        """

        data = self.webclient.post(
            self._url("aussie-bond-futures-npv"),
            self._encode(dict(code=code, price=price)),
            self._headers)
        return data

    def system_info(self):
        return self.webclient.get(self._url("system/dashboard"), headers=self._headers)



def connect(host=None, customer=None, username=None,
            password=None, secure=True, **kwargs):
    """
    connect to RiskAPI by taking configuration file into account
    configuration parameters are read from ~/.riskapi.conf, section [client]
    """

    cp = ConfigParser.RawConfigParser(allow_no_value=True)
    cp.read(os.path.expanduser("~/.riskapi.conf"))

    def get(name, default=None):
        if cp.has_option('client', name):
            return cp.get('client', name)
        else:
            return default

    if host is None:
        host = get('host', DEFAULT_HOST)

    if customer is None:
        customer = get('customer', 'internal')

    if username is None:
        username = get('user', None)

    if password is None:
        password = get('password', None)

    if secure:
        scheme = "https"
    else:
        scheme = "http"

    return RiskapiClient(host, customer, username, password, scheme, **kwargs)


def connect_local(host="localhost:8000", customer="", username="", password="", secure=False, **kwargs):
    return connect(host, customer, username, password, secure, **kwargs)
