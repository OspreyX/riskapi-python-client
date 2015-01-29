riskapi-python-client
=====================

Fully featured python client library for StatPro RiskAPI

Description
-----------

This library can be used to communicate programmatically with StatPro RiskAPI
from python, it can be embedded in any python 2.7 application and it does not
have mandatory external dependencies.


Installation
------------

`$ pip install git+https://github.com/StatProSA/riskapi-python-client.git`

If you want to communicate in ``msgpack`` you will also need the optional package msgpack-python::

`$ pip install msgpack-python>=0.4`


Usage
-----

Instantiate a client object and call its method. Arguments are just python
objects, serialization and deserialization is performed transparently.
HTTP errors are wrapped in ``"HTTPError"`` exceptions and raised, with the
attribute "code" containing the HTTP status code and the exception
message containing the error returned by the server, if any.

Please refer to StatPro RiskAPI documentation for further informations.


Getting started
---------------


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
