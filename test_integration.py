import random

import nose.tools as nt
from voluptuous import Schema, Match, Optional, All, Range, Any, Invalid

import riskapi_client

SIZE = 100
BM_SIZE = 200
ERRORS_THRESHOLD = 0.2

CURRENCIES = ["EUR", "USD", "JPY", "GBP", "AUD"]
ATTRIBUTES = ["X", "Y", "Z"]

PORTFOLIO = None
BENCHMARK = None
STRESS_TEST_CODES = None
LIQ_RISK_CODES = None


def setup():
    global PORTFOLIO, BENCHMARK, STRESS_TEST_CODES, LIQ_RISK_CODES

    client = riskapi_client.connect()
    try:
        products = client.products(limit=SIZE*100)

        PORTFOLIO = riskapi_client.Portfolio(
            random.choice(CURRENCIES),
            [riskapi_client.Holding(x['code'], None, 100.0/SIZE,
                                    None, random.sample(ATTRIBUTES, 3))
             for x in random.sample(products, SIZE)], "weights", 1000**2)

        BENCHMARK = riskapi_client.Portfolio(
            PORTFOLIO.currency,
            [riskapi_client.Holding(x['code'], None, 100.0/BM_SIZE,
                                    None, random.sample(ATTRIBUTES, 3))
             for x in random.sample(products, BM_SIZE)], "weights", 1000**2)

        STRESS_TEST_CODES = sorted([
            x['code'] for x in client.available_stress_test_scenarios()['data']])

        LIQ_RISK_CODES = sorted([
            x['code'] for x in client.available_liquidity_risk_scenarios()['data']])
    finally:
        client.webclient.close()


def Couple(t1, t2):
    """
    Validate an iterable of exactly two elements of the given types
    """

    def _check(v):
        try:
            if len(v) != 2:
                raise Invalid("Not a couple")
            v1 = v[0]
            v2 = v[1]
        except TypeError:
            raise Invalid("Input value is not iterable")

        if isinstance(t1, Schema):
            t1(v1)
        elif not isinstance(v1, t1):
            raise Invalid("first value: expected %s, found %s" % (
                t1.__name__, type(v1).__name__))

        if isinstance(t2, Schema):
            t2(v2)
        elif not isinstance(v2, t2):
            raise Invalid("second value: expected %s, found %s" % (
                t2.__name__, type(v2).__name__))

        return v

    return _check


IsoDateSchema = Schema(Match("^\d{4}-\d{2}-\d{2}$"))
TimestampSchema = Schema(Match("^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}.\d+\+.*$"))
PositiveInteger = All(int, Range(min=1))
NullableFloat = Any(float, None)

PortfolioInfoSchema = Schema({
    Optional('exposure'): float,
    Optional('size'): int,
    Optional('full_nominal_exposure'): [Couple(float, unicode)],
    Optional('covered_exposure'): float,
    Optional('portfolio_cash_scenarios'): [Couple(IsoDateSchema, float)],
    Optional('portfolio_quantities'): [float],
    Optional('asset_scenarios'): {unicode: [Couple(IsoDateSchema, float)]}
})

DataInfoSchema = Schema({
    'max_date_iso': IsoDateSchema,
    'max_date': PositiveInteger,
    'n_products': PositiveInteger,
    'scenarios_size': PositiveInteger,
    'stresstest_size': PositiveInteger,
    'timestamp': TimestampSchema,
    'liquidityrisk_size': PositiveInteger,
})

RiskSchema = Schema([{
    'frequency': All(int, Range(min=1, max=20)),
    'horizon': All(int, Range(min=1, max=20)),
    'lookback_days': All(int, Range(min=100)),
    'percentile': All(float, Range(min=0.5, max=0.999)),
    Optional('average_potential_upside'): NullableFloat,
    Optional('average_var'): NullableFloat,
    Optional('diversification'): NullableFloat,
    Optional('expected_loss'): NullableFloat,
    Optional('expected_return'): NullableFloat,
    Optional('expected_shortfall'): NullableFloat,
    Optional('expected_upside'): NullableFloat,
    Optional('potential_upside'): NullableFloat,
    Optional('var'): NullableFloat,
    Optional('volatility'): NullableFloat,
}])

StressTestSchema = Schema([Couple(unicode, float)])

LiquidityRiskSchema = Schema([
    Couple(unicode, Schema({
        'code': float,
        'bidask': float,
        'market_cap': float,
        'nominal': float,
        'pricer': float,
        'pct_owned': float,
        'global': float,
    }))
])

RiskDecompositionComponentSchema = Schema([
    Schema({
        Optional('contribution_risk'): float,
        Optional('contribution_pct'): float,
        Optional('marginal_risk'): float,
        Optional('marginal_pct'): float,
        'attributes': Schema([unicode])
    })
])

RiskDecompositionSchema = Schema({
    Optional('exposure'): [Schema({'exposure': float, 'attributes': Schema([unicode])})],
    Optional('var'): RiskDecompositionComponentSchema,
    Optional('potential_upside'): RiskDecompositionComponentSchema,
    Optional('expected_shortfall'): RiskDecompositionComponentSchema,
    Optional('expected_upside'): RiskDecompositionComponentSchema,
    Optional('volatility'): RiskDecompositionComponentSchema,
})



class TestRisk(object):
    def setUp(self):
        self.client = riskapi_client.connect()

        di = self.client.data_info()

        # collect attributes
        self.attributes = set()
        for holding in PORTFOLIO.holdings:
            self.attributes.add(tuple(holding.attributes))

    def tearDown(self):
        self.client.webclient.close()

    def check_errors(self, results):
        err_pct = len(results['errors']) / SIZE

        if err_pct > ERRORS_THRESHOLD:
            raise AssertionError("Too many errors: %s (%s%%, > threshold of %s%%)" % (
                len(results['errors']), err_pct*100, ERRORS_THRESHOLD*100))

    def check_fields(self, item, expected, unexpected=None):
        expected = set(expected)

        keys = set(item)

        missing = expected - keys

        if unexpected is not None:
            extra = set(unexpected).intersection(keys)
        else:
            extra = keys - expected

        msg = []

        if missing:
            msg.append("Missing expected keys: %r" % ", ".join(missing))

        if extra:
            msg.append("Found unexpected keys: %r" % ", ".join(extra))

        if msg:
            raise AssertionError(", ".join(msg))

    def test_portfolio_info(self):
        res = self.client.portfolio_info(PORTFOLIO)
        self.check_errors(res)
        PortfolioInfoSchema(res['results'])
        self.check_fields(res['results'],
                          ['size', 'exposure', 'full_nominal_exposure'])

    def test_portfolio_info_all_fields(self):
        fields = [
            'full_nominal_exposure',
            'exposure',
            'covered_exposure',
            'size',
            'portfolio_cash_scenarios',
            'portfolio_quantities',
            'asset_scenarios',
        ]
        res = self.client.portfolio_info(PORTFOLIO, fields)
        self.check_errors(res)
        PortfolioInfoSchema(res['results'])
        self.check_fields(res['results'], fields, [])

    def test_portfolio_info_no_fields(self):
        res = self.client.portfolio_info(PORTFOLIO, [])
        self.check_errors(res)
        PortfolioInfoSchema(res['results'])
        self.check_fields(res['results'], [])

    def test_data_info(self):
        res = self.client.data_info()

        DataInfoSchema(res)

        nt.assert_greater(res['n_products'], 120000)
        nt.assert_greater(res['scenarios_size'], 500)
        nt.assert_greater(res['liquidityrisk_size'], 1)

    def test_risk(self):
        res = self.client.risk(PORTFOLIO, [0.99])
        self.check_errors(res)

        RiskSchema(res['results'])

        nt.assert_equal(len(res['results']), 1)

        rr = res['results'][0]

        # basic fields are already checked by the schema, just check optional fields
        expected_fields = riskapi_client.RISK_FUNCTIONS
        self.check_fields(rr.keys(), expected_fields, [])

        nt.assert_almost_equal(rr['frequency'], 1)
        nt.assert_almost_equal(rr['horizon'], 1)
        nt.assert_almost_equal(rr['lookback_days'], 730)
        nt.assert_almost_equal(rr['percentile'], 0.99)


    def test_risk_all_parameters(self):
        pcts = [0.95, 0.99]
        lbds = [730, 365]
        hors = [1, 5]
        functions = ['var', 'volatility']

        res = self.client.risk(
            PORTFOLIO, pcts, functions,
            lbds, hors, hors, 0.15)

        self.check_errors(res)

        RiskSchema(res['results'])

        perms = set()
        for pp in pcts:
            for ll in lbds:
                for hh in hors:
                    for ff in hors:
                        perms.add((pp, ll, hh, ff))

        nt.assert_equal(len(res['results']), len(perms))

        for rr in res['results']:
            item = (rr['percentile'], rr['lookback_days'], rr['horizon'], rr['frequency'])

            if item not in perms:
                raise AssertionError("Parameters not found: %s" % str(item))

            # check that we have results only for the requested functions
            self.check_fields(rr.keys(), functions, set(riskapi_client.RISK_FUNCTIONS)-set(functions))

    def test_stress_test(self):
        res = self.client.stress_test(PORTFOLIO)

        self.check_errors(res)

        StressTestSchema(res['results'])

        nt.assert_items_equal([x[0] for x in res['results']], STRESS_TEST_CODES)

    def test_stress_test_with_codes(self):
        res = self.client.stress_test(PORTFOLIO, STRESS_TEST_CODES[:10])

        self.check_errors(res)

        StressTestSchema(res['results'])

        nt.assert_items_equal([x[0] for x in res['results']], STRESS_TEST_CODES[:10])

    def test_liquidity_risk(self):
        res = self.client.liquidity_risk(PORTFOLIO)

        self.check_errors(res)

        LiquidityRiskSchema(res['results'])

        nt.assert_items_equal([x[0] for x in res['results']], LIQ_RISK_CODES)

    def check_risk_decomposition(self, results, functions, fields):
        self.check_errors(results)

        RiskDecompositionSchema(results['results'])

        nt.assert_items_equal(
            results['results'].keys(),
            functions)

        fields += ("attributes",)

        for name, item in results['results'].iteritems():
            all_attributes = {tuple(x['attributes']) for x in item}
            nt.assert_items_equal(all_attributes, self.attributes)

            for row in item:
                if name == "exposure":
                    self.check_fields(row, ("exposure", "attributes"))
                else:
                    self.check_fields(row, fields)


    def test_risk_decomposition(self):
        res = self.client.risk_decomposition(PORTFOLIO, 0.99)

        self.check_risk_decomposition(
            res, riskapi_client.DECOMPOSABLE_RISK_FUNCTIONS,
            ("marginal_risk", "marginal_pct",
             "contribution_risk", "contribution_pct",))


    def test_risk_decomposition_with_parameters(self):
        res = self.client.risk_decomposition(
            PORTFOLIO,
            0.95, ['var', 'volatility', 'exposure'],
            700, 5, 5, ['marginal_risk'])

        self.check_risk_decomposition(
            res,
            ("var", "volatility", "exposure"),
            ("marginal_risk",))

    def test_relative_risk_decomposition(self):
        res = self.client.relative_risk_decomposition(PORTFOLIO, BENCHMARK, 0.99)

        self.check_risk_decomposition(
            res, riskapi_client.DECOMPOSABLE_RISK_FUNCTIONS,
            ("marginal_risk", "marginal_pct",
             "contribution_risk", "contribution_pct",))

    def test_relative_risk_decomposition_with_parameters(self):
        res = self.client.relative_risk_decomposition(PORTFOLIO, BENCHMARK,
            0.95, ['var', 'volatility'],
            700, 5, 5, ['marginal_risk'])

        self.check_risk_decomposition(
            res,
            ("var", "volatility"),
            ("marginal_risk",))

    def test_relative_risk_decomposition_with_exposure(self):
        with nt.assert_raises_regexp(riskapi_client.HTTPError, "unknown.*function.*exposure") as exc:
            self.client.relative_risk_decomposition(PORTFOLIO, BENCHMARK,
                0.95, ['var', 'volatility', 'exposure'],
                700, 5, 5, ['marginal_risk'])
        nt.assert_equal(exc.exception.code, 422)

