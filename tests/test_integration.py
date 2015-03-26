import random

import nose.tools as nt
from voluptuous import (
    Schema, Optional, All, Range, Any, Match, Datetime, ExactSequence)

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
AUSSIE_BOND_FUTURES_CODE = None


def setup():
    global PORTFOLIO, BENCHMARK, STRESS_TEST_CODES, LIQ_RISK_CODES, AUSSIE_BOND_FUTURES_CODE

    client = riskapi_client.connect()
    try:
        products = client.products()

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

        for prod in products:
            if 'aussie' in prod['pricer']:
                AUSSIE_BOND_FUTURES_CODE = prod['code']
                break
    finally:
        client.webclient.close()


IsoDateSchema = Datetime("%Y-%m-%d")
# %z is not always supported, so we can't use Datetime.
TimestampSchema = Schema(Match("^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(.\d+)?"))
PositiveInteger = All(int, Range(min=1))
NullableFloat = Any(float, None)


PortfolioInfoSchema = Schema({
    Optional('exposure'): float,
    Optional('size'): int,
    Optional('full_nominal_exposure'): [ExactSequence([float, unicode])],
    Optional('covered_exposure'): float,
    Optional('portfolio_cash_scenarios'): [ExactSequence([IsoDateSchema, float])],
    Optional('portfolio_quantities'): [float],
    Optional('asset_scenarios'): {unicode: [ExactSequence([IsoDateSchema, float])]}
}, required=True)


DataInfoSchema = Schema({
    'max_date_iso': IsoDateSchema,
    'max_date': PositiveInteger,
    'n_products': PositiveInteger,
    'scenarios_size': PositiveInteger,
    'stresstest_size': PositiveInteger,
    'timestamp': TimestampSchema,
    'liquidityrisk_size': PositiveInteger,
}, required=True)


RiskSchema = Schema([
    Schema({
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
    }, required=True)])


StressTestSchema = Schema([ExactSequence([unicode, float])])


LiquidityRiskComponentSchema = Schema({
        'code': float,
        'bidask': float,
        'market_cap': float,
        'nominal': float,
        'pricer': float,
        'pct_owned': float,
        'global': float,
    }, required=True)


LiquidityRiskSchema = Schema([
    ExactSequence([unicode, LiquidityRiskComponentSchema])
])


RiskDecompositionComponentSchema = Schema([
    Schema({
        Optional('contribution_risk'): NullableFloat,
        Optional('contribution_pct'): NullableFloat,
        Optional('marginal_risk'): NullableFloat,
        Optional('marginal_pct'): NullableFloat,
        'attributes': Schema([unicode])
    }, required=True)
])


RiskDecompositionSchema = Schema({
    Optional('exposure'): [Schema({'exposure': float, 'attributes': Schema([unicode])})],
    Optional('var'): RiskDecompositionComponentSchema,
    Optional('potential_upside'): RiskDecompositionComponentSchema,
    Optional('expected_shortfall'): RiskDecompositionComponentSchema,
    Optional('expected_upside'): RiskDecompositionComponentSchema,
    Optional('volatility'): RiskDecompositionComponentSchema,
}, required=True)


MultiLevelRiskDecompositionSchema = Schema([
    Any(
        Schema({
            Optional('exposure'): NullableFloat,
            Optional('var'): NullableFloat,
            Optional('potential_upside'): NullableFloat,
            Optional('expected_shortfall'): NullableFloat,
            Optional('expected_upside'): NullableFloat,
            Optional('volatility'): NullableFloat,
        }, required=True),
        Schema({
            Optional('exposure'): [Schema({'exposure': float, 'attributes': Schema([unicode])})],
            Optional('var'): RiskDecompositionComponentSchema,
            Optional('potential_upside'): RiskDecompositionComponentSchema,
            Optional('expected_shortfall'): RiskDecompositionComponentSchema,
            Optional('expected_upside'): RiskDecompositionComponentSchema,
            Optional('volatility'): RiskDecompositionComponentSchema,
        }, required=True),
       )
])



StressTestDecompositionSchema = Schema({
    unicode: [ExactSequence([Schema([unicode]), float])]
})


MultiLevelStressTestDecompositionSchema = Schema([
    StressTestDecompositionSchema
])


LiquidityRiskDecompositionSchema = Schema({
    unicode: [ExactSequence([Schema([unicode]), LiquidityRiskComponentSchema])]
})


MultiLevelLiquidityRiskDecompositionSchema = Schema([
    LiquidityRiskDecompositionSchema
])


RiskAttributionSchema = Schema({
    'allocation_risk': float,
    'selection_risk': float,
    'interaction_risk': float,
    'currency_effect': float,
    'local_fx_allocation_risk': float,
    'local_fx_selection_risk': float,
    'local_fx_interaction_risk': float,
}, required=True)


RiskAttributionDecompositionSchema = Schema([
    Schema({
        'allocation_risk': float,
        'selection_risk': float,
        'interaction_risk': float,
        'currency_effect': float,
        'local_fx_allocation_risk': float,
        'local_fx_selection_risk': float,
        'local_fx_interaction_risk': float,
        'attributes': [unicode],
    }, required=True)])


class TestRisk(object):
    def setUp(self):
        self.client = riskapi_client.connect()

        di = self.client.data_info()

        # collect attributes
        self.attributes = set()
        for holding in PORTFOLIO.holdings:
            self.attributes.add(tuple(holding.attributes))
        self.levels = []
        for lv in range(1, len(PORTFOLIO.holdings[0].attributes)+1):
            self.levels.append({x[:lv] for x in self.attributes})

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

        self.check_risk_decomposition_item(
            results['results'], self.attributes, functions, fields)

    def check_risk_decomposition_item(self, item, attributes, functions, fields):
        nt.assert_items_equal(
            item.keys(),
            functions)

        fields += ("attributes",)

        for name, item in item.iteritems():
            all_attributes = {tuple(x['attributes']) for x in item}
            nt.assert_items_equal(all_attributes, attributes)

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

    def test_multi_level_risk_decomposition(self):
        res = self.client.multi_level_risk_decomposition(PORTFOLIO, 0.99)

        self.check_errors(res)

        MultiLevelRiskDecompositionSchema(res['results'])

        for row in res['results']:
            self.check_fields(row, riskapi_client.DECOMPOSABLE_RISK_FUNCTIONS)

        # one row per level plus the total
        nt.assert_equal(len(res['results']), len(self.levels)+1)

        for attrs, item in zip(self.levels, res['results'][1:]):
            self.check_risk_decomposition_item(
                item, attrs, riskapi_client.DECOMPOSABLE_RISK_FUNCTIONS,
                ("marginal_risk", "marginal_pct",
                 "contribution_risk", "contribution_pct"))

    def test_multi_level_risk_decomposition_with_parameters(self):
        res = self.client.multi_level_risk_decomposition(
            PORTFOLIO, 0.95, ['var', 'volatility', 'exposure'],
            700, 5, 5, ['marginal_risk'])

        self.check_errors(res)

        MultiLevelRiskDecompositionSchema(res['results'])

        for row in res['results']:
            self.check_fields(row, ("var", "volatility", "exposure"))

        # one row per level plus the total
        nt.assert_equal(len(res['results']), len(self.levels)+1)

        for attrs, item in zip(self.levels, res['results'][1:]):
            self.check_risk_decomposition_item(
                item, attrs, ("var", "volatility", "exposure"),
                ("marginal_risk",))

    def test_relative_multi_level_risk_decomposition(self):
        res = self.client.relative_multi_level_risk_decomposition(PORTFOLIO, BENCHMARK, 0.99)

        self.check_errors(res)

        MultiLevelRiskDecompositionSchema(res['results'])

        for row in res['results']:
            self.check_fields(row, riskapi_client.DECOMPOSABLE_RISK_FUNCTIONS)

        # one row per level plus the total
        nt.assert_equal(len(res['results']), len(self.levels)+1)

        for attrs, item in zip(self.levels, res['results'][1:]):
            self.check_risk_decomposition_item(
                item, attrs, riskapi_client.DECOMPOSABLE_RISK_FUNCTIONS,
                ("marginal_risk", "marginal_pct",
                 "contribution_risk", "contribution_pct"))

    def test_relative_multi_level_risk_decomposition_with_parameters(self):
        res = self.client.relative_multi_level_risk_decomposition(
            PORTFOLIO, BENCHMARK, 0.99, ['var', 'volatility'],
            730, 1, 1, ['marginal_risk'])

        self.check_errors(res)

        MultiLevelRiskDecompositionSchema(res['results'])

        for row in res['results']:
            self.check_fields(row, ("var", "volatility"))

        # one row per level plus the total
        nt.assert_equal(len(res['results']), len(self.levels)+1)

        for attrs, item in zip(self.levels, res['results'][1:]):
            self.check_risk_decomposition_item(
                item, attrs, ("var", "volatility"),
                ("marginal_risk",))

    def test_relative_multi_level_risk_decomposition_with_exposure(self):
        # currently the error message is not clear
        with nt.assert_raises(riskapi_client.HTTPError) as exc:
            self.client.relative_multi_level_risk_decomposition(PORTFOLIO, BENCHMARK,
                0.95, ['var', 'volatility', 'exposure'],
                700, 5, 5, ['marginal_risk'])
        nt.assert_equal(exc.exception.code, 422)


    def test_stress_test_decomposition(self):
        res = self.client.stress_test_decomposition(PORTFOLIO)

        self.check_errors(res)

        StressTestDecompositionSchema(res['results'])

        nt.assert_items_equal(res['results'].keys(), STRESS_TEST_CODES)

    def test_stress_test_decomposition_with_codes(self):
        res = self.client.stress_test_decomposition(PORTFOLIO, STRESS_TEST_CODES[:10])

        self.check_errors(res)

        StressTestDecompositionSchema(res['results'])

        nt.assert_items_equal(res['results'].keys(), STRESS_TEST_CODES[:10])

    def test_relative_stress_test_decomposition(self):
        res = self.client.relative_stress_test_decomposition(PORTFOLIO, BENCHMARK)

        self.check_errors(res)

        StressTestDecompositionSchema(res['results'])

        nt.assert_items_equal(res['results'].keys(), STRESS_TEST_CODES)

    def test_relative_stress_test_decomposition_with_codes(self):
        res = self.client.relative_stress_test_decomposition(PORTFOLIO, BENCHMARK, STRESS_TEST_CODES[:10])

        self.check_errors(res)

        StressTestDecompositionSchema(res['results'])

        nt.assert_items_equal(res['results'].keys(), STRESS_TEST_CODES[:10])

    def test_multi_level_stress_test_decomposition(self):
        res = self.client.multi_level_stress_test_decomposition(PORTFOLIO)

        self.check_errors(res)

        MultiLevelStressTestDecompositionSchema(res['results'])

        # one row per level plus the total
        nt.assert_equal(len(res['results']), len(self.levels)+1)

        nt.assert_items_equal(res['results'][0].keys(), STRESS_TEST_CODES)

        for lvl, row in zip(self.levels, res['results'][1:]):
            nt.assert_items_equal(row.keys(), STRESS_TEST_CODES)
            for item in row.itervalues():
                nt.assert_items_equal(lvl, {tuple(x[0]) for x in item})

    def test_multi_level_stress_test_decomposition_with_codes(self):
        res = self.client.multi_level_stress_test_decomposition(PORTFOLIO, STRESS_TEST_CODES[:10])

        self.check_errors(res)

        MultiLevelStressTestDecompositionSchema(res['results'])

        # one row per level plus the total
        nt.assert_equal(len(res['results']), len(self.levels)+1)

        nt.assert_items_equal(res['results'][0].keys(), STRESS_TEST_CODES[:10])

        for lvl, row in zip(self.levels, res['results'][1:]):
            nt.assert_items_equal(row.keys(), STRESS_TEST_CODES[:10])
            for item in row.itervalues():
                nt.assert_items_equal(lvl, {tuple(x[0]) for x in item})

    def test_relative_multi_level_stress_test_decomposition(self):
        res = self.client.relative_multi_level_stress_test_decomposition(PORTFOLIO, BENCHMARK)

        self.check_errors(res)

        MultiLevelStressTestDecompositionSchema(res['results'])

        # one row per level plus the total
        nt.assert_equal(len(res['results']), len(self.levels)+1)

        nt.assert_items_equal(res['results'][0].keys(), STRESS_TEST_CODES)

        for lvl, row in zip(self.levels, res['results'][1:]):
            nt.assert_items_equal(row.keys(), STRESS_TEST_CODES)
            for item in row.itervalues():
                nt.assert_items_equal(lvl, {tuple(x[0]) for x in item})

    def test_relative_multi_level_stress_test_decomposition_with_codes(self):
        res = self.client.relative_multi_level_stress_test_decomposition(PORTFOLIO, BENCHMARK, STRESS_TEST_CODES[:10])

        self.check_errors(res)

        MultiLevelStressTestDecompositionSchema(res['results'])

        # one row per level plus the total
        nt.assert_equal(len(res['results']), len(self.levels)+1)

        nt.assert_items_equal(res['results'][0].keys(), STRESS_TEST_CODES[:10])

        for lvl, row in zip(self.levels, res['results'][1:]):
            nt.assert_items_equal(row.keys(), STRESS_TEST_CODES[:10])
            for item in row.itervalues():
                nt.assert_items_equal(lvl, {tuple(x[0]) for x in item})

    def test_liquidity_risk_decomposition(self):
        res = self.client.liquidity_risk_decomposition(PORTFOLIO)

        self.check_errors(res)

        LiquidityRiskDecompositionSchema(res['results'])

        nt.assert_items_equal(res['results'].keys(), LIQ_RISK_CODES)

        for item in res['results'].itervalues():
            nt.assert_items_equal({tuple(x[0]) for x in item}, self.attributes)


    def test_multi_level_liquidity_risk_decomposition(self):
        res = self.client.multi_level_liquidity_risk_decomposition(PORTFOLIO)

        self.check_errors(res)

        MultiLevelLiquidityRiskDecompositionSchema(res['results'])

        # one row per level plus the total
        nt.assert_equal(len(res['results']), len(self.levels)+1)

        nt.assert_items_equal(res['results'][0].keys(), LIQ_RISK_CODES)

        for lvl, row in zip(self.levels, res['results'][1:]):
            nt.assert_items_equal(row.keys(), LIQ_RISK_CODES)
            for item in row.itervalues():
                nt.assert_items_equal(lvl, {tuple(x[0]) for x in item})

    def test_aussie_bond_futures_NPV(self):
        res = self.client.aussie_bond_futures_NPV(AUSSIE_BOND_FUTURES_CODE, 100)
        nt.assert_is_instance(res, float)


    def test_risk_attribution(self):
        res = self.client.risk_attribution(
            PORTFOLIO, BENCHMARK, 0.99, "var", "zero_interaction")

        self.check_errors(res)

        RiskAttributionSchema(res['results'])


    def test_risk_attribution_decomposition(self):
        res = self.client.risk_attribution_decomposition(
            PORTFOLIO, BENCHMARK, 0.99, "var", "zero_interaction")

        self.check_errors(res)

        RiskAttributionDecompositionSchema(res['results'])
