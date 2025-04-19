import pytest

import graph


tests = [
    (('hcrank10', 'fetch_200'), ('fetch_200',), ('hcrank10',)),
    (('hcrank', 'fetch_200'), ('fetch_200',), ('hcrank',)),
    (('fetch_200', 'hcrank'), ('fetch_200',), ('hcrank',)),
    (('hcrank10', 'fetch_200'), ('fetch_200',), ('hcrank10',)),
    (('hcrank',), ('hcrank',), tuple()),
    #(('crawl', 'nutch_fetched_pct', 'nutch_unfetched_pct', 'nutch_gone_pct', 'nutch_redirTemp_pct', 'nutch_redirPerm_pct', 'nutch_notModified_pct'),
]

fails = [
    ('hcrank10', 'fetch_200', 'foo_pct'),
    ('hcrank10', 'fetch_200', 'foo_pct', 'rank'),
    ('hcrank10', 'fetch_200', 'foo_pct'),
]


def test_left_right():
    for test in tests:
        t, left, right = test
        assert graph.left_right(t) == (left, right)
        t2 = ['crawl']
        t2.extend(t)
        print('t2', t2)
        assert graph.left_right(t2) == (left, right)


def test_left_right_fail():
    for t in fails:
        with pytest.raises(ValueError):
            assert graph.left_right(t), t
