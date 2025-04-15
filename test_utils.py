import pytest

import utils


def test_thing_to_surt_host_name():
    tests = [
        ('va', 'va'),
        ('va,', 'va,'),
        ('va,*', 'va,'),

        ('https://example.com/', 'com,example'),
        ('https://sub.example.com/', 'com,example,sub'),
        ('https://*.example.com/', 'com,example,'),

        ('*.example.com/', 'com,example,'),
        #('.example.com/', 'com,example,'),  # python's library drops that leading dot
        ('sub.example.com', 'com,example,sub'),
        ('*.sub.example.com', 'com,example,sub,'),
    ]

    value_error = [
        ('example.com/foo', ''),
        ('example.com,', ''),
    ]

    for t1, t2 in tests:
        assert utils.thing_to_surt_host_name(t1) == t2, t1

    for t1, t2 in value_error:
        with pytest.raises(ValueError):
            utils.thing_to_surt_host_name(t1)
