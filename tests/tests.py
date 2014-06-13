#!/usr/bin/env python

from __future__ import unicode_literals
import unittest

import sys
sys.path.append('../src')

from stats import Stats, StatsException


class TestStats(unittest.TestCase):

    def test_valid_input(self):

        stats = Stats('HostState.txt', 'InstanceState.txt', 'Statistics.txt')

        stats.get_host_clustering()
        stats.get_data_center_clustering()
        stats.get_slot_usage()

        stats.output_data()

        with open('Statistics.txt') as f:
            data = f.read()

        assert data == 'HostClustering:8,60.0\nDatacentreClustering:13,100.0\nAvailableHosts:10,3,2,5,6', \
            'Unexpected results'

    def test_invalid_input(self):

        try:
            Stats('HostState_broken.txt', 'InstanceState_broken.txt', 'Statistics_broken.txt')
            assert False, 'Report generation did not fail with bad data as expected'
        except StatsException, e:

            expected_errors = [
                "HostState_broken.txt: Too many values ['extra_value']",
                "HostState_broken.txt: Missing fields [u'numberOfSlots', u'datacentreID']",
                "HostState_broken.txt: Invalid field <name> [numberOfSlots] <value> [cant_be_a_string] <error> "
                "[invalid literal for int() with base 10: 'cant_be_a_string']",
                "InstanceState_broken.txt: Too many values ['1']"
            ]

            for expected_error in expected_errors:
                assert expected_error in e.message, 'Expected error not found [{}]'.format(expected_error)


if __name__ == '__main__':
    unittest.main()