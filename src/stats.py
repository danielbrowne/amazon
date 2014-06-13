#!/usr/bin/env python

from __future__ import unicode_literals
from collections import defaultdict, OrderedDict
from copy import deepcopy
import csv
import logging


logger = logging.getLogger(__name__)


class StatsException(Exception):
    pass


class Stats(object):
    """
    Generate stats regarding virtual hosts
    """

    def __init__(self, hosts_fpath, instances_fpath, output_fpath):

        self.output_fpath = output_fpath

        self.data = dict()

        def validate_row(row, fieldnames):

            r = dict(filter(lambda i: i[-1] is not None, row.items()))

            if len(r) > len(fieldnames):
                raise StatsException('Too many values {}'.format(r.get('extra')))

            if len(r) < len(fieldnames):
                raise StatsException('Missing fields {}'.format(
                    list(set(fieldnames.keys()).difference(set(r.keys())))))

            for name, value in r.iteritems():
                if not isinstance(value, basestring):
                    raise StatsException('Invalid field [{}] [{}]'.format(name, value))

                expected_type = fieldnames.get(name)
                if expected_type != basestring:
                    try:
                        expected_type(value)
                    except (ValueError, TypeError), e:
                        raise StatsException('Invalid field <name> [{}] <value> [{}] <error> [{}]'.format(
                            name, value, e.message))

        try:
            fpath_errors = defaultdict(list)

            fpath = hosts_fpath
            fieldnames = OrderedDict([('hostID', basestring), ('numberOfSlots', int), ('datacentreID', basestring)])
            with open(fpath) as f:
                self.hosts_data = []
                for r in csv.DictReader(f, fieldnames=fieldnames.keys(), restkey='extra'):

                    try:
                        validate_row(r, fieldnames)
                    except StatsException, e:
                        fpath_errors[fpath].append(e)

                    self.hosts_data.append(r)

            fpath = instances_fpath
            fieldnames = OrderedDict([('instanceID', basestring), ('customerID', basestring), ('hostID', basestring)])
            with open(fpath) as f:
                self.instance_data = []
                for r in csv.DictReader(f, fieldnames=fieldnames.keys(), restkey='extra'):

                    try:
                        validate_row(r, fieldnames)
                    except StatsException, e:
                        fpath_errors[fpath].append(e)

                    self.instance_data.append(r)

        except IOError, e:
            if e.errno == 2:
                raise StatsException('File not found [{}]'.format(fpath))
            else:
                raise

        except csv.Error, e:
            raise StatsException('Error reading file [{}]'.format(fpath))

        if fpath_errors:
            errors_message = []
            for fpath, errors in fpath_errors.iteritems():
                errors_message.append('\n'.join([fpath + ': ' + e.message for e in errors]))

            raise StatsException('Data validation errors\n{}'.format('\n'.join(errors_message)))

        self.host_clustering = dict()
        self.data_center_clustering = dict()
        self.slot_usage = dict()

    def get_data_center_clustering(self):
        """
        Find the percentage of each customers hosts per data center
        """

        customer_template = {
            'total_instances': float(0),
            'data_centers': defaultdict(float)
        }

        self.data_center_clustering = dict()

        for instance in self.instance_data:
            customer_id = instance['customerID']

            data_center_id = None
            for host in self.hosts_data:
                if host['hostID'] == instance['hostID']:
                    data_center_id = host['datacentreID']
                    break

            if data_center_id is None:
                raise StatsException('No data center found for instance [{}] on host [{}]'.format(
                    instance['instanceID'], instance['hostID']))

            customer = self.data_center_clustering.get(customer_id, deepcopy(customer_template))
            customer['total_instances'] += 1
            customer['data_centers'][data_center_id] += 1
            self.data_center_clustering[customer_id] = customer

        for customer_id, customer_data in self.data_center_clustering.iteritems():
            total_instances = customer_data['total_instances']
            for data_center_id, data_center_instance_count in customer_data['data_centers'].iteritems():
                percentage_instances_on_data_center = round((data_center_instance_count / total_instances) * 100, 2)
                customer_data['data_centers'][data_center_id] = percentage_instances_on_data_center

    def get_host_clustering(self):
        """
        Find the percentage of each customers hosts per host
        """

        customer_template = {
            'total_instances': float(0),
            'hosts': defaultdict(float)
        }

        self.host_clustering.clear()

        for instance in self.instance_data:
            customer_id = instance['customerID']
            host_id = instance['hostID']

            customer = self.host_clustering.get(customer_id, deepcopy(customer_template))
            customer['total_instances'] += 1
            customer['hosts'][host_id] += 1
            self.host_clustering[customer_id] = customer

        for customer_id, customer_data in self.host_clustering.iteritems():
            total_instances = customer_data['total_instances']
            for host_id, host_instance_count in customer_data['hosts'].iteritems():
                percentage_instances_on_host = round((host_instance_count / total_instances) * 100, 2)
                customer_data['hosts'][host_id] = percentage_instances_on_host

    def get_slot_usage(self):
        """
        Get slot usage per host
        """

        self.slot_usage.clear()

        for host in self.hosts_data:
            slot_count = int(host['numberOfSlots'])

            used_slots = 0
            for instance in self.instance_data:
                if instance['hostID'] == host['hostID']:
                    used_slots += 1

            if used_slots > slot_count:
                raise StatsException('Host [{}] slots {}/{}'.format(host['hostID'], used_slots, slot_count))

            self.slot_usage[host['hostID']] = {
                'total': slot_count,
                'used': used_slots,
                'available': slot_count - used_slots
            }

    def output_data(self):
        """
        Write requested data to file
        """

        max_host_clustering_customer = None
        max_host_clustering = 0
        for customer_id, customer_data in self.host_clustering.iteritems():
            host_max = max(customer_data['hosts'].values())
            if host_max > max_host_clustering:
                max_host_clustering = host_max
                max_host_clustering_customer = customer_id

        max_dc_clustering_customer = None
        max_dc_clustering = 0
        for customer_id, customer_data in self.data_center_clustering.iteritems():
            dc_max = max(customer_data['data_centers'].values())
            if dc_max > max_dc_clustering:
                max_dc_clustering = dc_max
                max_dc_clustering_customer = customer_id

        available_hosts = [host_id for host_id, usage in self.slot_usage.iteritems() if usage['available'] > 0]

        try:
            with open(self.output_fpath, 'w') as f:
                if max_host_clustering_customer:
                    f.write('HostClustering:{},{}\n'.format(max_host_clustering_customer, max_host_clustering))
                if max_dc_clustering_customer:
                    f.write('DatacentreClustering:{},{}\n'.format(max_dc_clustering_customer, max_dc_clustering))
                if available_hosts:
                    f.write('AvailableHosts:{}'.format(','.join(available_hosts)))

        except IOError, e:
            raise StatsException('Unable to write to file [{}]'.format(self.output_fpath))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--hosts_file', default='HostState.txt')
    parser.add_argument('--instances_file', default='InstanceState.txt')
    parser.add_argument('--output_file', default='Statistics.txt')

    args = parser.parse_args()

    stats = Stats(args.hosts_file, args.instances_file, args.output_file)

    stats.get_host_clustering()
    stats.get_data_center_clustering()
    stats.get_slot_usage()

    stats.output_data()