#!/usr/bin/env python
""" Energy Price API exporter """

import sys
import argparse
import time
from datetime import datetime
from dateutil.parser import isoparse
import requests
import prometheus_client
from prometheus_client import Gauge


class EnergyCollector:
    """ Class for collecting and exporting energy price metrics """

    base_url = 'https://www.elprisenligenu.dk/api/v1/prices'

    # Hardcode gauges since there are so few
    per_kwh_gauge = Gauge('energy_price_per_kwh', 'Price pr kW', ['region', 'currency'])
    exr_used_gauge = Gauge('energy_price_exr_used', 'EUR to DKK exchange rate')

    regions = ['DK1', 'DK2']

    def __init__(self, args):
        """Construct the object and parse the arguments."""
        self.args = self._parse_args(args)

        # Start by getting the data
        self.data = self.get_data_files()

    @staticmethod
    def _parse_args(args):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-l',
            '--listen',
            dest='listen',
            default='0.0.0.0:9555',
            help='Listen host:port, i.e. 0.0.0.0:9417'
        )
        arguments = parser.parse_args(args)
        return arguments

    def get_data_files(self):

        # base_url/[year]/[month]-[day]_[region].json'
        year = datetime.today().strftime('%Y')
        month_day = datetime.today().strftime('%m-%d')

        data = {}

        for region in self.regions:

            url = f'{self.base_url}/{year}/{month_day}_{region}.json'
            data[region] = self.api_read_request(url)

        print('Fetched new data')
        return data

    def api_read_request(self, url):
        """ Make API request with payload """

        try:
            api_r = requests.get(url, timeout=(5, 30))
            response = api_r.json()
        except Exception as err:
            print(f'API request failed or invalid response ({err})')
            return None

        return response

    def convert_iso_to_epoch(self, timestamp):

        return int(isoparse(timestamp).timestamp())

    def extract_data_point(self, region, currency, timestamp=int(time.time())):

        currency_str_map = {
            'DKK': 'DKK_per_kWh',
            'EUR': 'EUR_per_kWh',
            'EXR': 'EXR'
        }

        for region, data in self.data.items():
            if region != region:
                continue
            for hour in data:
                if timestamp >= self.convert_iso_to_epoch(hour['time_start']) and \
                   timestamp < self.convert_iso_to_epoch(hour['time_end']):
                    return hour[currency_str_map[currency]]

        # If we get here - update data
        self.data = self.get_data_files()
        print('Sleeping 60 and - re-running')
        time.sleep(60)
        return self.extract_data_point(region, currency, timestamp)

    def process(self):

        self.per_kwh_gauge.labels('DK1', 'DKK').set(self.extract_data_point('DK1', 'DKK'))
        self.per_kwh_gauge.labels('DK2', 'DKK').set(self.extract_data_point('DK2', 'DKK'))
        self.per_kwh_gauge.labels('DK1', 'EUR').set(self.extract_data_point('DK1', 'EUR'))
        self.per_kwh_gauge.labels('DK2', 'EUR').set(self.extract_data_point('DK3', 'EUR'))
        self.exr_used_gauge.set(self.extract_data_point('DK1', 'EXR'))


def run():
    """ Run the main loop """

    EC = EnergyCollector(sys.argv[1:])
    args = EC.args

    (ip_addr, port) = args.listen.split(':')
    print(f'Starting listener on {ip_addr}:{port}')
    prometheus_client.start_http_server(port=int(port),
                                        addr=ip_addr)
    print('Starting main loop')
    while True:
        EC.process()
        time.sleep(300)


if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        print('Caught keyboard interrupt - exiting')
    except Exception as main_err:
        print(f'Caught unknown exception ({main_err})')
        raise
