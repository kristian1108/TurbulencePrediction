#!../venv/bin/python3

import requests
import xml.etree.ElementTree as eT
import datetime
import glob
import string
import random
from mongo_utils import MongoUtility
import logging
from logging import INFO, ERROR


INFO_LOGGER = logging.getLogger('info_logger')
ERROR_LOGGER = logging.getLogger('error_logger')
ERROR_LOGGER.isEnabledFor(ERROR)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',
                    level=INFO, filename='../logs/collect_data.log', filemode='w')

def get_url(hours_before):

    url = 'https://www.aviationweather.gov/adds/dataserver_current/httpparam?' \
          'dataSource=aircraftreports' \
          '&requestType=retrieve' \
          '&format=xml' \
          '&minAltitudeFt=5000' \
          '&maxAltitudeFt=50000' \
          f'&hoursBeforeNow={hours_before}'

    return url


def load_rss():

    hours_before = 1

    url = get_url(hours_before)

    try:
        resp = requests.get(url)
        INFO_LOGGER.info(f'Successfully fetched data from {url}')
    except Exception:
        ERROR_LOGGER.exception(f'Failed to fetch data from {url}')

    current_time = str(datetime.datetime.now()).replace(' ', '_')

    with open(f'../airep_files/airep_{current_time}.xml', 'wb') as f:
        f.write(resp.content)


def parse_xml(file):
    tree = eT.parse(file)
    root = tree.getroot()

    turbulence_reports = {}

    for item in root.findall('.//AircraftReport'):
        id = generate_id()
        rep = {}
        for child in item:
            if child.tag == 'turbulence_condition':
                rep[child.tag] = {'intensity': child.get('turbulence_intensity'), 'freq': child.get('turbulence_freq')}
            else:
                rep[child.tag] = child.text

        turbulence_reports[id] = rep

    INFO_LOGGER.info('Successfully parsed XML')

    return turbulence_reports


def generate_id(length=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def get_aireps():
    return [report for report in glob.glob('../airep_files/*.xml') if 'airep' in report]


def prep_data():
    load_rss()
    report_names = get_aireps()

    pireps = parse_xml(report_names[0])

    final = []

    for key, value in pireps.items():

        receipt = value['receipt_time'].replace('T', ':').replace('Z', '')+'/UTC'
        observation = value['observation_time'].replace('T', ':').replace('Z', '')+'/UTC'

        value['receipt_time'] = receipt
        value['observation_time'] = observation

        sample = {'key': key, 'payload': value}
        final.append(sample)

    INFO_LOGGER.info('Successfully prepared data for database.')

    return final


def push_to_db():

    pireps = prep_data()
    mongo = MongoUtility()

    try:
        for rep in pireps:
            mongo.send_sample(rep)
        INFO_LOGGER.info('Successfully sent samples to the database.')
    except Exception:
        ERROR_LOGGER.exception('Something went wrong pusing to the database.')


if __name__ == "__main__":
    push_to_db()
    INFO_LOGGER.info('Finished data collection sequence.')