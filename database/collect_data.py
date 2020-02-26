import requests
import xml.etree.ElementTree as eT
import datetime
import glob
import string
import random
from mongo_utils import MongoUtility


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

    resp = requests.get(url)
    current_time = str(datetime.datetime.now()).replace(' ', '_')

    with open(f'airep_{current_time}.xml', 'wb') as f:
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

    return turbulence_reports


def generate_id(length=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def get_aireps():
    return [report for report in glob.glob('*.xml') if 'airep' in report]


def main():
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

    return final

def push_to_db():

    pireps = main()
    mongo = MongoUtility()

    for rep in pireps:
        mongo.send_sample(rep)

def reset_database():
    mongo = MongoUtility()
    mongo.remove_all_documents(mongo.pilotreports)


if __name__ == "__main__":
    push_to_db()