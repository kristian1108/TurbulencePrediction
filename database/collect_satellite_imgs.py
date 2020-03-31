import requests
import shutil
import datetime
import os
from collect_pireps import ERROR_LOGGER, INFO_LOGGER
import boto3
from settings_secret import *


def get_url(data_type, day, time, glm=False):

    id_num = f'2020{day}{time}'

    if not glm:
        return f'https://cdn.star.nesdis.noaa.gov/GOES16/ABI/CONUS/{data_type}/{id_num}' \
               f'_GOES16-ABI-CONUS-{data_type}-5000x3000.jpg'

    else:
        return f'https://cdn.star.nesdis.noaa.gov/GOES16/GLM/CONUS/{data_type}/{id_num}' \
               f'_GOES16-GLM-CONUS-{data_type}-5000x3000.jpg'


def initialize_directory(image_categories):
    if os.path.isdir('../satellite_imagery/'):
        pass

    else:
        INFO_LOGGER.info('Initializing the satellite_imagery directory...')
        os.mkdir('../satellite_imagery/')
        for cat in image_categories:
            os.mkdir(f'../satellite_imagery/{cat}/')


def push_to_amazon(s3, path, bucket='turbulenceprediction'):

    s3.upload_file(path, bucket, path[3:])

    os.remove(path)


if __name__ == '__main__':

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    with open('next_time.txt', 'r') as file:
        next_time = file.read()

    band_imgs = ['AirMass', 'Sandwich', '05', '07', '08', '09', '10', '11', '12',
                   '14', '16']

    glm_imgs = ['EXTENT']

    all_imgs = band_imgs + glm_imgs

    initialize_directory(all_imgs)

    day = next_time.split(',')[0]
    time = next_time.split(',')[1]
    int_time = int(time)
    int_day = int(day)

    while len(time) < 4:
        time = '0'+time

    while len(day) < 3:
        day = '0'+day

    INFO_LOGGER.info(f'Next date successfully read as day: {day}, time: {time}')

    date = str(datetime.datetime.now()).split(' ')[0]

    for img in all_imgs:

        if img in glm_imgs:
            glm = True
        else:
            glm = False

        url = get_url(img, day, time, glm=glm)

        resp = requests.get(url, stream=True)

        if resp.status_code == 200:
            INFO_LOGGER.info(f'Image found at URL {url}')

            file_path = f'../satellite_imagery/{img}/{img}|{date}:{time}Z.jpeg'

            with open(file_path, 'wb+') as file:
                resp.raw.decode_content = True
                shutil.copyfileobj(resp.raw, file)
                push_to_amazon(s3, file_path)
                INFO_LOGGER.info(f'Image successfully saved to Amazon @ /satellite_imagery/{img}/{img}|{date}:{time}Z.jpeg')

    hours = int(time[:2])
    minutes = int(time[2:])

    if int_time < 2350 and minutes <= 50:
        next_time = int_time + 10
    elif int_time < 2350 and minutes > 50:
        next_time = f'{hours+1}0{(minutes+10)%60}'
    else:
        int_day += 1
        next_time = 1

    with open('next_time.txt', 'w') as file:
        file.write(f'{int_day},{next_time}')
        INFO_LOGGER.info(f'Next time written as day: {int_day}, time: {next_time}')
