import requests
import shutil
import datetime
import os
from collect_pireps import ERROR_LOGGER, INFO_LOGGER


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


if __name__ == '__main__':

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

            with open(f'../satellite_imagery/{img}/{img}|{date}:{time}Z.jpeg', 'wb+') as file:
                resp.raw.decode_content = True
                shutil.copyfileobj(resp.raw, file)
                INFO_LOGGER.info(f'Image successfully saved to ../satellite_imagery/{img}/{img}|{date}:{time}Z.jpeg')

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
