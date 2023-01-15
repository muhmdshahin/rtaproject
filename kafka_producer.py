from confluent_kafka import Producer

import csv
import json
import time
import logging
import sys



logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

p=Producer({'bootstrap.servers':'localhost:9092'})

#####################

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
        
#####################
print('Kafka Producer has been initiated...')

def produce_from_file(csvFilePath):

    # read csv file
    with open(csvFilePath, encoding='utf-8') as csvf:
        # load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf)

        # convert each csv row into python dict
        for row in csvReader:
            # add this python dict to json array
            data = row
            m = json.dumps(data)
            p.poll(1)
            p.produce('user-tracker', m.encode('utf-8'), callback=receipt)
            p.flush()
            time.sleep(1)

    # convert python jsonArray to JSON String and write to file


def main():
    produce_from_file('rta.csv')

        
if __name__ == '__main__':
    main()