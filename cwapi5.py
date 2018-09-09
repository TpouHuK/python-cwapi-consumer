import pika
import json
import time
import database
import threading
import socket import gethostbyname, create_connection
import logging
from os import linesep
from time import sleep

APP_NAME = 'foobar_app_xidui'
API_KEY = 'fxW9aZJSmoHyMtWf0Z6LtcC2PaG1mLyX38uIyW'
URL = f'amqps://{APP_NAME}:{API_KEY}@api.chtwrs.com:5673/'

CHECK_INTERNET = 'google.com'
DB_WRITE_SEC = 60.0
RECONNECT_SEC = 5
LOG_FILE = 'python_cwapi.log'

CONSOLE_LOG_LEVEL = logging.DEBUG

CASTLE_NUMBERS = {'🖤':1, '☘️':2, '🍁':3,
        '🐢':4, '🦇':5, '🌹':6, '🍆':7}

def from_castle_to_number(castle):
    return CASTLE_NUMBERS.get(castle, '8')

def recieve_messages_deals(ch, method, properties, body):
    print(ch)
    print(method)
    print(properties)
    print(body)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    string_body = body.decode()
    data = json.loads(string_body)

    logger.debug((f"*{data['sellerCastle']}{data['sellerName']} => "
           f"{data['buyerCastle']}{data['buyerName']} "
           f"{data['qty']}x{data['price']}💰 {data['item']}"))

    try:
        data['time'] = int(properties.timestamp)
    except:
        data['time'] = time.time()

    data['sellerCastle'] = from_castle_to_number(data['sellerCastle'])
    data['buyerCastle'] = from_castle_to_number(data['buyerCastle'])
    database.deals_push(data)

def recieve_messages_offers(ch, method, properties, body):
    print(ch)
    print(method)
    print(properties)
    print(body)
    ch.basic_ack(delivery_tag = method.delivery_tag)
    string_body = body.decode()
    data = json.loads(string_body)

    logger.debug((f"^{data['sellerCastle']}{data['sellerName']} || "
           f"{data['qty']}x{data['price']}💰 {data['item']}"))

    try:
        data['time'] = int(properties.timestamp)
    except:
        data['time'] = time.time()

    data['sellerCastle'] = from_castle_to_number(data['sellerCastle'])
    database.offers_push(data)

def connect_to_server():
    logger.info('Connecting to CWAPI')
    connection_parameters = pika.URLParameters(URL)
    try:
        connection = pika.BlockingConnection(connection_parameters)
    except pika.exceptions.ProbableAuthenticationError as e:
        stop_db_update()
        logger.critical(str(e))
        exit()

    channel = connection.channel()
    channel.basic_qos(prefetch_count=2)

    deals = APP_NAME + '_deals'
    offers = APP_NAME + '_offers'
    au_digest = APP_NAME + '_au_digest'

    try:
        channel.basic_consume(recieve_messages_deals, deals)
        channel.basic_consume(recieve_messages_offers, offers)
    except pika.exceptions.ChannelClosed as e:
        stop_db_update()
        logger.critical(str(e))
        exit()

    logger.info('Connected to CWAPI')

    return channel, connection

def check_internet_on():
    try:
        host = socket.gethostbyname(CHECK_INTERNET)
        s = socket.create_connection((host, 80), 2)
        return True
    except Exception as e:
        logger.warning(e)
    return False

def timer_commit_database():
    while not database_update_timer.is_set():
        database_update_timer.wait(timeout=DB_WRITE_SEC)
        database.database_commit()
        logger.debug('Database saved')

def stop_db_update():
    database_update_timer.set()

def setup_logging():
    file_formatter = logging.Formatter(
        f'|%(asctime)s  %(levelname)s|{linesep}%(message)s{linesep}',
        '%Y-%m-%d %H:%M:%S')
    console_formatter = logging.Formatter(
        '%(message)s %(asctime)s [%(levelname)s]',
        '%H:%M:%S')

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(console_formatter)
    stream_handler.setLevel(CONSOLE_LOG_LEVEL)

    logger = logging.getLogger('cwapi')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

logger = setup_logging()

database_update_timer = threading.Event()
database_update = threading.Thread(target=timer_commit_database)
database_update.start()

while True:
    try:
        while not check_internet_on():
            sleep(RECONNECT_SEC)
            logger.warning('Internet unreachable')

        logger.info('Trying to consume')
        channel, connection = connect_to_server()
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info('KeyboardInterrupt')
        connection.close()
        break
    except pika.exceptions.ConnectionClosed:
        logger.warning(f'Connection closed =>'
                       f'Try to reconnect in {RECONNECT_SEC} seconds')
        sleep(RECONNECT_SEC)

stop_db_update()
