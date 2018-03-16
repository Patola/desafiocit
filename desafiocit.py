#!/usr/bin/env python
# coding=latin-1

"""desafiocit: a simple microservices implementation for CI&T."""

__author__ = "Cláudio 'Patola' Sampaio"
__copyright__ = "Copyright 2018 by Cláudio Sampaio"
__credits__ = ["Cláudio 'Patola' Sampaio", "CI&T"]
__license__ = "MIT License"
__version__ = "1.0"
__maintainer__ = "Cláudio 'Patola' Sampaio"
__email__ = "patola@gmail.com"
__status__ = "Prototype"


from flask import Flask
from celery import Celery
from unidecode import unidecode
import pika
import pandas
import unicodedata
from pandas.io import sql
import pymysql
import json
import simplejson
import csv

# CELERY_TASK_SERIALIZER = 'pickle'

MY_EXCHANGE = 'cards'
MY_QUEUE = 'cards'
MY_ROUTINGKEY = 'moving_cards'
MY_HOST = '127.0.0.1'


MY_DBHOST = '127.0.0.1'  # if we used localhost it would use a file socket
MY_DBUSER = 'root'
MY_DBPASSWORD = 'root'
MY_DBDATABASE = 'tcgplace'
MY_DBTABLE = 'magiccards'


MY_BROKERURL = 'amqp://guest:guest@localhost:5672//'
MY_RESULTBACKEND = 'amqp://guest:guest@localhost:5672//'
MY_OUTFILE = '/tmp/cards_db.txt'


app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = MY_BROKERURL
app.config['CELERY_RESULT_BACKEND'] = MY_RESULTBACKEND
app.config['BROKER_HEARTBEAT'] = 0

# celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'],
#                 backend=app.config['CELERY_RESULT_BACKEND'])
# celery.conf.update(app.config)

rbconnection = pika.BlockingConnection(pika.ConnectionParameters(host=MY_HOST))
channel = rbconnection.channel()
channel.exchange_declare(exchange=MY_EXCHANGE, exchange_type='direct')
myqueue_object = channel.queue_declare(queue=MY_QUEUE)
channel.queue_bind(exchange=MY_EXCHANGE, queue=myqueue_object.method.queue,
        routing_key=MY_ROUTINGKEY)

dbconnection = pymysql.connect(
        host=MY_DBHOST, user=MY_DBUSER, password=MY_DBPASSWORD,
        db=MY_DBDATABASE, charset='latin1',
        cursorclass=pymysql.cursors.DictCursor)

def sanitize(arg):
    if isinstance(arg,str):
        return unidecode(unicode(arg,'utf8'))
    elif isinstance(arg, unicode):
        return unidecode(arg)
    elif isinstance(arg, int):
        return str(arg)
    elif isinstance(arg, dict):
        for key, value in arg.iteritems():
            arg[key] = sanitize(value)
        return arg

def make_celery(app):
    celery = Celery(app.import_name, backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


celery = make_celery(app)

@celery.task()
def moveall_async():
    """Background task to move all cards"""
    rbconnection2 = pika.BlockingConnection(pika.ConnectionParameters(host=MY_HOST))
    channel2 = rbconnection2.channel()
    channel2.exchange_declare(exchange=MY_EXCHANGE, exchange_type='direct')
    channel2.queue_declare(queue=MY_QUEUE)
    channel2.queue_bind(exchange=MY_EXCHANGE, queue=myqueue_object.method.queue,
        routing_key=MY_ROUTINGKEY)

    dbconnection2 = pymysql.connect(
        host=MY_DBHOST, user=MY_DBUSER, password=MY_DBPASSWORD,
        db=MY_DBDATABASE, charset='latin1',
        cursorclass=pymysql.cursors.DictCursor)


    with dbconnection2.cursor() as cursor2:
        sqlquery2 = "SELECT * FROM magiccard ORDER BY ExpansionId"
        cursor2.execute(sqlquery2)
        for card2 in cursor2.fetchall():
            json_record2 = sanitize(simplejson.dumps(card2,
                           encoding='latin1'))
            channel2.basic_publish(
                     exchange=MY_EXCHANGE,
                     routing_key=MY_ROUTINGKEY,
                     body=json_record2,
                     )
    rbconnection2.close()
    dbconnection2.close()           
    f.close()
    return


@app.route('/moveall', methods=['GET'])
def moveall():
    moveall_async.apply_async()
    return 'Accepted.', 202


@app.route('/movecards/:<expansion_id>', methods=['POST'])
def movecards(expansion_id):
    with dbconnection.cursor() as cursor:
        expansion_name = ''
        # validate expansion_id:
        sqlquery = "SELECT * FROM magicexpansion WHERE ExpansionId = %s"
        amount = cursor.execute(sqlquery, expansion_id)
        if amount == 0:
            content = 'Not found!'
            writelog("movecards: not found")
            return content, 404
        else:
            card=cursor.fetchone()
            expansion_name=card['Name']
            writelog("movecards: "+ expansion_name)

        sqlquery = "SELECT * FROM magiccard WHERE ExpansionId = %s"
        amount=cursor.execute(sqlquery, expansion_id)
        for card in cursor.fetchall():
            json_record = sanitize(simplejson.dumps(card))  # sanitize string
            channel.basic_publish(
                     exchange=MY_EXCHANGE,
                     routing_key=MY_ROUTINGKEY,
                     body=json_record
                     )
 

    content = expansion_name + ' ' + str(amount)
    return content


@app.route('/card/:<card_id>', methods=['GET'])
def card(card_id):
    f = open(MY_OUTFILE, 'r')
    fcsv = csv.reader(f)
    for row in fcsv:
        if str(row[19] == str(card_id):
            return str(row)
    content = ''
    return content, 404


def cards_consumer_callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    f = open(MY_OUTFILE, 'a')
#    simplejson.dump(body, f)
    data = simplejson.JSONDecoder().decode(body)
    sanitize(data)
    output = csv.writer(f)
    output.writerow(data.values())

    f.close()
    return
 

@app.route('/card/:<card_id>', methods=['GET'])
def getcard(card_id):
    try:
        with open(MY_OUTFILE, newline='', mode='r') as mycsv:
            reader = csv.reader(mycsv)
            for row in reader:
                if row[0] == card_id:
                    mycsv.close()
                    return row
            mycsv.close()
            content = 'Card not found!'
            return content, 404
    except IOError:
        return 'No outfile found', 412

@celery.task()
def cards_consumer_service():
    f = open(MY_OUTFILE, "w") # initialize cards file
    f.truncate()
    f.close()
    rbconnection3 = pika.BlockingConnection(pika.ConnectionParameters(host=MY_HOST))
    channel3 = rbconnection3.channel()
    channel3.exchange_declare(exchange=MY_EXCHANGE, exchange_type='direct')
    channel3.queue_declare(queue=MY_QUEUE)
    channel3.queue_bind(exchange=MY_EXCHANGE, queue=myqueue_object.method.queue,
        routing_key=MY_ROUTINGKEY)

   
    channel3.basic_consume(cards_consumer_callback,
                          queue=MY_QUEUE, no_ack=False)
    channel3.start_consuming()


cards_consumer_service.delay()
