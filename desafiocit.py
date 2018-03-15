#!/usr/bin/env python
# vim: set fileencoding=utf-8 :

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
from flask_api import status
from celery import Celery
import pika
import pandas as pd
from pandas.io import sql
import pymysql
import simplejson
import csv


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

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

rbconnection = pika.BlockingConnection(pika.ConnectionParameters(host=MY_HOST))
channel = rbconnection.channel()
channel.exchange_declare(exchange=MY_EXCHANGE, exchange_type='direct')
myqueue_object = channel.queue_declare(queue=MY_QUEUE)
channel.queue_bind(exchange=MY_EXCHANGE, queue=myqueue_object.method.queue)

dbconnection = pymysql.connect(
        host=MY_DBHOST, user=MY_DBUSER, password=MY_DBPASSWORD,
        db=MY_DBDATABASE, charset='latin1',
        cursorclass=pymysql.cursors.DictCursor)


def cards_consumer_callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)


@celery.task
def moveall_async(conn):
    """Background task to move all cards"""
    with conn.cursor() as cursor:
        sqlquery = "SELECT ExpansionId,Name from magicexpansion"
        amount = cursor.execute(sqlquery)
        for expansion in cursor.fetchall():
            expansion_id = expansion['ExpansionId']
            movecards(expansion_id)
    return


@app.route('/moveall', methods=['GET'])
def moveall():
    moveall_async.delay(dbconnection)
    return 'Accepted.', 202


@app.route('/movecards/:<expansion_id>', methods=['POST'])
def movecards(expansion_id):
    with dbconnection.cursor() as cursor:
        expansion_name=''
        # validate expansion_id:
        sqlquery = "SELECT * FROM magicexpansion WHERE ExpansionId = %s"
        amount = cursor.execute(sqlquery, expansion_id)
        if amount == 0:
            content = 'Not found!'
            return content, 404
        else:
            card=cursor.fetchone()
            expansion_name=card['Name']

        sqlquery = "SELECT * FROM magiccard WHERE ExpansionId = %s"
        amount=cursor.execute(sqlquery, expansion_id)
        for card in cursor.fetchall():
            json_record = simplejson.dumps(card)
            channel.basic_publish(
                    exchange=MY_EXCHANGE,
                    routing_key=MY_ROUTINGKEY,
                    body=json_record
                    )


    content = expansion_name + ' ' + str(amount)
    return content


@app.route('/card/:<card_id>', methods=['GET'])
def card(card_id):
    content = ''
    return content

def callback_consumer(ch, method, properties, body):
    jsonbody=json.loads(body)
    f=csv.writer(open(MY_OUTFILE,'a'))
    for row in jsonbody: # Hopefully only one
        f.writerow(row.values())
    f.close()


@celery.task
def cards_consumer_service(channel,MY_QUEUE):

    f=open(MY_OUTFILE, "rw+")
    f.truncate()
    f.close()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback_consumer,
                          queue=MY_QUEUE,
                          no_ack=False)
    channel.start_consuming()


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

