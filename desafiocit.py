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
from flask.ext.api import status
from celery import Celery
import pika
import pandas as pd
from pandas.io import sql
import pymysql
import simplejson


MY_EXCHANGE = 'cards'
MY_QUEUE = 'cards'
MY_ROUTINGKEY = 'moving_cards'


MY_DBHOST = '127.0.0.1'  # if we used localhost it would use a file socket
MY_DBUSER = 'root'
MY_DBPASSWORD = 'root'
MY_DBDATABASE = 'tcgplace'
MY_DBTABLE = 'magiccards'


MY_BROKERURL = 'amqp://guest:guest@localhost:5672//'
MY_RESULTBACKEND = 'amqp://guest:guest@localhost:5672//'
MY_OUTFILE = '/tmp/cards_db.txt'


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = MY_SQLURI
app.config['CELERY_BROKER_URL'] = MY_BROKERURL
app.config['CELERY_RESULT_BACKEND'] = MY_RESULTBACKEND

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

rbconnection = pika.BlockingConnection(pika.ConnectionParameters(host=MY_HOST))
channel = rbconnection.channel()
channel.exchange_declare(exchange=MY_EXCHANGE, exchange_type='direct')
myqueue_object = channel.queue_declare(queue=MY_QUEUE)
channel.queue_bind(exchange=MY_EXCHANGE, queue=myqueue_object)

dbconnection = pymysql.connect(
        host=MY_DBHOST, user=MY_DBUSER, password=MY_DBPASSWORD,
        db=MY_DBDATABASE, charset='latin1',
        cursorclass=pymysql.cursors.DictCursor)


def cards_consumer_callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)


@celery.task
def moveall_async():
    """Background task to move all cards"""
    return


@app.route('/moveall', methods=['GET'])
def moveall():
    moveall_async()
    content = ''
    return content, status.HTTP_202_ACCEPTED


@app.route('/movecards/:<expansion_id>', methods=['POST'])
def movecards(expansion_id):
    with conn.cursor() as cursor:
        expansion_name=''
        # validate expansion_id:
        sqlquery = "SELECT * FROM magicexpansion WHERE ExpansionId = %s"
        amount = cursor.execute(sqlquery, expansion_id)
        if amount == 0:
            return '', status.404_NOT_FOUND
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


def cards_consumer():
    return


def getcsv_by_id(file, gathererid):
    return
