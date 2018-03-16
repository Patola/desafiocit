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
import threading
import pika
import pandas
from pandas.io import sql
import pymysql
import json
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
MY_OUTFILE = './cards_db.txt'


app = Flask(__name__)

rbconnection = pika.BlockingConnection(pika.ConnectionParameters(host=MY_HOST))
channel = rbconnection.channel()
channel.exchange_declare(exchange=MY_EXCHANGE, exchange_type='direct')
myqueue_object = channel.queue_declare(queue=MY_QUEUE, durable=True)
channel.queue_bind(exchange=MY_EXCHANGE, queue=MY_QUEUE, routing_key=MY_ROUTINGKEY)

dbconnection = pymysql.connect(
        host=MY_DBHOST, user=MY_DBUSER, password=MY_DBPASSWORD,
        db=MY_DBDATABASE, charset='latin1',
        cursorclass=pymysql.cursors.DictCursor)

def writelog(msg):
    f=open("./log-error.log", "a")
    f.write(msg + "\n")


def cards_consumer_callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)


def moveall_async():
    """Background task to move all cards"""
    with dbconnection.cursor() as cursor:
        sqlquery = "SELECT ExpansionId,Name from magicexpansion"
        cursor.execute(sqlquery)
        for expansion in cursor.fetchall():
            expansion_id = expansion['ExpansionId']
            writelog("moveall_async: expansion_id " + str(expansion_id))
            movecards(expansion_id)
    return


@app.route('/moveall', methods=['GET'])
def moveall():
    writelog("moveall")
    threading.Thread(target=moveall_async).start()
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
            writelog("movecards: not found")
            return content, 404
        else:
            card=cursor.fetchone()
            expansion_name=card['Name']
            writelog("movecards: "+ expansion_name)

        sqlquery = "SELECT * FROM magiccard WHERE ExpansionId = %s"
        amount=cursor.execute(sqlquery, expansion_id)
        writelog ("found " + str(amount) + " results in magiccard")
        listjson=[dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in cursor.fetchall()]
        writelog("listjson: type "+str(type(listjson)))
        for jsonelem in listjson:
            channel.basic_publish(
                exchange=MY_EXCHANGE,
                routing_key=MY_ROUTINGKEY,
                body=json.dumps(jsonelem, ensure_ascii=True),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                ))


    content = expansion_name + ' ' + str(amount)
    return content


@app.route('/card/:<card_id>', methods=['GET'])
def card(card_id):
    content = ''
    return content

def callback_consumer(ch, method, properties, body):
    writelog("Consumer running with body type: [" + str(type(body)) + "] string " + str(body))
#    newbody=body.replace("{u'","{'").replace(", u'",", '").replace(": u'",": '")
#    writelog("Consumer running with newbody type: [" + str(type(newbody)) + "] string " + str(newbody))
    transbody=json.loads(body)
    f = csv.writer(open(MY_OUTFILE, 'a'))
#    for row in jsonbody: # Hopefully only one
    f.writerow(transbody.values())
    #f.close()


def cards_consumer_service():

    writelog("cards_consumer_service, outfile " + MY_OUTFILE)
    f = open(MY_OUTFILE, "w")
    f.truncate()
    f.close()

#    channel.basic_qos(prefetch_count=1)
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


threading.Thread(target=cards_consumer_service).start()
