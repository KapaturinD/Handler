#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pika, sys, os
import json
import psycopg2
from psycopg2 import Error


def rmq():
    credentials = pika.PlainCredentials(username='admin', password='7NkFGG409M')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.0.89.14', port=5672, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='puid', durable=True)

    def callback(ch, method, properties, body):
        data = json.loads(body)
        #Сonnected to PostgreSQL

        connection = psycopg2.connect(user="mydbuser",
                                      password="123456789",
                                      host="10.0.89.31",
                                      port="5432",
                                      database="journals")

        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print(" [x] Received %r" % data, "\n", "[x] connected to database", record, "\n")

    channel.basic_consume(queue='puid', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        rmq()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
