import json
from kafka import SimpleProducer, KafkaClient, KafkaProducer
import tweepy
import configparser
import os
import json
import yaml

import logging
logging.basicConfig(level=logging.INFO)

KAFKA_TOPIC = 'teststream2'
KAFKA_SERVER = 'localhost:9092'

class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twitter stream - tweets and publish to Kafka"""
   
    producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda x: 
            x.encode('utf-8')
    )
  
    def on_data(self, data):
        """ This method is trigger whenever new data arrives from real-time stream.
        We asynchronously push this data to kafka queue"""
        data_json = json.loads(data)

        # handle tweets arrived without 'text' property
        if data_json.get('text') is None:
            return True
        str_tweet = data_json['text'] 
        logging.info(f"------- -- {str_tweet}") # DEBUG
        try:
            self.producer.send(KAFKA_TOPIC, value=str_tweet)
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print(f"Error ocurred on: kafka producer {status_code}")
        return True # keep the stream alive

    def on_timeout(self):
        print(f"Error ocurred, caused by timeout error in producer {status_code}")
        return True # keep the stream alive

if __name__ == '__main__':

    # read credentials for twitter app
    with open(f"credentials_config.yml", 'r') as file:
        try:
            creds = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            raise exc

    # generate auth  twitter object
    auth = tweepy.OAuthHandler(creds['consumerKey'], creds['consumerSecret'])
    auth.set_access_token(creds['accessToken'], creds['accessTokenSecret'])
    api = tweepy.API(auth)

    # create stream, bind the listener
    stream = tweepy.Stream(auth, listener=TweeterStreamListener(api))

    # custom filter rules pull all traffic for those filters in real time.
    stream.filter(track = ['love', 'hate'])