import json
from kafka import SimpleProducer, KafkaClient, KafkaProducer
import tweepy
import configparser
import os
import json
import yaml



class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twitter stream and push it to Kafka"""
   
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"])


    def on_data(self, data):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        data_json = json.loads(data)
        str_tweet = data_json['text'].encode('utf-8')
        print(str_tweet)
        try:
            self.producer.send('twitterstream', str_tweet)
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        print(f"Error received in kafka producer {status_code}")
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

if __name__ == '__main__':

    # read credentials for twitter app
    with open(f"credentials_config.yml", 'r') as file:
        try:
            creds = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            raise exc

    # Create Auth object
    auth = tweepy.OAuthHandler(creds['consumerKey'], creds['consumerSecret'])
    auth.set_access_token(creds['accessToken'], creds['accessTokenSecret'])
    api = tweepy.API(auth)

    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener=TweeterStreamListener(api))

    #Custom Filter rules pull all traffic for those filters in real time.
    #stream.filter(track = ['love', 'hate'], languages = ['en'])
    stream.filter(track=['python','java','scala'])