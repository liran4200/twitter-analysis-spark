from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

KAFKA_TOPIC = 'teststream2'
KAFKA_SERVER = 'localhost:9092'

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)

    # creating a streaming context with batch interval of 10 sec
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("./Dataset/positive.txt")
    nwords = load_wordlist("./Dataset/negative.txt")
    sentiments_counts = proccess_stream(ssc, pwords, nwords, 100)
    make_plot(sentiments_counts)


def make_plot(counts):
    """
    This function plots the counts of positive and negative words for each timestep.
    """
    positiveCounts = []
    negativeCounts = []
    time = []

    for val in counts:
        positiveTuple = val[0]
        positiveCounts.append(positiveTuple[1])
        negativeTuple = val[1]
        negativeCounts.append(negativeTuple[1])

    for i in range(len(counts)):
        time.append(i)
        
    posLine = plt.plot(time, positiveCounts,'bo-', label='Positive')
    negLine = plt.plot(time, negativeCounts,'go-', label='Negative')
    plt.axis([0, len(counts), 0, max(max(positiveCounts), max(negativeCounts))+50])
    plt.xlabel('Time step')
    plt.ylabel('Sentiments counts')
    plt.legend(loc = 'upper left')
    plt.show()

	
def load_wordlist(filename):
    """ 
    This function returns a list or set of words from the given filename.
    """	
    words = {}
    f = open(filename, 'rU')
    text = f.read()
    text = text.split('\n')
    for line in text:
        words[line] = 1
    f.close()
    return words


def wordSentiment(word,pwords,nwords):
    if word in pwords:
        return ('positive', 1) 
    
    if word in nwords:
        return ('negative', 1)


def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount) 


def sendRecord(record):
    connection = createNewConnection()
    connection.send(record)
    connection.close()


def proccess_stream(ssc, pwords, nwords, duration):
    # create kafka direct stream 
    kstream = KafkaUtils.createDirectStream(
    ssc, topics = [KAFKA_TOPIC], kafkaParams = {"metadata.broker.list": KAFKA_SERVER})

    # get tweets to list from tuple such as (None, tweet_string)
    tweets = kstream.map(lambda x: x[1])
    tweets.pprint()

    # each element of tweets will be the text of a tweet.
    words = tweets.flatMap(lambda line:line.split(" "))

    # classfied tweets by dataset
    positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))
    negative = words.map(lambda word: ('Negative', 1) if word in nwords else ('Negative', 0))
    sentiments = positive.union(negative)

    # calc sentiments counts of all the tweets
    sentiments_counts = sentiments.reduceByKey(lambda x,y: x+y)

    # update current step    
    current_sentiment_counts = sentiments_counts.updateStateByKey(updateFunction)
    current_sentiment_counts.pprint()
    
    # the counts variable have the word counts for all time steps
    counts = []
    sentiments_counts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    
    # start the computation
    ssc.start() 

    # stop computation by duration 
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully = True)

    return counts


if __name__=="__main__":
    main()
