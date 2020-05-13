import constant
import tweepy
from kafka import KafkaProducer
from tweepy import Stream
from tweepy.streaming import StreamListener


auth = tweepy.OAuthHandler(constant.CONSUMER_KEY, constant.CONSUMER_SECRET)
auth.set_access_token(constant.ACCESS_TOKEN, constant.ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)


class TwitterStreamProducer(StreamListener):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def on_data(self, data):
        self.producer.send(constant.TOPIC, data.encode('utf-8'))
        print(data)
        print("========================================================")
        return True

    def on_error(self, status):
        print(status)
        return True


twitter_stream = Stream(auth, TwitterStreamProducer())

twitter_stream.filter(track=['#coronavirus'])
#twitter_stream.filter(track=['#trump'])
