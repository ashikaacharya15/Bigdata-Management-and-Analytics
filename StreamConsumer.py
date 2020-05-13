import constant, json
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
from textblob import TextBlob


def get_sentiment(polarity):
    if polarity > 0.0:
        sentiment = "positive"
    elif polarity < 0.0:
        sentiment = "negative"
    else:
        sentiment = "neutral"
    return sentiment


def main():
    consumer = KafkaConsumer(constant.TOPIC)
    elastic_search = Elasticsearch()

    for msg in consumer:
        data_dict = json.loads(msg.value)
        text = TextBlob(data_dict['text'])
        user_name = data_dict['user']['name']
        screen_name = data_dict['user']['screen_name']
        location = data_dict['user']['location']
        followers_count = data_dict['user']['followers_count']
        polarity = str(text.sentiment.polarity)
        sentiment = get_sentiment(float(polarity))

        elastic_search.index('twitter_sentiment',
                             body={"name": user_name, "screen_name": screen_name, "location": location,
                                   "followers_count": followers_count, "tweet": str(text), "sentiment": sentiment})

        # elastic_search.index('twitter_sentiment_for_trump',
        #                      body={"name": user_name, "screen_name": screen_name, "location": location,
        #                            "followers_count": followers_count, "tweet": str(text), "sentiment": sentiment})
        print("writing into elastic search")


if __name__ == "__main__":
    main()
