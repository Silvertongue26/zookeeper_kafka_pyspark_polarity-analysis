# Import libraries
import auth_tokens as auth
import tweepy
import logging

from kafka import KafkaProducer

# Generate Kafka producer/ localhost and 9092 default ports
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],  api_version=(2, 0, 2))

# Search name for twitter
search_term = 'elon musk'

# Topic name for Kafka tracing
topic_name = 'TW_ANALYSIS'

def twitterAuth():
    # Create Twitter API authentication object
    authenticate = tweepy.OAuthHandler(auth.consumer_key, auth.consumer_secret)
    # Access information for Twitter API
    authenticate.set_access_token(auth.access_token, auth.access_secret)
    # Api object creation
    api = tweepy.API(authenticate, wait_on_rate_limit=True)

    return api

class TweetListener(tweepy.Stream):

    def on_data(self, raw_data):
        # Log data to TW_DEBUG.log
        logging.info(raw_data)

        # Send to our producer
        producer.send(topic_name, value=raw_data)

        return True

    def on_error(self, status_code):
        # Error if disconnect
        if status_code == 420:
            return False

    def start_streaming_tweets(self, search_term):
        # Start catching tweets from twitter, delete '[' and ']' for general search
        self.filter(track=[search_term], languages=["en"])


if __name__ == '__main__':
    # Creat loggong instance
    logging.basicConfig(filename='TW_DEBUG.log', encoding='UTF-8', level=logging.DEBUG)

    # TWitter API usage
    twitter_stream = TweetListener(auth.consumer_key, auth.consumer_secret, auth.access_token, auth.access_secret)
    twitter_stream.start_streaming_tweets(search_term)
