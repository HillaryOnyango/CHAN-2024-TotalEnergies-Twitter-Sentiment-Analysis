import tweepy
from config import Config
import logging
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TwitterClient:
    def __init__(self):
        self.client = None
        self.setup_client()
    
    def setup_client(self):
        """Initialize Twitter API client"""
        try:
            self.client = tweepy.Client(
                bearer_token=Config.TWITTER_BEARER_TOKEN,
                consumer_key=Config.TWITTER_API_KEY,
                consumer_secret=Config.TWITTER_API_SECRET,
                access_token=Config.TWITTER_ACCESS_TOKEN,
                access_token_secret=Config.TWITTER_ACCESS_TOKEN_SECRET,
                wait_on_rate_limit=True
            )
            logger.info("Twitter client initialized successfully")
        except Exception as e:
            logger.error(f"Twitter client initialization failed: {e}")
            raise
    
    def search_tweets(self, query, max_results=100):
        """Search for tweets using Twitter API v2"""
        try:
            tweets = tweepy.Paginator(
                self.client.search_recent_tweets,
                query=query,
                tweet_fields=['created_at', 'author_id', 'public_metrics', 'context_annotations'],
                max_results=max_results
            ).flatten(limit=max_results)
            
            tweet_data = []
            for tweet in tweets:
                tweet_dict = {
                    'id': tweet.id,
                    'text': tweet.text,
                    'author_id': tweet.author_id,
                    'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
                    'public_metrics': tweet.public_metrics,
                    'hashtags': self.extract_hashtags(tweet.text)
                }
                tweet_data.append(tweet_dict)
            
            logger.info(f"Retrieved {len(tweet_data)} tweets")
            return tweet_data
            
        except Exception as e:
            logger.error(f"Error searching tweets: {e}")
            return []
    
    def extract_hashtags(self, text):
        """Extract hashtags from tweet text"""
        import re
        hashtags = re.findall(r'#\w+', text.lower())
        return hashtags
    
    def stream_tweets(self, callback_function):
        """Stream tweets in real-time"""
        class TweetStreamListener(tweepy.StreamingClient):
            def __init__(self, bearer_token, callback):
                super().__init__(bearer_token)
                self.callback = callback
            
            def on_tweet(self, tweet):
                tweet_data = {
                    'id': tweet.id,
                    'text': tweet.text,
                    'author_id': tweet.author_id,
                    'created_at': datetime.now().isoformat(),
                    'hashtags': TwitterClient.extract_hashtags(None, tweet.text)
                }
                self.callback(tweet_data)
        
        try:
            stream = TweetStreamListener(Config.TWITTER_BEARER_TOKEN, callback_function)
            
            # Add rules for hashtags
            for hashtag in Config.HASHTAGS:
                stream.add_rules(tweepy.StreamRule(hashtag))
            
            logger.info("Starting tweet stream...")
            stream.filter()
            
        except Exception as e:
            logger.error(f"Streaming error: {e}")