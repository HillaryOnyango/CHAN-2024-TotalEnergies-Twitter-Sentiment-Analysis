import logging
import json
import threading
import time
# Change from kafka-python to confluent-kafka
from confluent_kafka import Producer, Consumer, KafkaError

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from config import Config
from db import DatabaseManager
from twitter_client import TwitterClient
from utils.text_preprocessing import TextPreprocessor
from utils.metrics import MetricsCalculator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CHAN2024SentimentAnalyzer:
    def __init__(self):
        # Initialize components
        self.db_manager = DatabaseManager()
        self.twitter_client = TwitterClient()
        self.text_processor = TextPreprocessor()
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        self.metrics_calculator = MetricsCalculator(self.db_manager)
        
        # Kafka setup with confluent-kafka
        producer_config = {
            'bootstrap.servers': ','.join(Config.KAFKA_CONFIG['bootstrap_servers']),
            'client.id': 'chan2024-producer'
        }
        self.producer = Producer(producer_config)
        
        consumer_config = {
            'bootstrap.servers': ','.join(Config.KAFKA_CONFIG['bootstrap_servers']),
            'group.id': 'chan2024-consumer-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([Config.KAFKA_CONFIG['topics']['raw']])
        
        logger.info("CHAN2024 Sentiment Analyzer initialized")
    
    def setup_database(self):
        """Initialize database tables"""
        self.db_manager.create_tables()
        logger.info("Database setup completed")
    
    def analyze_sentiment(self, text):
        """Analyze sentiment using VADER"""
        scores = self.sentiment_analyzer.polarity_scores(text)
        compound = scores['compound']
        
        if compound >= 0.05:
            label = 'positive'
        elif compound <= -0.05:
            label = 'negative'
        else:
            label = 'neutral'
        
        return compound, label
    
    def delivery_report(self, err, msg):
        """Delivery report callback for Kafka producer"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def process_tweet(self, tweet_data):
        """Process individual tweet"""
        try:
            # Clean text
            original_text = tweet_data['text']
            cleaned_text = self.text_processor.clean_text(original_text)
            
            # Extract hashtags and determine country
            hashtags = self.text_processor.extract_hashtags(original_text)
            country = self.text_processor.categorize_country(original_text, hashtags)
            
            # Analyze sentiment
            sentiment_score, sentiment_label = self.analyze_sentiment(cleaned_text)
            
            # Prepare processed data
            processed_tweet = {
                'tweet_id': str(tweet_data['id']),
                'text': original_text,
                'author_id': tweet_data.get('author_id'),
                'created_at': tweet_data.get('created_at'),
                'hashtags': hashtags,
                'country': country,
                'sentiment_score': sentiment_score,
                'sentiment_label': sentiment_label,
                'retweet_count': tweet_data.get('public_metrics', {}).get('retweet_count', 0),
                'like_count': tweet_data.get('public_metrics', {}).get('like_count', 0),
                'reply_count': tweet_data.get('public_metrics', {}).get('reply_count', 0)
            }
            
            # Store in database
            success = self.db_manager.insert_tweet(processed_tweet)
            
            if success:
                # Send to appropriate Kafka topics
                if country in ['Kenya', 'Uganda', 'Tanzania']:
                    topic = Config.KAFKA_CONFIG['topics'][country.lower()]
                    self.producer.produce(
                        topic, 
                        value=json.dumps(processed_tweet).encode('utf-8'),
                        callback=self.delivery_report
                    )
                elif country in ['Pamoja', 'CHAN-General']:
                    # Send Pamoja content to all country topics since it's shared
                    for host_country in ['kenya', 'uganda', 'tanzania']:
                        topic = Config.KAFKA_CONFIG['topics'][host_country]
                        processed_tweet_copy = processed_tweet.copy()
                        processed_tweet_copy['shared_for_country'] = host_country.title()
                        self.producer.produce(
                            topic, 
                            value=json.dumps(processed_tweet_copy).encode('utf-8'),
                            callback=self.delivery_report
                        )
                
                # Flush to ensure message delivery
                self.producer.flush()
                logger.info(f"Processed tweet from {country}: {sentiment_label} sentiment")
            
            return processed_tweet
            
        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
            return None
    
    def fetch_and_produce_tweets(self):
        """Fetch tweets using the specific CHAN 2024 hashtags and send to Kafka"""
        try:
            # Create search queries for each country's specific hashtags
            kenya_query = "(#CHAN2024 OR #TotalEnergiesCHAN2024 OR #HarambeeStars) -is:retweet lang:en"
            uganda_query = "(#TotalEnergiesCHAN2024 OR #CHAN2024 OR #UgandaCranes) -is:retweet lang:en"  
            tanzania_query = "(#CHAN2024 OR #TotalEnergiesCHAN2024 OR #TaifaStars) -is:retweet lang:en"
            
            queries = [
                ("Kenya-focused", kenya_query),
                ("Uganda-focused", uganda_query), 
                ("Tanzania-focused", tanzania_query)
            ]
            
            all_tweets = []
            
            for query_name, query in queries:
                tweets = self.twitter_client.search_tweets(query, max_results=50)
                logger.info(f"Fetched {len(tweets)} tweets for {query_name}")
                all_tweets.extend(tweets)
            
            # Remove duplicates based on tweet ID
            unique_tweets = {}
            for tweet in all_tweets:
                unique_tweets[tweet['id']] = tweet
            
            final_tweets = list(unique_tweets.values())
            
            # Send to Kafka
            for tweet in final_tweets:
                self.producer.produce(
                    Config.KAFKA_CONFIG['topics']['raw'], 
                    value=json.dumps(tweet).encode('utf-8'),
                    callback=self.delivery_report
                )
            
            # Flush to ensure all messages are sent
            self.producer.flush()
            logger.info(f"Sent {len(final_tweets)} unique tweets to Kafka")
            return len(final_tweets)
            
        except Exception as e:
            logger.error(f"Error fetching tweets: {e}")
            return 0
    
    def consume_and_process_tweets(self):
        """Consume tweets from Kafka and process them"""
        logger.info("Starting tweet consumer...")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.info(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Process the message
                try:
                    tweet_data = json.loads(msg.value().decode('utf-8'))
                    processed_tweet = self.process_tweet(tweet_data)
                    
                    if processed_tweet:
                        # Send processed tweet to another topic if needed
                        self.producer.produce(
                            Config.KAFKA_CONFIG['topics']['processed'], 
                            value=json.dumps(processed_tweet).encode('utf-8'),
                            callback=self.delivery_report
                        )
                        self.producer.flush()
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.consumer.close()
    
    def generate_analytics_report(self):
        """Generate comprehensive analytics report with country rankings"""
        print("\n" + "="*70)
        print("🏆 CHAN 2024 TOTALENERGIES COUNTRY TWEET COMPARISON REPORT")
        print("="*70)
        
        # Get country rankings
        rankings = self.metrics_calculator.get_country_rankings(days=7)
        
        if not rankings:
            print("❌ No data available for analysis")
            return
        
        # Display overall winner
        winner = min(rankings.items(), key=lambda x: x[1]['rank'])
        print(f"\n🥇 TWEET VOLUME CHAMPION: {winner[0].upper()}")
        print(f"   Total Tweets: {winner[1]['tweet_count']}")
        print(f"   Average Engagement: {winner[1]['avg_engagement']}")
        print(f"   Sentiment Score: {winner[1]['avg_sentiment']}")
        
        print(f"\n📊 COMPLETE COUNTRY RANKINGS:")
        print("-" * 70)
        
        for country, data in sorted(rankings.items(), key=lambda x: x[1]['rank']):
            rank_emoji = "🥇" if data['rank'] == 1 else "🥈" if data['rank'] == 2 else "🥉"
            
            print(f"{rank_emoji} #{data['rank']} {country.upper()}")
            print(f"   📱 Total Tweets: {data['tweet_count']}")
            print(f"   💭 Avg Sentiment: {data['avg_sentiment']} ")
            print(f"   😊 Positive: {data['positive_percentage']}% ({data['positive_tweets']} tweets)")
            print(f"   😞 Negative: {data['negative_percentage']}% ({data['negative_tweets']} tweets)")
            print(f"   😐 Neutral: {data['neutral_percentage']}% ({data['neutral_tweets']} tweets)")
            print(f"   🔥 Avg Engagement: {data['avg_engagement']}")
            print()
        
        # Hashtag winners
        hashtag_winners = self.metrics_calculator.get_hashtag_winners(days=7)
        
        if hashtag_winners:
            print(f"🏷️ HASHTAG PERFORMANCE LEADERS:")
            print("-" * 70)
            
            for hashtag, data in hashtag_winners.items():
                if hashtag.lower() in ['#chan2024', '#totalenergieschan2024', '#harambeestars', '#ugandacranes', '#taifastars']:
                    print(f"   {hashtag}: 👑 {data['winner']} ({data['usage_count']} tweets)")
                    print(f"      Sentiment: {data['avg_sentiment']} | Engagement: {data['avg_engagement']}")
                    
                    # Show competition
                    competitors = [f"{country}: {count}" for country, count in data['competition']]
                    print(f"      Competition: {' | '.join(competitors)}")
                    print()
        
        # Summary insights
        total_tweets = sum(data['tweet_count'] for data in rankings.values())
        avg_sentiment_all = sum(data['avg_sentiment'] * data['tweet_count'] for data in rankings.values()) / total_tweets
        
        print(f"📈 SUMMARY INSIGHTS:")
        print("-" * 70)
        print(f"   🌍 Total Tweets Analyzed: {total_tweets}")
        print(f"   📊 Overall Sentiment: {avg_sentiment_all:.3f}")
        print(f"   🏆 Most Active Country: {winner[0]}")
        
        # Determine most positive country
        most_positive = max(rankings.items(), key=lambda x: x[1]['positive_percentage'])
        print(f"   😊 Most Positive Country: {most_positive[0]} ({most_positive[1]['positive_percentage']}% positive)")
        
        # Determine highest engagement
        most_engaging = max(rankings.items(), key=lambda x: x[1]['avg_engagement'])
        print(f"   🔥 Highest Engagement: {most_engaging[0]} (avg: {most_engaging[1]['avg_engagement']})")
        
        print("\n" + "="*70)
        print("🤝 Note: All three countries are united in hosting CHAN 2024 Pamoja!")
        print("="*70)
    
    def run_batch_analysis(self):
        """Run batch analysis (one-time fetch and process)"""
        logger.info("Starting batch analysis...")
        
        # Setup database
        self.setup_database()
        
        # Fetch and process tweets
        self.fetch_and_produce_tweets()
        
        # Process tweets from Kafka (with timeout)
        start_time = time.time()
        timeout = 60  # 1 minute timeout
        
        while time.time() - start_time < timeout:
            msg = self.consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break
            
            try:
                tweet_data = json.loads(msg.value().decode('utf-8'))
                self.process_tweet(tweet_data)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding message: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
        
        # Generate report
        self.generate_analytics_report()
    
    def run_real_time_analysis(self):
        """Run real-time analysis with continuous streaming"""
        logger.info("Starting real-time analysis...")
        
        # Setup database
        self.setup_database()
        
        # Start producer thread
        producer_thread = threading.Thread(target=self.continuous_tweet_fetching)
        producer_thread.daemon = True
        producer_thread.start()
        
        # Start consumer thread
        consumer_thread = threading.Thread(target=self.consume_and_process_tweets)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Generate periodic reports
        try:
            while True:
                time.sleep(300)  # Generate report every 5 minutes
                self.generate_analytics_report()
        except KeyboardInterrupt:
            logger.info("Stopping real-time analysis...")
            self.consumer.close()
    
    def continuous_tweet_fetching(self):
        """Continuously fetch tweets"""
        while True:
            try:
                self.fetch_and_produce_tweets()
                time.sleep(60)  # Fetch every minute
            except Exception as e:
                logger.error(f"Error in continuous fetching: {e}")
                time.sleep(60)

def main():
    """Main function to run the sentiment analyzer"""
    analyzer = CHAN2024SentimentAnalyzer()
    
    # You can choose to run batch or real-time analysis
    # analyzer.run_batch_analysis()  # For one-time analysis
    analyzer.run_real_time_analysis()  # For continuous analysis

if __name__ == "__main__":
    main()