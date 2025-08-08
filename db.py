import psycopg2
from psycopg2.extras import RealDictCursor
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**Config.DB_CONFIG)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def create_tables(self):
        """Create necessary tables"""
        create_tables_sql = """
        -- Main tweets table
        CREATE TABLE IF NOT EXISTS tweets (
            id SERIAL PRIMARY KEY,
            tweet_id VARCHAR(50) UNIQUE NOT NULL,
            text TEXT NOT NULL,
            author_id VARCHAR(50),
            created_at TIMESTAMP,
            hashtags TEXT[],
            country VARCHAR(20),
            sentiment_score DECIMAL(3,2),
            sentiment_label VARCHAR(20),
            retweet_count INTEGER DEFAULT 0,
            like_count INTEGER DEFAULT 0,
            reply_count INTEGER DEFAULT 0,
            is_shared_content BOOLEAN DEFAULT FALSE,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Hashtag analytics table
        CREATE TABLE IF NOT EXISTS hashtag_analytics (
            id SERIAL PRIMARY KEY,
            hashtag VARCHAR(100),
            country VARCHAR(20),
            tweet_count INTEGER,
            avg_sentiment DECIMAL(3,2),
            positive_count INTEGER,
            negative_count INTEGER,
            neutral_count INTEGER,
            is_shared_hashtag BOOLEAN DEFAULT FALSE,
            analysis_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Country comparison table (now includes Pamoja category)
        CREATE TABLE IF NOT EXISTS country_metrics (
            id SERIAL PRIMARY KEY,
            country VARCHAR(20),
            total_tweets INTEGER,
            avg_sentiment DECIMAL(3,2),
            engagement_rate DECIMAL(5,2),
            top_hashtags TEXT[],
            shared_content_count INTEGER DEFAULT 0,
            analysis_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Pamoja (shared) content tracking
        CREATE TABLE IF NOT EXISTS pamoja_metrics (
            id SERIAL PRIMARY KEY,
            hashtag VARCHAR(100),
            tweet_count INTEGER,
            avg_sentiment DECIMAL(3,2),
            countries_mentioned TEXT[],
            analysis_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_tweets_country ON tweets(country);
        CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at);
        CREATE INDEX IF NOT EXISTS idx_tweets_hashtags ON tweets USING GIN(hashtags);
        CREATE INDEX IF NOT EXISTS idx_tweets_shared ON tweets(is_shared_content);
        CREATE INDEX IF NOT EXISTS idx_hashtag_analytics_date ON hashtag_analytics(analysis_date);
        CREATE INDEX IF NOT EXISTS idx_hashtag_shared ON hashtag_analytics(is_shared_hashtag);
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_tables_sql)
            self.connection.commit()
            cursor.close()
            logger.info("Tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            self.connection.rollback()
    
    def insert_tweet(self, tweet_data):
        """Insert processed tweet into database"""
        try:
            cursor = self.connection.cursor()
            
            insert_query = """
            INSERT INTO tweets (tweet_id, text, author_id, created_at, hashtags, 
                              country, sentiment_score, sentiment_label, 
                              retweet_count, like_count, reply_count, is_shared_content)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tweet_id) DO NOTHING
            """
            
            # Check if it's shared content (Pamoja category)
            is_shared = tweet_data['country'] in ['Pamoja', 'CHAN-General']
            
            cursor.execute(insert_query, (
                tweet_data['tweet_id'],
                tweet_data['text'],
                tweet_data.get('author_id'),
                tweet_data.get('created_at'),
                tweet_data.get('hashtags', []),
                tweet_data['country'],
                tweet_data['sentiment_score'],
                tweet_data['sentiment_label'],
                tweet_data.get('retweet_count', 0),
                tweet_data.get('like_count', 0),
                tweet_data.get('reply_count', 0),
                is_shared
            ))
            
            self.connection.commit()
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"Database insert error: {e}")
            self.connection.rollback()
            return False
    
    def get_analytics_data(self, country=None, date_from=None, date_to=None, include_shared=True):
        """Retrieve analytics data with Pamoja content consideration"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            query = """
            SELECT 
                country,
                DATE(created_at) as date,
                COUNT(*) as tweet_count,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(CASE WHEN sentiment_label = 'positive' THEN 1 END) as positive_tweets,
                COUNT(CASE WHEN sentiment_label = 'negative' THEN 1 END) as negative_tweets,
                COUNT(CASE WHEN sentiment_label = 'neutral' THEN 1 END) as neutral_tweets,
                COUNT(CASE WHEN is_shared_content = true THEN 1 END) as shared_content_count
            FROM tweets 
            WHERE 1=1
            """
            
            params = []
            
            if country:
                if country == 'AllHosts':
                    # Include Kenya, Uganda, Tanzania, and their shared content
                    query += " AND (country IN ('Kenya', 'Uganda', 'Tanzania', 'Pamoja', 'CHAN-General'))"
                else:
                    query += " AND country = %s"
                    params.append(country)
            
            if not include_shared:
                query += " AND is_shared_content = false"
            
            if date_from:
                query += " AND created_at >= %s"
                params.append(date_from)
                
            if date_to:
                query += " AND created_at <= %s"
                params.append(date_to)
            
            query += " GROUP BY country, DATE(created_at) ORDER BY date DESC, tweet_count DESC"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except Exception as e:
            logger.error(f"Analytics query error: {e}")
            return []
    
    def get_country_tweet_comparison(self, days=7):
        """Get tweet count comparison between countries"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            date_from = datetime.now() - timedelta(days=days)
            
            query = """
            SELECT 
                country,
                COUNT(*) as tweet_count,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(CASE WHEN sentiment_label = 'positive' THEN 1 END) as positive_tweets,
                COUNT(CASE WHEN sentiment_label = 'negative' THEN 1 END) as negative_tweets,
                COUNT(CASE WHEN sentiment_label = 'neutral' THEN 1 END) as neutral_tweets,
                AVG(retweet_count + like_count + reply_count) as avg_engagement
            FROM tweets 
            WHERE country IN ('Kenya', 'Uganda', 'Tanzania')
            AND created_at >= %s
            GROUP BY country
            ORDER BY tweet_count DESC
            """
            
            cursor.execute(query, (date_from,))
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except Exception as e:
            logger.error(f"Country comparison query error: {e}")
            return []
    
    def get_hashtag_performance_by_country(self, days=7):
        """Get hashtag performance breakdown by country"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            date_from = datetime.now() - timedelta(days=days)
            
            query = """
            SELECT 
                country,
                unnest(hashtags) as hashtag,
                COUNT(*) as usage_count,
                AVG(sentiment_score) as avg_sentiment,
                AVG(retweet_count + like_count + reply_count) as avg_engagement
            FROM tweets 
            WHERE country IN ('Kenya', 'Uganda', 'Tanzania')
            AND created_at >= %s
            AND hashtags IS NOT NULL
            GROUP BY country, hashtag
            HAVING COUNT(*) > 2
            ORDER BY country, usage_count DESC
            """
            
            cursor.execute(query, (date_from,))
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except Exception as e:
            logger.error(f"Hashtag performance query error: {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")