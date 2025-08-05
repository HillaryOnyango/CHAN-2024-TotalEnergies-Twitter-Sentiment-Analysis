import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Twitter API Configuration
    TWITTER_BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN')
    TWITTER_API_KEY = os.getenv('TWITTER_API_KEY')
    TWITTER_API_SECRET = os.getenv('TWITTER_API_SECRET')
    TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
    TWITTER_ACCESS_TOKEN_SECRET = os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
    
    # Database Configuration
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'chan2024_analytics'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    # Kafka Configuration
    KAFKA_CONFIG = {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(','),
        'topics': {
            'raw': os.getenv('KAFKA_TOPIC_RAW', 'chan2024-tweets-raw'),
            'processed': os.getenv('KAFKA_TOPIC_PROCESSED', 'chan2024-tweets-processed'),
            'kenya': 'kenya-hashtags',
            'uganda': 'uganda-hashtags',
            'tanzania': 'tanzania-hashtags'
        }
    }
    
    # CHAN 2024 Configuration - Updated with specific hashtags
    HASHTAGS = [
        "#CHAN2024", "#TotalEnergiesCHAN2024", "#Pamoja",
        "#HarambeeStars", "#UgandaCranes", "#TaifaStars",
        "#Kenya", "#Uganda", "#Tanzania",
        "#TeamKenya", "#TeamUganda", "#TeamTanzania",
        "#EastAfrica", "#EAC"
    ]
    
    # Country-specific hashtags mapping
    COUNTRY_HASHTAGS = {
        'Kenya': ["#chan2024", "#totalenergieschan2024", "#harambeestars"],
        'Uganda': ["#totalenergieschan2024", "#chan2024", "#ugandacranes"],
        'Tanzania': ["#chan2024", "#totalenergieschan2024", "#taifastars"]
    }
    
    # Shared hashtags for all host countries
    SHARED_HASHTAGS = ["#pamoja", "#chan2024", "#totalenergieschan2024", "#eastafrica", "#eac"]
    
    COUNTRY_KEYWORDS = {
        'Kenya': ['kenya', 'harambee', 'stars', 'nairobi', 'teamkenya', 'ke', 'harambeefc', 'harambeestars'],
        'Uganda': ['uganda', 'cranes', 'kampala', 'teamuganda', 'ug', 'thecranes', 'ugandacranes'],
        'Tanzania': ['tanzania', 'taifa', 'stars', 'dar', 'teamtanzania', 'tz', 'taifastars', 'simba']
    }
    
    # City and venue keywords for better detection
    VENUE_KEYWORDS = {
        'Kenya': ['nairobi', 'kasarani', 'nyayo', 'moi'],
        'Uganda': ['kampala', 'nakivubo', 'mandela', 'namboole'],
        'Tanzania': ['dar es salaam', 'mkapa', 'uhuru', 'azam']
    }