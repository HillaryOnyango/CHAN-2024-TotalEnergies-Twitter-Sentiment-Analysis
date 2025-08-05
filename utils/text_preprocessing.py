import re
import string
from textblob import TextBlob

class TextPreprocessor:
    def __init__(self):
        self.stopwords = set(['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'])
    
    def clean_text(self, text):
        """Clean and preprocess tweet text"""
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        
        # Remove user mentions and hashtags for sentiment analysis
        # (but keep original for hashtag extraction)
        cleaned_text = re.sub(r'@\w+|#\w+', '', text)
        
        # Remove extra whitespace
        cleaned_text = ' '.join(cleaned_text.split())
        
        # Remove punctuation except emoticons
        cleaned_text = re.sub(r'[^\w\s\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]', '', cleaned_text)
        
        return cleaned_text.strip()
    
    def extract_hashtags(self, text):
        """Extract hashtags from text"""
        hashtags = re.findall(r'#\w+', text.lower())
        return hashtags
    
    def extract_mentions(self, text):
        """Extract user mentions from text"""
        mentions = re.findall(r'@\w+', text.lower())
        return mentions
    
    def detect_language(self, text):
        """Detect text language using TextBlob"""
        try:
            blob = TextBlob(text)
            return blob.detect_language()
        except:
            return 'en'  # Default to English
    
    def categorize_country(self, text, hashtags):
        """Determine country based on text content and hashtags with specific CHAN2024 hashtags"""
        text_lower = text.lower()
        hashtags_lower = [h.lower() for h in hashtags]
        all_content = text_lower + ' ' + ' '.join(hashtags_lower)
        
        from config import Config
        
        # Check for country-specific hashtags first (highest priority)
        country_hashtag_scores = {}
        for country, country_hashtags in Config.COUNTRY_HASHTAGS.items():
            # Check for exact hashtag matches
            hashtag_matches = sum(1 for hashtag in country_hashtags if hashtag in hashtags_lower)
            if hashtag_matches > 0:
                country_hashtag_scores[country] = hashtag_matches
        
        # If we have specific hashtag matches, prioritize them
        if country_hashtag_scores:
            # Special logic for shared hashtags
            shared_count = sum(1 for hashtag in hashtags_lower if hashtag in Config.SHARED_HASHTAGS)
            country_specific_count = max(country_hashtag_scores.values())
            
            # If only shared hashtags and no country-specific indicators
            if shared_count > 0 and country_specific_count == shared_count:
                # Check for country-specific keywords to break the tie
                country_keyword_scores = {}
                for country, keywords in Config.COUNTRY_KEYWORDS.items():
                    score = sum(1 for keyword in keywords if keyword in all_content)
                    if score > 0:
                        country_keyword_scores[country] = score
                
                if country_keyword_scores:
                    return max(country_keyword_scores, key=country_keyword_scores.get)
                else:
                    return 'Pamoja'  # Pure shared content
            
            return max(country_hashtag_scores, key=country_hashtag_scores.get)
        
        # Fallback to keyword-based detection
        country_scores = {}
        for country, keywords in Config.COUNTRY_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword in all_content)
            if score > 0:
                country_scores[country] = score
        
        # ChAN Sentimental Analysis

        This project provides utilities for preprocessing and analyzing text data (such as tweets) related to the African Nations Championship (CHAN). It includes tools for cleaning text, extracting hashtags and mentions, detecting language, and categorizing content by country or general CHAN topics.

        ## Features

        - **Text Cleaning:** Remove URLs, mentions, hashtags, and unnecessary punctuation.
        - **Hashtag & Mention Extraction:** Identify hashtags and user mentions in text.
        - **Language Detection:** Automatically detect the language of the input text.
        - **Country Categorization:** Assign tweets to specific countries or general CHAN topics based on hashtags, keywords, and venues.

        ## Installation

        1. Clone the repository:
            ```bash
            git clone https://github.com/yourusername/chan-sentimental-analysis.git
            cd chan-sentimental-analysis
            ```

        2. Install dependencies:
            ```bash
            pip install -r requirements.txt
            ```

        ## Usage

        Import and use the `TextPreprocessor` class in your Python code:

        for country, venues in Config.VENUE_KEYWORDS.items():
            venue_score = sum(2 for venue in venues if venue in all_content)
            if venue_score > 0:
                country_scores[country] = country_scores.get(country, 0) + venue_score
        
        if country_scores:
            return max(country_scores, key=country_scores.get)
        
        # Check if it's general CHAN content
        chan_indicators = ['chan', 'african nations championship', 'caf', 'totalenergies']
        has_chan_content = any(indicator in all_content for indicator in chan_indicators)
        has_shared_hashtag = any(shared in all_content for shared in Config.SHARED_HASHTAGS)
        
        if has_chan_content or has_shared_hashtag:
            return 'CHAN-General'
        
        return 'General'
