import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

class MetricsCalculator:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def calculate_sentiment_distribution(self, country=None, days=7):
        """Calculate sentiment distribution for a country"""
        date_from = datetime.now() - timedelta(days=days)
        data = self.db.get_analytics_data(country=country, date_from=date_from)
        
        if not data:
            return {}
        
        df = pd.DataFrame(data)
        
        total_tweets = df['tweet_count'].sum()
        total_positive = df['positive_tweets'].sum()
        total_negative = df['negative_tweets'].sum()
        total_neutral = df['neutral_tweets'].sum()
        
        return {
            'total_tweets': int(total_tweets),
            'positive_percentage': round((total_positive / total_tweets) * 100, 2),
            'negative_percentage': round((total_negative / total_tweets) * 100, 2),
            'neutral_percentage': round((total_neutral / total_tweets) * 100, 2),
            'avg_sentiment': round(df['avg_sentiment'].mean(), 3)
        }
    
    def compare_countries(self, days=7):
        """Compare sentiment metrics across countries including shared Pamoja content"""
        countries = ['Kenya', 'Uganda', 'Tanzania', 'Pamoja']
        comparison = {}
        
        for country in countries:
            metrics = self.calculate_sentiment_distribution(country, days)
            comparison[country] = metrics
        
        # Add combined metrics for all host countries
        all_hosts_metrics = self.calculate_sentiment_distribution('AllHosts', days)
        comparison['All_Host_Countries'] = all_hosts_metrics
        
        return comparison
    
    def get_country_rankings(self, days=7):
        """Get country rankings based on tweet volume and engagement"""
        country_comparison = self.db.get_country_tweet_comparison(days)
        
        if not country_comparison:
            return {}
        
        # Sort by tweet count (descending)
        ranked_countries = sorted(country_comparison, key=lambda x: x['tweet_count'], reverse=True)
        
        rankings = {}
        for i, country_data in enumerate(ranked_countries, 1):
            rankings[country_data['country']] = {
                'rank': i,
                'tweet_count': country_data['tweet_count'],
                'avg_sentiment': round(float(country_data['avg_sentiment']), 3),
                'positive_tweets': country_data['positive_tweets'],
                'negative_tweets': country_data['negative_tweets'],
                'neutral_tweets': country_data['neutral_tweets'],
                'avg_engagement': round(float(country_data['avg_engagement']), 2),
                'positive_percentage': round((country_data['positive_tweets'] / country_data['tweet_count']) * 100, 2),
                'negative_percentage': round((country_data['negative_tweets'] / country_data['tweet_count']) * 100, 2),
                'neutral_percentage': round((country_data['neutral_tweets'] / country_data['tweet_count']) * 100, 2)
            }
        
        return rankings
    
    def get_hashtag_winners(self, days=7):
        """Determine which country wins for each specific hashtag"""
        hashtag_data = self.db.get_hashtag_performance_by_country(days)
        
        if not hashtag_data:
            return {}
        
        # Group by hashtag
        hashtag_winners = {}
        hashtag_groups = {}
        
        for row in hashtag_data:
            hashtag = row['hashtag']
            if hashtag not in hashtag_groups:
                hashtag_groups[hashtag] = []
            hashtag_groups[hashtag].append(row)
        
        # Find winner for each hashtag
        for hashtag, countries in hashtag_groups.items():
            if len(countries) > 1:  # Only if multiple countries use the hashtag
                winner = max(countries, key=lambda x: x['usage_count'])
                hashtag_winners[hashtag] = {
                    'winner': winner['country'],
                    'usage_count': winner['usage_count'],
                    'avg_sentiment': round(float(winner['avg_sentiment']), 3),
                    'avg_engagement': round(float(winner['avg_engagement']), 2),
                    'competition': [(c['country'], c['usage_count']) for c in countries]
                }
        
        return hashtag_winners
    
    def generate_trend_data(self, days=7):
        """Generate trend data for visualization"""
        date_from = datetime.now() - timedelta(days=days)
        data = self.db.get_analytics_data(date_from=date_from)
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def calculate_engagement_metrics(self):
        """Calculate engagement metrics by country"""
        # This would require additional database queries
        # Implementation depends on specific engagement metrics needed
        pass
    
    def export_analytics_report(self, filepath, days=7):
        """Export analytics to CSV/Excel"""
        df = self.generate_trend_data(days)
        if not df.empty:
            df.to_csv(filepath, index=False)
            return True
        return False