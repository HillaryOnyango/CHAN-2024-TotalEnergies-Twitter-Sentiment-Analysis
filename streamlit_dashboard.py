import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import time
from streamlit_autorefresh import st_autorefresh

from config import Config
from db import DatabaseManager
from utils.metrics import MetricsCalculator

# Page configuration
st.set_page_config(
    page_title="CHAN 2024 TotalEnergies Twitter Analytics",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="expanded"
)

class CHAN2024Dashboard:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.metrics_calculator = MetricsCalculator(self.db_manager)
        
    def load_data(self, days=7):
        """Load data for dashboard"""
        try:
            # Get country rankings
            rankings = self.metrics_calculator.get_country_rankings(days)
            
            # Get hashtag performance
            hashtag_winners = self.metrics_calculator.get_hashtag_winners(days)
            
            # Get trend data
            trend_data = self.metrics_calculator.generate_trend_data(days)
            
            return rankings, hashtag_winners, trend_data
            
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return {}, {}, pd.DataFrame()
    
    def create_country_comparison_chart(self, rankings):
        """Create country comparison bar chart"""
        if not rankings:
            return None
            
        countries = list(rankings.keys())
        tweet_counts = [data['tweet_count'] for data in rankings.values()]
        sentiments = [data['avg_sentiment'] for data in rankings.values()]
        
        # Create subplot with secondary y-axis
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add tweet count bars
        fig.add_trace(
            go.Bar(
                x=countries,
                y=tweet_counts,
                name="Tweet Count",
                marker_color=['#FF6B6B', '#4ECDC4', '#45B7D1'],
                text=tweet_counts,
                textposition='outside'
            ),
            secondary_y=False,
        )
        
        # Add sentiment line
        fig.add_trace(
            go.Scatter(
                x=countries,
                y=sentiments,
                mode='lines+markers',
                name="Avg Sentiment",
                line=dict(color='orange', width=3),
                marker=dict(size=10)
            ),
            secondary_y=True,
        )
        
        # Update layout
        fig.update_xaxes(title_text="Countries")
        fig.update_yaxes(title_text="Number of Tweets", secondary_y=False)
        fig.update_yaxes(title_text="Average Sentiment Score", secondary_y=True)
        
        fig.update_layout(
            title="🏆 Country Tweet Volume & Sentiment Comparison",
            height=500,
            showlegend=True
        )
        
        return fig
    
    def create_sentiment_distribution_pie(self, rankings):
        """Create sentiment distribution pie charts"""
        if not rankings:
            return None
            
        fig = make_subplots(
            rows=1, cols=3,
            specs=[[{'type':'domain'}, {'type':'domain'}, {'type':'domain'}]],
            subplot_titles=list(rankings.keys())
        )
        
        colors = ['#FF9999', '#66B2FF', '#99FF99']  # Red, Blue, Green
        
        for i, (country, data) in enumerate(rankings.items()):
            fig.add_trace(
                go.Pie(
                    labels=['Positive', 'Negative', 'Neutral'],
                    values=[data['positive_tweets'], data['negative_tweets'], data['neutral_tweets']],
                    name=country,
                    marker_colors=['#4CAF50', '#F44336', '#FF9800']
                ),
                row=1, col=i+1
            )
        
        fig.update_layout(
            title_text="😊 Sentiment Distribution by Country",
            height=400
        )
        
        return fig
    
    def create_hashtag_performance_chart(self, hashtag_winners):
        """Create hashtag performance comparison"""
        if not hashtag_winners:
            return None
            
        # Filter for main hashtags
        main_hashtags = ['#chan2024', '#totalenergieschan2024', '#harambeestars', '#ugandacranes', '#taifastars']
        filtered_data = {k: v for k, v in hashtag_winners.items() if k.lower() in main_hashtags}
        
        if not filtered_data:
            return None
        
        hashtags = list(filtered_data.keys())
        winners = [data['winner'] for data in filtered_data.values()]
        counts = [data['usage_count'] for data in filtered_data.values()]
        
        # Color mapping
        color_map = {'Kenya': '#FF6B6B', 'Uganda': '#4ECDC4', 'Tanzania': '#45B7D1'}
        colors = [color_map.get(winner, '#888888') for winner in winners]
        
        fig = go.Figure(data=[
            go.Bar(
                x=hashtags,
                y=counts,
                marker_color=colors,
                text=[f"{winner}<br>{count}" for winner, count in zip(winners, counts)],
                textposition='outside'
            )
        ])
        
        fig.update_layout(
            title="🏷️ Hashtag Performance Leaders",
            xaxis_title="Hashtags",
            yaxis_title="Tweet Count",
            height=400
        )
        
        return fig
    
    def create_time_series_chart(self, trend_data):
        """Create time series chart showing tweet trends"""
        if trend_data.empty:
            return None
            
        fig = px.line(
            trend_data, 
            x='date', 
            y='tweet_count', 
            color='country',
            title="📈 Tweet Volume Trends Over Time",
            color_discrete_map={
                'Kenya': '#FF6B6B',
                'Uganda': '#4ECDC4', 
                'Tanzania': '#45B7D1'
            }
        )
        
        fig.update_layout(height=400)
        return fig
    
    def create_engagement_metrics_chart(self, rankings):
        """Create engagement metrics comparison"""
        if not rankings:
            return None
            
        countries = list(rankings.keys())
        engagement = [data['avg_engagement'] for data in rankings.values()]
        
        fig = go.Figure(data=[
            go.Bar(
                x=countries,
                y=engagement,
                marker_color=['#FF6B6B', '#4ECDC4', '#45B7D1'],
                text=[f"{eng:.1f}" for eng in engagement],
                textposition='outside'
            )
        ])
        
        fig.update_layout(
            title="🔥 Average Engagement by Country",
            xaxis_title="Countries",
            yaxis_title="Avg Engagement (Likes + Retweets + Replies)",
            height=400
        )
        
        return fig
    
    def display_key_metrics(self, rankings):
        """Display key metrics in columns"""
        if not rankings:
            st.error("No data available")
            return
            
        # Get winner
        winner = min(rankings.items(), key=lambda x: x[1]['rank'])
        total_tweets = sum(data['tweet_count'] for data in rankings.values())
        avg_sentiment = sum(data['avg_sentiment'] * data['tweet_count'] for data in rankings.values()) / total_tweets
        
        # Most positive country
        most_positive = max(rankings.items(), key=lambda x: x[1]['positive_percentage'])
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "🏆 Tweet Volume Champion",
                winner[0],
                f"{winner[1]['tweet_count']} tweets"
            )
        
        with col2:
            st.metric(
                "📊 Total Tweets",
                f"{total_tweets:,}",
                f"Avg Sentiment: {avg_sentiment:.3f}"
            )
        
        with col3:
            st.metric(
                "😊 Most Positive",
                most_positive[0],
                f"{most_positive[1]['positive_percentage']:.1f}%"
            )
        
        with col4:
            most_engaging = max(rankings.items(), key=lambda x: x[1]['avg_engagement'])
            st.metric(
                "🔥 Highest Engagement",
                most_engaging[0],
                f"{most_engaging[1]['avg_engagement']:.1f} avg"
            )
    
    def display_detailed_table(self, rankings):
        """Display detailed country comparison table"""
        if not rankings:
            return
            
        # Convert to DataFrame for better display
        table_data = []
        for country, data in sorted(rankings.items(), key=lambda x: x[1]['rank']):
            table_data.append({
                'Rank': f"#{data['rank']}",
                'Country': country,
                'Total Tweets': data['tweet_count'],
                'Avg Sentiment': f"{data['avg_sentiment']:.3f}",
                'Positive %': f"{data['positive_percentage']:.1f}%",
                'Negative %': f"{data['negative_percentage']:.1f}%",
                'Neutral %': f"{data['neutral_percentage']:.1f}%",
                'Avg Engagement': f"{data['avg_engagement']:.1f}"
            })
        
        df = pd.DataFrame(table_data)
        
        # Style the dataframe
        def highlight_winner(val):
            if val == '#1':
                return 'background-color: gold'
            elif val == '#2': 
                return 'background-color: silver'
            elif val == '#3':
                return 'background-color: #CD7F32'
            return ''
        
        styled_df = df.style.applymap(highlight_winner, subset=['Rank'])
        st.dataframe(styled_df, use_container_width=True)

def main():
    """Main Streamlit app"""
    
    # Auto-refresh every 30 seconds
    st_autorefresh(interval=30000, key="datarefresh")
    
    # Title and header
    st.title("🏆 CHAN 2024 TotalEnergies Twitter Analytics Dashboard")
    st.markdown("### Real-time sentiment analysis and country comparison")
    
    # Initialize dashboard
    dashboard = CHAN2024Dashboard()
    
    # Sidebar controls
    st.sidebar.header("📊 Dashboard Controls")
    
    # Time range selector
    time_range = st.sidebar.selectbox(
        "Select Time Range",
        ["Last 24 hours", "Last 3 days", "Last 7 days", "Last 14 days", "Last 30 days"],
        index=2
    )
    
    # Convert to days
    days_map = {
        "Last 24 hours": 1,
        "Last 3 days": 3, 
        "Last 7 days": 7,
        "Last 14 days": 14,
        "Last 30 days": 30
    }
    selected_days = days_map[time_range]
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.rerun()
    
    # Load data
    with st.spinner("Loading latest data..."):
        rankings, hashtag_winners, trend_data = dashboard.load_data(selected_days)
    
    if not rankings:
        st.warning("⚠️ No data available. Make sure the data pipeline is running!")
        st.info("💡 Start the main.py script to begin collecting tweets.")
        return
    
    # Display key metrics
    st.markdown("## 📈 Key Metrics")
    dashboard.display_key_metrics(rankings)
    
    # Main charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Country comparison chart
        comparison_fig = dashboard.create_country_comparison_chart(rankings)
        if comparison_fig:
            st.plotly_chart(comparison_fig, use_container_width=True)
    
    with col2:
        # Engagement metrics
        engagement_fig = dashboard.create_engagement_metrics_chart(rankings)
        if engagement_fig:
            st.plotly_chart(engagement_fig, use_container_width=True)
    
    # Sentiment distribution
    st.markdown("## 😊 Sentiment Analysis")
    sentiment_fig = dashboard.create_sentiment_distribution_pie(rankings)
    if sentiment_fig:
        st.plotly_chart(sentiment_fig, use_container_width=True)
    
    # Hashtag performance
    st.markdown("## 🏷️ Hashtag Performance")
    hashtag_fig = dashboard.create_hashtag_performance_chart(hashtag_winners)
    if hashtag_fig:
        st.plotly_chart(hashtag_fig, use_container_width=True)
    else:
        st.info("No hashtag performance data available yet.")
    
    # Time series trends
    if not trend_data.empty:
        st.markdown("## 📊 Tweet Trends Over Time")
        trend_fig = dashboard.create_time_series_chart(trend_data)
        if trend_fig:
            st.plotly_chart(trend_fig, use_container_width=True)
    
    # Detailed table
    st.markdown("## 📋 Detailed Country Rankings")
    dashboard.display_detailed_table(rankings)
    
    # Recent tweets section
    st.markdown("## 🐦 Recent Tweet Insights")
    
    # Show hashtag winners details
    if hashtag_winners:
        st.markdown("### Hashtag Leaders")
        for hashtag, data in list(hashtag_winners.items())[:5]:
            col1, col2, col3 = st.columns([2, 1, 2])
            with col1:
                st.write(f"**{hashtag}**")
            with col2:
                st.write(f"👑 {data['winner']}")
            with col3:
                st.write(f"{data['usage_count']} tweets | Sentiment: {data['avg_sentiment']:.3f}")
    
    # Footer
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("🇰🇪 **Kenya**: #HarambeeStars")
    with col2:
        st.markdown("🇺🇬 **Uganda**: #UgandaCranes") 
    with col3:
        st.markdown("🇹🇿 **Tanzania**: #TaifaStars")
    
    st.markdown("**🤝 Pamoja - Together we host CHAN 2024!**")
    
    # Last updated timestamp
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()

# =============================================================================
# run_dashboard.py (Launcher script)
# =============================================================================
import subprocess
import sys
import os

def run_dashboard():
    """Launch the Streamlit dashboard"""
    try:
        # Set environment variables if needed
        env = os.environ.copy()
        
        # Run streamlit
        cmd = [sys.executable, "-m", "streamlit", "run", "streamlit_dashboard.py", "--server.port=8501"]
        
        print("🚀 Starting CHAN 2024 Dashboard...")
        print("📊 Dashboard will be available at: http://localhost:8501")
        print("⏹️  Press Ctrl+C to stop the dashboard")
        
        subprocess.run(cmd, env=env)
        
    except KeyboardInterrupt:
        print("\n👋 Dashboard shutdown complete!")
    except Exception as e:
        print(f"❌ Error starting dashboard: {e}")

if __name__ == "__main__":
    run_dashboard()