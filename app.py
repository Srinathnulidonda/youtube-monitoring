import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from googleapiclient.discovery import build
import requests
import sqlite3
import re
from threading import Thread
import time
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'telugu_cinema_monitor_2025'

# Configuration
CONFIG = {
    'YOUTUBE_API_KEY': os.getenv('YOUTUBE_API_KEY'),
    'TELEGRAM_BOT_TOKEN': os.getenv('TELEGRAM_BOT_TOKEN'),
    'TELEGRAM_CHANNEL_ID': os.getenv('TELEGRAM_CHANNEL_ID'),
    'ADMIN_USERNAME': os.getenv('ADMIN_USERNAME', 'admin'),
    'ADMIN_PASSWORD': os.getenv('ADMIN_PASSWORD', 'password123'),
    'MONITORING_INTERVAL': 1800,  # 30 minutes
    'AUTO_POST_THRESHOLD': 5,  # Only 5/5 priority auto-posted
    'MONITORING_DAYS': 2,  # Only last 2 days
    'MAX_RESULTS_PER_SEARCH': 25,
}

class TeluguCinemaMonitor:
    def __init__(self):
        self.youtube = None
        self.api_quota_used = 0
        self.max_daily_quota = 10000
        self.quota_reset_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        self.last_check = datetime.now() - timedelta(days=1)
        self.init_database()
        self.init_youtube_api()
        
        # Official Telugu cinema channels and their verified status
        self.official_channels = {
            # Major Production Houses
            'UC-_5Lj_yajKrEhvt8LQ3Jw': {'name': 'AahaSTV', 'type': 'official', 'priority_boost': 2},
            'UCi7xZmd_NqwLCOZzrAGIFgg': {'name': 'T-Series Telugu', 'type': 'official', 'priority_boost': 2},
            'UCLm_u6TnJhw2tRvmJO7bIBQ': {'name': 'Aditya Music', 'type': 'official', 'priority_boost': 2},
            'UCPkIkiODGDAhJdSJmVa1JYg': {'name': 'Lahari Music', 'type': 'official', 'priority_boost': 2},
            'UC-1NMppIgk8q6BYlVqXeT6A': {'name': 'Mango Music', 'type': 'official', 'priority_boost': 2},
            'UCGbVb0bMxKtBSjFKlhSJ7mw': {'name': 'Saregama Telugu', 'type': 'official', 'priority_boost': 2},
            'UCQSf3-xEzYLy5n-Gq_XNZFQ': {'name': 'Sony Music South', 'type': 'official', 'priority_boost': 2},
            'UCT7njX_vUGPg8SzXkamcPow': {'name': 'Jio Studios', 'type': 'official', 'priority_boost': 2},
            'UCCb3E3_GIlJJBPRyNBAYG4g': {'name': 'Mythri Movie Makers', 'type': 'official', 'priority_boost': 2},
            'UC_L7SzQFgRH9nVOe2cJ7hqw': {'name': 'Haarika & Hassine Creations', 'type': 'official', 'priority_boost': 2},
            'UCNOJlqxaZz5Oy5Uw3LjKTpg': {'name': 'Geetha Arts', 'type': 'official', 'priority_boost': 2},
            'UCYq5VPOOx6Kd8vwCEoL5GcQ': {'name': 'Dil Raju Productions', 'type': 'official', 'priority_boost': 2},
            'UCr7nk_DPTQ4W7Rg8g4Z7X-Q': {'name': 'UV Creations', 'type': 'official', 'priority_boost': 2},
            
            # Major News Channels
            'UCfQmN8u4LKpv6YdngOyLM8g': {'name': 'TV9 Telugu', 'type': 'news', 'priority_boost': 1},
            'UCMhq23LMNgzJJCr7K1h9lTQ': {'name': 'ABN Telugu', 'type': 'news', 'priority_boost': 1},
            'UC3EhhhvODy4GQHOOpxNdSnQ': {'name': 'V6 News Telugu', 'type': 'news', 'priority_boost': 1},
            'UCLCkOhQ3zq4VAhLPQFcS1VA': {'name': 'Sakshi TV', 'type': 'news', 'priority_boost': 1},
            'UC34HtHEkLIgKnU3PKvJKiJw': {'name': 'NTV Telugu', 'type': 'news', 'priority_boost': 1},
            'UCLqBdTEn_9M-2E7GZHGnqMg': {'name': 'Studio One', 'type': 'news', 'priority_boost': 1},
            
            # Entertainment Channels
            'UCp1tiJTtdB_qOqhJhIJ4Ygg': {'name': 'Gemini TV', 'type': 'entertainment', 'priority_boost': 1},
            'UC1yKCHDaAjQMl-vqH0Dz_Zw': {'name': 'ETV Plus', 'type': 'entertainment', 'priority_boost': 1},
            'UCt0Q8L2CJ99YMU3Jbm1hLWA': {'name': 'Star Maa', 'type': 'entertainment', 'priority_boost': 1},
            'UCb6oYbCIzGe4R7RmXI7qmRA': {'name': 'Zee Telugu', 'type': 'entertainment', 'priority_boost': 1},
            
            # Box Office & Analysis
            'UCfgArb5-TgW9N0m7vF3mjTg': {'name': 'Great Andhra', 'type': 'analysis', 'priority_boost': 1},
            'UCsVMjJRd-RxJIDYJYmvR3LA': {'name': 'Telugu Cinema', 'type': 'analysis', 'priority_boost': 1},
        }
        
        # Enhanced Telugu keywords with official markers
        self.telugu_keywords = [
            'telugu movie official trailer',
            'telugu movie official teaser', 
            'tollywood official trailer',
            'telugu film official',
            'telugu movie first look',
            'telugu box office collection',
            'tollywood news today',
            'telugu movie review',
            'telugu songs official',
            'telugu cinema news',
        ]
        
        # Content categories with refined priority system
        self.content_categories = {
            'official_trailer': {'keywords': ['official trailer', 'theatrical trailer'], 'priority': 5},
            'official_teaser': {'keywords': ['official teaser', 'first look', 'title teaser'], 'priority': 5},
            'official_song': {'keywords': ['full video song', 'lyrical song', 'official video song'], 'priority': 4},
            'box_office': {'keywords': ['box office', 'collection', 'day 1 collection', 'opening day'], 'priority': 4},
            'breaking_news': {'keywords': ['breaking news', 'exclusive', 'confirmed'], 'priority': 4},
            'movie_review': {'keywords': ['movie review', 'rating', 'critics review'], 'priority': 3},
            'audio_launch': {'keywords': ['audio launch', 'music launch', 'pre release'], 'priority': 3},
            'interview': {'keywords': ['interview', 'exclusive interview'], 'priority': 2},
            'behind_scenes': {'keywords': ['making', 'behind the scenes', 'bts'], 'priority': 2},
            'other': {'keywords': [], 'priority': 1}
        }

    def init_database(self):
        """Initialize SQLite database with enhanced schema"""
        conn = sqlite3.connect('telugu_cinema.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY,
                title TEXT,
                channel TEXT,
                channel_id TEXT,
                published_at TEXT,
                description TEXT,
                thumbnail TEXT,
                view_count INTEGER DEFAULT 0,
                like_count INTEGER DEFAULT 0,
                comment_count INTEGER DEFAULT 0,
                category TEXT,
                priority INTEGER,
                is_official_source BOOLEAN DEFAULT FALSE,
                channel_type TEXT,
                sent_to_telegram BOOLEAN DEFAULT FALSE,
                admin_approved BOOLEAN DEFAULT FALSE,
                auto_posted BOOLEAN DEFAULT FALSE,
                verification_score INTEGER DEFAULT 0,
                engagement_rate REAL DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitoring_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                videos_found INTEGER DEFAULT 0,
                official_videos INTEGER DEFAULT 0,
                auto_posted INTEGER DEFAULT 0,
                manual_posted INTEGER DEFAULT 0,
                api_calls INTEGER DEFAULT 0,
                quota_used INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS api_quota_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT,
                quota_cost INTEGER,
                total_used INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Add new columns if they don't exist
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN channel_id TEXT')
            cursor.execute('ALTER TABLE videos ADD COLUMN is_official_source BOOLEAN DEFAULT FALSE')
            cursor.execute('ALTER TABLE videos ADD COLUMN channel_type TEXT')
            cursor.execute('ALTER TABLE videos ADD COLUMN verification_score INTEGER DEFAULT 0')
            cursor.execute('ALTER TABLE videos ADD COLUMN engagement_rate REAL DEFAULT 0.0')
            cursor.execute('ALTER TABLE videos ADD COLUMN comment_count INTEGER DEFAULT 0')
            cursor.execute('ALTER TABLE monitoring_stats ADD COLUMN official_videos INTEGER DEFAULT 0')
            cursor.execute('ALTER TABLE monitoring_stats ADD COLUMN quota_used INTEGER DEFAULT 0')
        except sqlite3.OperationalError:
            pass  # Columns already exist
        
        conn.commit()
        conn.close()

    def init_youtube_api(self):
        """Initialize YouTube API with quota tracking"""
        if CONFIG['YOUTUBE_API_KEY']:
            try:
                self.youtube = build('youtube', 'v3', developerKey=CONFIG['YOUTUBE_API_KEY'])
                logger.info("YouTube API initialized successfully")
                self.reset_daily_quota_if_needed()
            except Exception as e:
                logger.error(f"Failed to initialize YouTube API: {e}")

    def reset_daily_quota_if_needed(self):
        """Reset quota if new day started"""
        now = datetime.now()
        if now >= self.quota_reset_time:
            self.api_quota_used = 0
            self.quota_reset_time = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            logger.info("Daily API quota reset")

    def log_api_usage(self, operation, cost):
        """Log API usage for tracking"""
        self.api_quota_used += cost
        
        conn = sqlite3.connect('telugu_cinema.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO api_quota_log (operation, quota_cost, total_used)
            VALUES (?, ?, ?)
        ''', (operation, cost, self.api_quota_used))
        conn.commit()
        conn.close()

    def is_official_channel(self, channel_id):
        """Check if channel is official"""
        return channel_id in self.official_channels

    def get_channel_info(self, channel_id):
        """Get channel information and verification status"""
        if channel_id in self.official_channels:
            return self.official_channels[channel_id]
        return {'name': 'Unknown', 'type': 'unofficial', 'priority_boost': 0}

    def calculate_verification_score(self, video_data):
        """Calculate verification score based on multiple factors"""
        score = 0
        
        # Official channel boost
        if video_data.get('is_official_source', False):
            score += 50
        
        # Subscriber count (if available)
        # Note: Would need additional API call to get subscriber count
        
        # Engagement metrics
        view_count = video_data.get('view_count', 0)
        like_count = video_data.get('like_count', 0)
        comment_count = video_data.get('comment_count', 0)
        
        if view_count > 0:
            engagement_rate = (like_count + comment_count) / view_count
            video_data['engagement_rate'] = engagement_rate
            
            if engagement_rate > 0.01:  # 1% engagement
                score += 20
            elif engagement_rate > 0.005:  # 0.5% engagement
                score += 10
        
        # Title quality (official markers)
        title_lower = video_data.get('title', '').lower()
        official_markers = ['official', 'trailer', 'teaser', 'first look', 'exclusive']
        score += sum(10 for marker in official_markers if marker in title_lower)
        
        return min(score, 100)  # Cap at 100

    def categorize_content(self, title, description=""):
        """Enhanced content categorization"""
        title_lower = title.lower()
        desc_lower = description.lower()
        text = f"{title_lower} {desc_lower}"
        
        # Check for official markers first
        official_markers = ['official trailer', 'official teaser', 'official video', 'official song']
        has_official_marker = any(marker in text for marker in official_markers)
        
        # Categorize based on content type
        for category, data in self.content_categories.items():
            for keyword in data['keywords']:
                if keyword in text:
                    priority = data['priority']
                    if has_official_marker and category.startswith('official'):
                        priority = 5  # Boost official content
                    return category, priority
        
        return 'other', self.content_categories['other']['priority']

    def calculate_final_priority(self, video_data):
        """Calculate final priority with all factors"""
        base_priority = video_data['priority']
        
        # Official source boost
        if video_data.get('is_official_source', False):
            channel_info = self.get_channel_info(video_data.get('channel_id', ''))
            base_priority += channel_info.get('priority_boost', 0)
        
        # Verification score influence
        verification_score = video_data.get('verification_score', 0)
        if verification_score >= 80:
            base_priority += 1
        elif verification_score >= 60:
            base_priority += 0.5
        
        # Recent content boost
        pub_time = datetime.fromisoformat(video_data['published_at'].replace('Z', '+00:00'))
        time_diff = datetime.now() - pub_time.replace(tzinfo=None)
        if time_diff.total_seconds() < 3600:  # Within 1 hour
            base_priority += 0.5
        elif time_diff.total_seconds() < 21600:  # Within 6 hours
            base_priority += 0.25
        
        # High engagement boost
        engagement_rate = video_data.get('engagement_rate', 0)
        if engagement_rate > 0.02:  # 2% engagement
            base_priority += 0.5
        elif engagement_rate > 0.01:  # 1% engagement
            base_priority += 0.25
        
        return min(int(base_priority), 5)  # Cap at 5

    def search_telugu_content(self):
        """Enhanced search focusing on official sources"""
        if not self.youtube:
            logger.error("YouTube API not initialized")
            return []

        self.reset_daily_quota_if_needed()
        
        if self.api_quota_used >= self.max_daily_quota * 0.9:  # 90% quota used
            logger.warning("API quota nearly exhausted, skipping search")
            return []

        all_videos = []
        published_after = (datetime.now() - timedelta(days=CONFIG['MONITORING_DAYS'])).isoformat() + 'Z'
        
        # Search official channels first
        for channel_id, channel_info in list(self.official_channels.items())[:10]:  # Limit channels
            try:
                search_response = self.youtube.search().list(
                    channelId=channel_id,
                    part='id,snippet',
                    type='video',
                    publishedAfter=published_after,
                    maxResults=5,
                    order='date'
                ).execute()
                
                self.log_api_usage(f"Channel search: {channel_info['name']}", 100)
                
                for item in search_response['items']:
                    video_data = self.extract_video_data(item, channel_id, channel_info)
                    if video_data:
                        all_videos.append(video_data)
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error searching channel {channel_info['name']}: {e}")
                continue
        
        # Search by keywords for additional content
        for keyword in self.telugu_keywords[:5]:  # Limit keywords
            if self.api_quota_used >= self.max_daily_quota * 0.9:
                break
                
            try:
                search_response = self.youtube.search().list(
                    q=keyword,
                    part='id,snippet',
                    type='video',
                    publishedAfter=published_after,
                    maxResults=CONFIG['MAX_RESULTS_PER_SEARCH'] // 5,
                    order='relevance',
                    regionCode='IN'
                ).execute()
                
                self.log_api_usage(f"Keyword search: {keyword}", 100)
                
                for item in search_response['items']:
                    channel_id = item['snippet']['channelId']
                    channel_info = self.get_channel_info(channel_id)
                    video_data = self.extract_video_data(item, channel_id, channel_info)
                    if video_data:
                        all_videos.append(video_data)
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error searching keyword '{keyword}': {e}")
                continue

        # Remove duplicates and sort by priority
        unique_videos = {v['id']: v for v in all_videos}.values()
        sorted_videos = sorted(unique_videos, key=lambda x: (x['priority'], x['verification_score']), reverse=True)
        
        return list(sorted_videos)

    def extract_video_data(self, item, channel_id, channel_info):
        """Extract and enrich video data"""
        try:
            video_data = {
                'id': item['id']['videoId'],
                'title': item['snippet']['title'],
                'channel': item['snippet']['channelTitle'],
                'channel_id': channel_id,
                'published_at': item['snippet']['publishedAt'],
                'description': item['snippet']['description'],
                'thumbnail': item['snippet']['thumbnails'].get('medium', {}).get('url', ''),
                'is_official_source': self.is_official_channel(channel_id),
                'channel_type': channel_info['type']
            }
            
            # Get detailed video statistics
            video_stats = self.youtube.videos().list(
                part='statistics',
                id=video_data['id']
            ).execute()
            
            self.log_api_usage("Video statistics", 1)
            
            if video_stats['items']:
                stats = video_stats['items'][0]['statistics']
                video_data.update({
                    'view_count': int(stats.get('viewCount', 0)),
                    'like_count': int(stats.get('likeCount', 0)),
                    'comment_count': int(stats.get('commentCount', 0))
                })
            
            # Categorize and prioritize
            category, priority = self.categorize_content(video_data['title'], video_data['description'])
            video_data.update({
                'category': category,
                'priority': priority
            })
            
            # Calculate verification score
            video_data['verification_score'] = self.calculate_verification_score(video_data)
            
            # Calculate final priority
            video_data['priority'] = self.calculate_final_priority(video_data)
            
            return video_data
            
        except Exception as e:
            logger.error(f"Error extracting video data: {e}")
            return None

    def save_videos_to_db(self, videos):
        """Save videos to database with enhanced data"""
        conn = sqlite3.connect('telugu_cinema.db')
        cursor = conn.cursor()
        
        new_videos = []
        
        for video in videos:
            # Check if video already exists
            cursor.execute('SELECT id FROM videos WHERE id = ?', (video['id'],))
            if not cursor.fetchone():
                cursor.execute('''
                    INSERT INTO videos 
                    (id, title, channel, channel_id, published_at, description, thumbnail, 
                     view_count, like_count, comment_count, category, priority, 
                     is_official_source, channel_type, verification_score, engagement_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    video['id'], video['title'], video['channel'], video['channel_id'],
                    video['published_at'], video['description'], video['thumbnail'],
                    video.get('view_count', 0), video.get('like_count', 0), 
                    video.get('comment_count', 0), video['category'], video['priority'],
                    video.get('is_official_source', False), video.get('channel_type', 'unofficial'),
                    video.get('verification_score', 0), video.get('engagement_rate', 0.0)
                ))
                new_videos.append(video)
        
        conn.commit()
        conn.close()
        
        return new_videos

    def format_telegram_message(self, video):
        """Enhanced Telegram message formatting"""
        category_emojis = {
            'official_trailer': '🎬',
            'official_teaser': '📽️',
            'official_song': '🎵',
            'box_office': '💰',
            'breaking_news': '🚨',
            'movie_review': '⭐',
            'audio_launch': '🎤',
            'interview': '🎙️',
            'behind_scenes': '🎭',
            'other': '📺'
        }
        
        emoji = category_emojis.get(video['category'], '📺')
        priority_stars = '⭐' * video['priority']
        
        # Official source indicator
        official_badge = '✅ OFFICIAL' if video.get('is_official_source', False) else '📺 CHANNEL'
        
        # Format published time
        pub_time = datetime.fromisoformat(video['published_at'].replace('Z', '+00:00'))
        time_ago = datetime.now() - pub_time.replace(tzinfo=None)
        
        if time_ago.total_seconds() < 3600:
            time_str = f"{int(time_ago.total_seconds() // 60)} minutes ago"
        elif time_ago.total_seconds() < 86400:
            time_str = f"{int(time_ago.total_seconds() // 3600)} hours ago"
        else:
            time_str = f"{time_ago.days} day(s) ago"
        
        # Engagement metrics
        engagement_rate = video.get('engagement_rate', 0) * 100
        verification_score = video.get('verification_score', 0)
        
        message = f"""
{emoji} *{video['category'].replace('_', ' ').upper()}* | Telugu Cinema
{priority_stars} Priority: {video['priority']}/5 | {official_badge}

🎭 *{video['title']}*

📺 Channel: {video['channel']}
👀 Views: {video.get('view_count', 0):,}
👍 Likes: {video.get('like_count', 0):,}
💬 Comments: {video.get('comment_count', 0):,}
📊 Engagement: {engagement_rate:.2f}%
✅ Verification: {verification_score}/100
⏰ {time_str}

🔗 [Watch Now](https://youtube.com/watch?v={video['id']})

#TeluguCinema #{video['category'].replace('_', '').capitalize()} #Tollywood #Priority{video['priority']}
        """.strip()
        
        return message

    def send_to_telegram(self, video, is_auto=True):
        """Send video update to Telegram channel"""
        if not CONFIG['TELEGRAM_BOT_TOKEN'] or not CONFIG['TELEGRAM_CHANNEL_ID']:
            logger.warning("Telegram credentials not configured")
            return False
        
        message = self.format_telegram_message(video)
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHANNEL_ID'],
            'text': message,
            'parse_mode': 'Markdown',
            'disable_web_page_preview': False
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                post_type = "automatically" if is_auto else "manually"
                logger.info(f"Successfully sent video to Telegram ({post_type}): {video['title']}")
                
                # Mark as sent in database
                conn = sqlite3.connect('telugu_cinema.db')
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE videos 
                    SET sent_to_telegram = TRUE, auto_posted = ?, admin_approved = ?
                    WHERE id = ?
                ''', (is_auto, not is_auto, video['id']))
                conn.commit()
                conn.close()
                
                return True
            else:
                logger.error(f"Failed to send to Telegram: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending to Telegram: {e}")
            return False

    def run_monitoring_cycle(self):
        """Enhanced monitoring cycle"""
        logger.info("Starting monitoring cycle...")
        
        try:
            # Check API quota
            if self.api_quota_used >= self.max_daily_quota * 0.9:
                logger.warning("API quota nearly exhausted, skipping monitoring")
                return
            
            # Search for new videos
            videos = self.search_telugu_content()
            logger.info(f"Found {len(videos)} videos")
            
            # Save to database and get new ones
            new_videos = self.save_videos_to_db(videos)
            logger.info(f"Found {len(new_videos)} new videos")
            
            # Count official vs unofficial
            official_count = sum(1 for v in new_videos if v.get('is_official_source', False))
            
            # Auto-post only priority 5 videos
            auto_sent_count = 0
            
            for video in new_videos:
                if video['priority'] >= CONFIG['AUTO_POST_THRESHOLD']:
                    if self.send_to_telegram(video, is_auto=True):
                        auto_sent_count += 1
                    time.sleep(3)  # Rate limiting
            
            # Update monitoring stats
            self.update_monitoring_stats(len(videos), official_count, auto_sent_count, 0)
            
            logger.info(f"Monitoring cycle completed. Found: {len(videos)}, Official: {official_count}, Auto-sent: {auto_sent_count}")
            
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")

    def update_monitoring_stats(self, videos_found, official_videos, auto_posted, manual_posted):
        """Update monitoring statistics"""
        conn = sqlite3.connect('telugu_cinema.db')
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        cursor.execute('''
            INSERT INTO monitoring_stats 
            (date, videos_found, official_videos, auto_posted, manual_posted, api_calls, quota_used)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (today, videos_found, official_videos, auto_posted, manual_posted, 
              self.api_quota_used, self.api_quota_used))
        
        conn.commit()
        conn.close()

    def get_recent_videos(self, days=2):
        """Get videos from last N days only"""
        conn = sqlite3.connect('telugu_cinema.db')
        cursor = conn.cursor()
        
        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        cursor.execute('''
            SELECT * FROM videos 
            WHERE published_at >= ? 
            ORDER BY priority DESC, verification_score DESC, published_at DESC
            LIMIT 50
        ''', (cutoff_date,))
        
        videos = cursor.fetchall()
        conn.close()
        
        return videos

    def get_pending_videos(self):
        """Get videos pending manual approval from last 2 days"""
        conn = sqlite3.connect('telugu_cinema.db')
        cursor = conn.cursor()
        
        cutoff_date = (datetime.now() - timedelta(days=CONFIG['MONITORING_DAYS'])).isoformat()
        
        cursor.execute('''
            SELECT * FROM videos 
            WHERE sent_to_telegram = FALSE 
            AND published_at >= ?
            AND priority < ?
            ORDER BY is_official_source DESC, priority DESC, verification_score DESC, published_at DESC
        ''', (cutoff_date, CONFIG['AUTO_POST_THRESHOLD']))
        
        videos = cursor.fetchall()
        conn.close()
        
        return videos

    def approve_and_send_video(self, video_id):
        """Approve and send a video manually"""
        conn = sqlite3.connect('telugu_cinema.db')
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM videos WHERE id = ?', (video_id,))
        video_data = cursor.fetchone()
        conn.close()
        
        if not video_data:
            return False, "Video not found"
        
        # Convert to dict
        video = {
            'id': video_data[0],
            'title': video_data[1],
            'channel': video_data[2],
            'channel_id': video_data[3],
            'published_at': video_data[4],
            'description': video_data[5],
            'thumbnail': video_data[6],
            'view_count': video_data[7],
            'like_count': video_data[8],
            'comment_count': video_data[9],
            'category': video_data[10],
            'priority': video_data[11],
            'is_official_source': bool(video_data[12]),
            'channel_type': video_data[13],
            'verification_score': video_data[17],
            'engagement_rate': video_data[18]
        }
        
        if self.send_to_telegram(video, is_auto=False):
            # Update stats
            self.update_monitoring_stats(0, 0, 0, 1)
            return True, "Video sent successfully"
        else:
            return False, "Failed to send video"

    def get_dashboard_data(self):
        """Get comprehensive dashboard data"""
        conn = sqlite3.connect('telugu_cinema.db')
        cursor = conn.cursor()
        
        # Get recent videos (last 2 days only)
        recent_videos = self.get_recent_videos(CONFIG['MONITORING_DAYS'])
        
        # Get pending manual approval videos
        pending_videos = self.get_pending_videos()
        
        # Get today's statistics
        today = datetime.now().strftime('%Y-%m-%d')
        cutoff_date = (datetime.now() - timedelta(days=CONFIG['MONITORING_DAYS'])).isoformat()
        
        # Category statistics (last 2 days)
        cursor.execute('''
            SELECT category, COUNT(*) as count 
            FROM videos 
            WHERE published_at >= ?
            GROUP BY category
            ORDER BY count DESC
        ''', (cutoff_date,))
        category_stats = dict(cursor.fetchall())
        
        # Priority distribution (last 2 days)
        cursor.execute('''
            SELECT priority, COUNT(*) as count 
            FROM videos 
            WHERE published_at >= ?
            GROUP BY priority
            ORDER BY priority DESC
        ''', (cutoff_date,))
        priority_stats = dict(cursor.fetchall())
        
        # Official vs Unofficial sources (last 2 days)
        cursor.execute('''
            SELECT 
                COUNT(CASE WHEN is_official_source = 1 THEN 1 END) as official,
                COUNT(CASE WHEN is_official_source = 0 THEN 1 END) as unofficial
            FROM videos 
            WHERE published_at >= ?
        ''', (cutoff_date,))
        source_stats = cursor.fetchone()
        
        # Posting statistics (last 2 days)
        cursor.execute('''
            SELECT 
                COUNT(CASE WHEN auto_posted = 1 THEN 1 END) as auto_posted,
                COUNT(CASE WHEN admin_approved = 1 THEN 1 END) as manual_posted,
                COUNT(CASE WHEN sent_to_telegram = 0 THEN 1 END) as pending
            FROM videos 
            WHERE published_at >= ?
        ''', (cutoff_date,))
        posting_stats = cursor.fetchone()
        
        # Channel type distribution (last 2 days)
        cursor.execute('''
            SELECT channel_type, COUNT(*) as count 
            FROM videos 
            WHERE published_at >= ? AND is_official_source = 1
            GROUP BY channel_type
            ORDER BY count DESC
        ''', (cutoff_date,))
        channel_type_stats = dict(cursor.fetchall())
        
        # API quota usage for last 7 days
        cursor.execute('''
            SELECT DATE(timestamp) as date, SUM(quota_cost) as daily_usage
            FROM api_quota_log 
            WHERE DATE(timestamp) >= DATE('now', '-7 days')
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
        ''')
        quota_history = cursor.fetchall()
        
        # Top performing videos (last 2 days)
        cursor.execute('''
            SELECT * FROM videos 
            WHERE published_at >= ?
            ORDER BY (view_count + like_count * 10) DESC
            LIMIT 10
        ''', (cutoff_date,))
        top_videos = cursor.fetchall()
        
        conn.close()
        
        # Calculate quota remaining
        quota_remaining = self.max_daily_quota - self.api_quota_used
        quota_percentage = (self.api_quota_used / self.max_daily_quota) * 100
        
        # Time until quota reset
        time_to_reset = self.quota_reset_time - datetime.now()
        hours_to_reset = int(time_to_reset.total_seconds() // 3600)
        minutes_to_reset = int((time_to_reset.total_seconds() % 3600) // 60)
        
        return {
            'recent_videos': recent_videos,
            'pending_videos': pending_videos,
            'category_stats': category_stats,
            'priority_stats': priority_stats,
            'source_stats': {
                'official': source_stats[0] if source_stats else 0,
                'unofficial': source_stats[1] if source_stats else 0
            },
            'posting_stats': {
                'auto_posted': posting_stats[0] if posting_stats else 0,
                'manual_posted': posting_stats[1] if posting_stats else 0,
                'pending': posting_stats[2] if posting_stats else 0
            },
            'channel_type_stats': channel_type_stats,
            'top_videos': top_videos,
            'quota_info': {
                'used': self.api_quota_used,
                'remaining': quota_remaining,
                'total': self.max_daily_quota,
                'percentage': quota_percentage,
                'hours_to_reset': hours_to_reset,
                'minutes_to_reset': minutes_to_reset
            },
            'quota_history': quota_history,
            'monitoring_days': CONFIG['MONITORING_DAYS'],
            'auto_post_threshold': CONFIG['AUTO_POST_THRESHOLD'],
            'official_channels_count': len(self.official_channels)
        }

    def get_api_quota_status(self):
        """Get real-time API quota status"""
        self.reset_daily_quota_if_needed()
        
        quota_remaining = self.max_daily_quota - self.api_quota_used
        quota_percentage = (self.api_quota_used / self.max_daily_quota) * 100
        
        time_to_reset = self.quota_reset_time - datetime.now()
        
        return {
            'used': self.api_quota_used,
            'remaining': quota_remaining,
            'total': self.max_daily_quota,
            'percentage': round(quota_percentage, 2),
            'reset_in_hours': int(time_to_reset.total_seconds() // 3600),
            'reset_in_minutes': int((time_to_reset.total_seconds() % 3600) // 60),
            'status': 'critical' if quota_percentage > 90 else 'warning' if quota_percentage > 70 else 'good'
        }

# Initialize monitor
monitor = TeluguCinemaMonitor()

def background_monitor():
    """Enhanced background monitoring thread"""
    while True:
        try:
            # Check if it's a good time to run (avoid peak hours)
            current_hour = datetime.now().hour
            if 2 <= current_hour <= 6:  # Run during low-traffic hours
                logger.info("Running scheduled monitoring during off-peak hours")
                monitor.run_monitoring_cycle()
            else:
                # Run regular monitoring
                monitor.run_monitoring_cycle()
            
            time.sleep(CONFIG['MONITORING_INTERVAL'])
            
        except Exception as e:
            logger.error(f"Background monitor error: {e}")
            time.sleep(600)  # Wait 10 minutes on error

# Start background monitoring
monitoring_thread = Thread(target=background_monitor, daemon=True)
monitoring_thread.start()

# Flask Routes
@app.route('/')
def index():
    if 'logged_in' not in session:
        return redirect(url_for('login'))
    return redirect(url_for('dashboard'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        if username == CONFIG['ADMIN_USERNAME'] and password == CONFIG['ADMIN_PASSWORD']:
            session['logged_in'] = True
            return redirect(url_for('dashboard'))
        else:
            return render_template('login.html', error='Invalid credentials')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect(url_for('login'))

@app.route('/dashboard')
def dashboard():
    if 'logged_in' not in session:
        return redirect(url_for('login'))
    
    return render_template('dashboard.html')

@app.route('/api/dashboard-data')
def api_dashboard_data():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    data = monitor.get_dashboard_data()
    return jsonify(data)

@app.route('/api/quota-status')
def api_quota_status():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    quota_status = monitor.get_api_quota_status()
    return jsonify(quota_status)

@app.route('/api/pending-videos')
def api_pending_videos():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    pending_videos = monitor.get_pending_videos()
    return jsonify({'pending_videos': pending_videos})

@app.route('/api/recent-videos')
def api_recent_videos():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    recent_videos = monitor.get_recent_videos(CONFIG['MONITORING_DAYS'])
    return jsonify({'recent_videos': recent_videos})

@app.route('/api/approve-video/<video_id>', methods=['POST'])
def api_approve_video(video_id):
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    success, message = monitor.approve_and_send_video(video_id)
    return jsonify({'success': success, 'message': message})

@app.route('/api/manual-check')
def api_manual_check():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    # Check quota before starting
    quota_status = monitor.get_api_quota_status()
    if quota_status['percentage'] > 90:
        return jsonify({
            'success': False, 
            'error': 'API quota nearly exhausted. Please wait for reset.'
        })
    
    try:
        # Run monitoring cycle in background
        Thread(target=monitor.run_monitoring_cycle).start()
        return jsonify({'success': True, 'message': 'Manual check initiated'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/config', methods=['GET', 'POST'])
def api_config():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    if request.method == 'POST':
        data = request.json
        
        # Update configuration
        for key, value in data.items():
            if key in CONFIG and key not in ['ADMIN_USERNAME', 'ADMIN_PASSWORD']:
                CONFIG[key] = value
                
        return jsonify({'success': True, 'message': 'Configuration updated'})
    
    # Return masked sensitive info
    return jsonify({
        'youtube_api_key': CONFIG['YOUTUBE_API_KEY'][:10] + '...' if CONFIG['YOUTUBE_API_KEY'] else '',
        'telegram_bot_token': CONFIG['TELEGRAM_BOT_TOKEN'][:10] + '...' if CONFIG['TELEGRAM_BOT_TOKEN'] else '',
        'telegram_channel_id': CONFIG['TELEGRAM_CHANNEL_ID'],
        'monitoring_interval': CONFIG['MONITORING_INTERVAL'],
        'auto_post_threshold': CONFIG['AUTO_POST_THRESHOLD'],
        'monitoring_days': CONFIG['MONITORING_DAYS'],
        'max_results_per_search': CONFIG['MAX_RESULTS_PER_SEARCH']
    })

@app.route('/api/official-channels')
def api_official_channels():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    return jsonify({
        'channels': monitor.official_channels,
        'total_count': len(monitor.official_channels)
    })

@app.route('/api/bulk-approve', methods=['POST'])
def api_bulk_approve():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    data = request.json
    video_ids = data.get('video_ids', [])
    
    if not video_ids:
        return jsonify({'success': False, 'error': 'No video IDs provided'})
    
    success_count = 0
    failed_count = 0
    
    for video_id in video_ids:
        success, _ = monitor.approve_and_send_video(video_id)
        if success:
            success_count += 1
        else:
            failed_count += 1
        time.sleep(2)  # Rate limiting
    
    return jsonify({
        'success': True,
        'message': f'Bulk approval completed. Success: {success_count}, Failed: {failed_count}'
    })

if __name__ == '__main__':
    print("🎬 Enhanced Telugu Cinema Monitoring System Starting...")
    print("=" * 60)
    print("📺 Dashboard: http://localhost:5000")
    print("🔐 Default Login: admin / password123")
    print("⚙️  Configure API keys in dashboard or environment variables")
    print(f"🤖 Auto-posting: Only Priority {CONFIG['AUTO_POST_THRESHOLD']}/5 videos")
    print(f"📅 Monitoring: Last {CONFIG['MONITORING_DAYS']} days only")
    print(f"✅ Official Channels: {len(monitor.official_channels)} configured")
    print("👨‍💼 Manual approval: All other content")
    print("📊 Real-time API quota monitoring")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)