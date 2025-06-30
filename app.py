import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from googleapiclient.discovery import build
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import re
from threading import Thread
import time
from collections import defaultdict
from urllib.parse import urlparse
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'telugu_cinema_monitor_2025_secure')

# Enhanced Configuration with Render PostgreSQL support
CONFIG = {
    'DATABASE_URL': os.getenv('DATABASE_URL'),  # Render PostgreSQL URL
    'YOUTUBE_API_KEY': os.getenv('YOUTUBE_API_KEY'),
    'TELEGRAM_BOT_TOKEN': os.getenv('TELEGRAM_BOT_TOKEN'),
    'TELEGRAM_CHANNEL_ID': os.getenv('TELEGRAM_CHANNEL_ID'),
    'ADMIN_USERNAME': os.getenv('ADMIN_USERNAME', 'admin'),
    'ADMIN_PASSWORD': os.getenv('ADMIN_PASSWORD', 'password123'),
    'MONITORING_INTERVAL': int(os.getenv('MONITORING_INTERVAL', '1800')),  # 30 minutes
    'AUTO_POST_THRESHOLD': int(os.getenv('AUTO_POST_THRESHOLD', '4')),
    'CONTENT_AGE_LIMIT_DAYS': int(os.getenv('CONTENT_AGE_LIMIT_DAYS', '2')),  # Show only 2 days old content
    'MAX_DAILY_QUOTA': int(os.getenv('MAX_DAILY_QUOTA', '10000')),
    'ENABLE_CONTENT_FILTERING': True,
    'WEBHOOK_SECRET': os.getenv('WEBHOOK_SECRET', 'your_webhook_secret'),
}

class EnhancedTeluguCinemaMonitor:
    def __init__(self):
        self.youtube = None
        self.api_quota_used = 0
        self.last_check = datetime.now() - timedelta(days=1)
        self.init_database()
        self.init_youtube_api()
        
        # Official Telugu cinema channels (verified sources)
        self.official_channels = {
            # Production Houses
            'mythriofficial': {'name': 'Mythri Movie Makers', 'priority_boost': 2},
            'svccinema': {'name': 'Sri Venkateswara Creations', 'priority_boost': 2},
            'dilrajuofficial': {'name': 'Dil Raju Productions', 'priority_boost': 2},
            'annapurnaofficial': {'name': 'Annapurna Studios', 'priority_boost': 2},
            'geethaartsofficial': {'name': 'Geetha Arts', 'priority_boost': 2},
            'uvcreatinons': {'name': 'UV Creations', 'priority_boost': 2},
            'varuntelugu': {'name': 'Vamsi Shekar Productions', 'priority_boost': 2},
            
            # Music Labels
            'sonymusicindia': {'name': 'Sony Music India', 'priority_boost': 1},
            'adityamusic': {'name': 'Aditya Music', 'priority_boost': 2},
            'speedrecords': {'name': 'Speed Records', 'priority_boost': 1},
            'laharimusic': {'name': 'Lahari Music', 'priority_boost': 2},
            'mango_music': {'name': 'Mango Music', 'priority_boost': 1},
            
            # News Channels
            'hmtvlive': {'name': 'HMTV', 'priority_boost': 1},
            'tv9telugu': {'name': 'TV9 Telugu', 'priority_boost': 1},
            'ntv_telugu': {'name': 'NTV Telugu', 'priority_boost': 1},
            'v6newstvtelugu': {'name': 'V6 News Telugu', 'priority_boost': 1},
            'etvteluguindia': {'name': 'ETV Telugu', 'priority_boost': 1},
            
            # Star Channels
            'prabhasofficial': {'name': 'Prabhas Official', 'priority_boost': 2},
            'alluarjunofficial': {'name': 'Allu Arjun Official', 'priority_boost': 2},
        }
        
        # Enhanced Telugu keywords for monitoring
        self.telugu_keywords = [
            # Movie related
            'telugu movie trailer', 'tollywood trailer', 'telugu cinema trailer',
            'telugu movie teaser', 'tollywood teaser', 'telugu first look',
            'telugu movie songs', 'tollywood songs', 'telugu lyrical video',
            'telugu movie review', 'tollywood review', 'telugu box office',
            'telugu movie news', 'tollywood news', 'telugu cinema news',
            
            # Star specific (latest movies)
            'prabhas new movie', 'prabhas trailer', 'prabhas teaser',
            'mahesh babu new movie', 'mahesh babu trailer', 'mahesh babu teaser',
            'ram charan new movie', 'ram charan trailer', 'ram charan teaser',
            'jr ntr new movie', 'jr ntr trailer', 'jr ntr teaser',
            'allu arjun new movie', 'allu arjun trailer', 'allu arjun teaser',
            'chiranjeevi new movie', 'chiranjeevi trailer', 'chiranjeevi teaser',
            'vijay deverakonda new movie', 'vijay deverakonda trailer',
            'nani new movie', 'nani trailer', 'nani teaser',
            'ravi teja new movie', 'ravi teja trailer', 'ravi teja teaser',
        ]
        
        # Enhanced content categories with better priority system
        self.content_categories = {
            'official_trailer': {
                'keywords': ['official trailer', 'theatrical trailer'], 
                'priority': 5,
                'auto_post': True
            },
            'teaser': {
                'keywords': ['teaser', 'first look', 'title reveal'], 
                'priority': 5,
                'auto_post': True
            },
            'lyrical_video': {
                'keywords': ['lyrical video', 'lyrical song', 'video song'], 
                'priority': 4,
                'auto_post': True
            },
            'audio_launch': {
                'keywords': ['audio launch', 'music launch', 'pre release'], 
                'priority': 4,
                'auto_post': True
            },
            'movie_update': {
                'keywords': ['movie update', 'shooting update', 'latest update'], 
                'priority': 3,
                'auto_post': False
            },
            'box_office': {
                'keywords': ['box office', 'collection', 'day 1 collection'], 
                'priority': 4,
                'auto_post': True
            },
            'review': {
                'keywords': ['review', 'rating', 'public talk'], 
                'priority': 3,
                'auto_post': False
            },
            'interview': {
                'keywords': ['interview', 'exclusive interview'], 
                'priority': 2,
                'auto_post': False
            },
            'behind_scenes': {
                'keywords': ['making', 'behind the scenes', 'bts'], 
                'priority': 2,
                'auto_post': False
            },
            'other': {
                'keywords': [], 
                'priority': 1,
                'auto_post': False
            }
        }

        # Spam/irrelevant content filters
        self.spam_keywords = [
            'reaction', 'roast', 'troll', 'funny', 'comedy', 'spoof',
            'remix', 'mashup', 'cover', 'dubbing', 'deleted scenes',
            'leaked', 'pirated', 'download', 'free movie', 'full movie',
            'whatsapp status', 'ringtone', 'bgm', 'compilation'
        ]

    def init_database(self):
        """Initialize PostgreSQL database for Render"""
        try:
            conn = psycopg2.connect(CONFIG['DATABASE_URL'])
            cursor = conn.cursor()
            
            # Create tables with proper indexing
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS videos (
                    id VARCHAR(50) PRIMARY KEY,
                    title TEXT NOT NULL,
                    channel VARCHAR(255) NOT NULL,
                    channel_id VARCHAR(50),
                    published_at TIMESTAMP NOT NULL,
                    description TEXT,
                    thumbnail TEXT,
                    video_url TEXT,
                    view_count BIGINT DEFAULT 0,
                    like_count BIGINT DEFAULT 0,
                    comment_count BIGINT DEFAULT 0,
                    category VARCHAR(50),
                    priority INTEGER DEFAULT 1,
                    is_official_source BOOLEAN DEFAULT FALSE,
                    content_quality_score FLOAT DEFAULT 0,
                    sent_to_telegram BOOLEAN DEFAULT FALSE,
                    admin_approved BOOLEAN DEFAULT FALSE,
                    auto_posted BOOLEAN DEFAULT FALSE,
                    is_spam BOOLEAN DEFAULT FALSE,
                    duplicate_check_hash VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS monitoring_stats (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    videos_found INTEGER DEFAULT 0,
                    auto_posted INTEGER DEFAULT 0,
                    manual_posted INTEGER DEFAULT 0,
                    api_calls INTEGER DEFAULT 0,
                    spam_filtered INTEGER DEFAULT 0,
                    duplicates_filtered INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS channel_whitelist (
                    id SERIAL PRIMARY KEY,
                    channel_id VARCHAR(50) UNIQUE NOT NULL,
                    channel_name VARCHAR(255) NOT NULL,
                    priority_boost INTEGER DEFAULT 0,
                    is_verified BOOLEAN DEFAULT FALSE,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS content_filters (
                    id SERIAL PRIMARY KEY,
                    filter_type VARCHAR(20) NOT NULL, -- 'include' or 'exclude'
                    keyword VARCHAR(255) NOT NULL,
                    category VARCHAR(50),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_published_at ON videos(published_at DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_priority ON videos(priority DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_category ON videos(category)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_pending ON videos(sent_to_telegram, priority)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id)')
            
            # Insert default official channels
            for channel_handle, data in self.official_channels.items():
                cursor.execute('''
                    INSERT INTO channel_whitelist (channel_id, channel_name, priority_boost, is_verified)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (channel_id) DO UPDATE SET
                    channel_name = EXCLUDED.channel_name,
                    priority_boost = EXCLUDED.priority_boost
                ''', (channel_handle, data['name'], data['priority_boost'], True))
            
            conn.commit()
            conn.close()
            logger.info("PostgreSQL database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    def get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(CONFIG['DATABASE_URL'])

    def init_youtube_api(self):
        """Initialize YouTube API with better error handling"""
        if CONFIG['YOUTUBE_API_KEY']:
            try:
                self.youtube = build('youtube', 'v3', developerKey=CONFIG['YOUTUBE_API_KEY'])
                logger.info("YouTube API initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize YouTube API: {e}")
                raise

    def is_spam_content(self, title, description="", channel_name=""):
        """Enhanced spam detection"""
        text = f"{title.lower()} {description.lower()} {channel_name.lower()}"
        
        # Check for spam keywords
        for spam_word in self.spam_keywords:
            if spam_word in text:
                return True
        
        # Check for excessive special characters or numbers (clickbait)
        special_char_ratio = sum(1 for c in title if not c.isalnum() and not c.isspace()) / len(title)
        if special_char_ratio > 0.3:
            return True
        
        # Check for excessive uppercase (clickbait)
        uppercase_ratio = sum(1 for c in title if c.isupper()) / len(title)
        if uppercase_ratio > 0.6:
            return True
        
        return False

    def is_official_source(self, channel_id, channel_name):
        """Check if the channel is an official/verified source"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT is_verified, priority_boost 
                FROM channel_whitelist 
                WHERE channel_id = %s OR LOWER(channel_name) LIKE LOWER(%s)
            ''', (channel_id, f'%{channel_name}%'))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return result[0], result[1]  # is_verified, priority_boost
            
            # Check if channel name contains official keywords
            official_indicators = ['official', 'music', 'entertainment', 'studios', 'productions']
            for indicator in official_indicators:
                if indicator in channel_name.lower():
                    return True, 1
            
            return False, 0
            
        except Exception as e:
            logger.error(f"Error checking official source: {e}")
            return False, 0

    def calculate_content_quality_score(self, video_data):
        """Calculate content quality score based on multiple factors"""
        score = 0
        
        # View count factor (normalized)
        view_count = video_data.get('view_count', 0)
        if view_count > 1000000:  # 1M+
            score += 3
        elif view_count > 100000:  # 100K+
            score += 2
        elif view_count > 10000:  # 10K+
            score += 1
        
        # Engagement ratio
        like_count = video_data.get('like_count', 0)
        if view_count > 0 and like_count > 0:
            engagement_ratio = like_count / view_count
            if engagement_ratio > 0.05:  # 5%+ like ratio
                score += 2
            elif engagement_ratio > 0.02:  # 2%+ like ratio
                score += 1
        
        # Recency boost
        pub_time = datetime.fromisoformat(video_data['published_at'].replace('Z', '+00:00'))
        time_diff = datetime.now() - pub_time.replace(tzinfo=None)
        
        if time_diff.total_seconds() < 3600:  # Within 1 hour
            score += 3
        elif time_diff.total_seconds() < 21600:  # Within 6 hours
            score += 2
        elif time_diff.total_seconds() < 86400:  # Within 24 hours
            score += 1
        
        # Official source boost
        if video_data.get('is_official_source', False):
            score += 2
        
        # Title quality (contains proper movie names, etc.)
        title_lower = video_data['title'].lower()
        quality_indicators = ['official', 'trailer', 'teaser', 'lyrical', 'video song']
        for indicator in quality_indicators:
            if indicator in title_lower:
                score += 1
                break
        
        return min(score, 10)  # Cap at 10

    def categorize_content(self, title, description="", channel_name=""):
        """Enhanced content categorization"""
        title_lower = title.lower()
        desc_lower = description.lower()
        text = f"{title_lower} {desc_lower} {channel_name.lower()}"
        
        # Check categories in priority order
        for category, data in self.content_categories.items():
            for keyword in data['keywords']:
                if keyword in text:
                    return category, data['priority'], data['auto_post']
        
        return 'other', self.content_categories['other']['priority'], False

    def generate_duplicate_hash(self, title, channel_id):
        """Generate hash for duplicate detection"""
        # Normalize title for better duplicate detection
        normalized_title = re.sub(r'[^\w\s]', '', title.lower())
        normalized_title = ' '.join(normalized_title.split())
        
        hash_input = f"{normalized_title}_{channel_id}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def search_telugu_content(self, max_results=50):
        """Enhanced Telugu content search with better filtering"""
        if not self.youtube:
            logger.error("YouTube API not initialized")
            return []

        all_videos = []
        
        # Calculate time range (last 2 days for admin display)
        published_after = (datetime.now() - timedelta(days=CONFIG['CONTENT_AGE_LIMIT_DAYS'])).isoformat() + 'Z'
        
        # Use fewer, more targeted keywords to save quota
        priority_keywords = self.telugu_keywords[:8]  # Top 8 keywords
        
        for keyword in priority_keywords:
            try:
                # Search for videos with enhanced parameters
                search_response = self.youtube.search().list(
                    q=keyword,
                    part='id,snippet',
                    type='video',
                    publishedAfter=published_after,
                    maxResults=max_results // len(priority_keywords),
                    order='relevance',
                    regionCode='IN',
                    relevanceLanguage='te',
                    videoDuration='medium',  # Exclude very short clips
                    videoDefinition='any'
                ).execute()
                
                self.api_quota_used += 100
                
                for item in search_response['items']:
                    video_data = self.process_video_item(item)
                    if video_data:  # Only add if not filtered out
                        all_videos.append(video_data)
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error searching for keyword '{keyword}': {e}")
                continue

        # Remove duplicates and apply final filtering
        unique_videos = self.filter_and_deduplicate_videos(all_videos)
        
        # Sort by quality score and priority
        sorted_videos = sorted(
            unique_videos, 
            key=lambda x: (x['content_quality_score'], x['priority']), 
            reverse=True
        )
        
        return sorted_videos

    def process_video_item(self, item):
        """Process individual video item with enhanced filtering"""
        try:
            video_id = item['id']['videoId']
            snippet = item['snippet']
            
            # Basic video data
            video_data = {
                'id': video_id,
                'title': snippet['title'],
                'channel': snippet['channelTitle'],
                'channel_id': snippet['channelId'],
                'published_at': snippet['publishedAt'],
                'description': snippet['description'],
                'thumbnail': snippet['thumbnails'].get('medium', {}).get('url', ''),
                'video_url': f'https://youtube.com/watch?v={video_id}'
            }
            
            # Spam filter
            if self.is_spam_content(video_data['title'], video_data['description'], video_data['channel']):
                logger.info(f"Filtered spam content: {video_data['title']}")
                return None
            
            # Get video statistics
            video_stats = self.youtube.videos().list(
                part='statistics',
                id=video_id
            ).execute()
            
            self.api_quota_used += 1
            
            if video_stats['items']:
                stats = video_stats['items'][0]['statistics']
                video_data.update({
                    'view_count': int(stats.get('viewCount', 0)),
                    'like_count': int(stats.get('likeCount', 0)),
                    'comment_count': int(stats.get('commentCount', 0))
                })
            
            # Check if official source
            is_official, priority_boost = self.is_official_source(
                video_data['channel_id'], 
                video_data['channel']
            )
            video_data['is_official_source'] = is_official
            
            # Categorize content
            category, base_priority, auto_post = self.categorize_content(
                video_data['title'], 
                video_data['description'],
                video_data['channel']
            )
            
            # Calculate final priority with boosts
            final_priority = min(5, base_priority + priority_boost)
            
            video_data.update({
                'category': category,
                'priority': final_priority,
                'auto_post': auto_post and final_priority >= CONFIG['AUTO_POST_THRESHOLD'],
                'content_quality_score': self.calculate_content_quality_score(video_data),
                'duplicate_check_hash': self.generate_duplicate_hash(video_data['title'], video_data['channel_id'])
            })
            
            return video_data
            
        except Exception as e:
            logger.error(f"Error processing video item: {e}")
            return None

    def filter_and_deduplicate_videos(self, videos):
        """Filter and deduplicate videos"""
        seen_hashes = set()
        unique_videos = []
        
        for video in videos:
            hash_key = video['duplicate_check_hash']
            if hash_key not in seen_hashes:
                seen_hashes.add(hash_key)
                unique_videos.append(video)
        
        return unique_videos

    def save_videos_to_db(self, videos):
        """Save videos to PostgreSQL database"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            new_videos = []
            
            for video in videos:
                # Check if video already exists
                cursor.execute('SELECT id FROM videos WHERE id = %s', (video['id'],))
                if not cursor.fetchone():
                    cursor.execute('''
                        INSERT INTO videos 
                        (id, title, channel, channel_id, published_at, description, thumbnail, video_url,
                         view_count, like_count, comment_count, category, priority, is_official_source,
                         content_quality_score, duplicate_check_hash)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        video['id'], video['title'], video['channel'], video['channel_id'],
                        video['published_at'], video['description'], video['thumbnail'], video['video_url'],
                        video.get('view_count', 0), video.get('like_count', 0), video.get('comment_count', 0),
                        video['category'], video['priority'], video['is_official_source'],
                        video['content_quality_score'], video['duplicate_check_hash']
                    ))
                    new_videos.append(video)
            
            conn.commit()
            conn.close()
            
            return new_videos
            
        except Exception as e:
            logger.error(f"Error saving videos to database: {e}")
            return []

    def format_telegram_message(self, video):
        """Enhanced Telegram message formatting"""
        category_emojis = {
            'official_trailer': '🎬',
            'teaser': '📽️',
            'lyrical_video': '🎵',
            'audio_launch': '🎼',
            'movie_update': '📢',
            'box_office': '💰',
            'review': '⭐',
            'interview': '🎤',
            'behind_scenes': '🎭',
            'other': '📺'
        }
        
        emoji = category_emojis.get(video['category'], '📺')
        priority_stars = '⭐' * video['priority']
        
        # Format published time
        pub_time = datetime.fromisoformat(video['published_at'].replace('Z', '+00:00'))
        time_ago = datetime.now() - pub_time.replace(tzinfo=None)
        
        if time_ago.total_seconds() < 3600:
            time_str = f"{int(time_ago.total_seconds() // 60)} minutes ago"
        elif time_ago.total_seconds() < 86400:
            time_str = f"{int(time_ago.total_seconds() // 3600)} hours ago"
        else:
            time_str = f"{int(time_ago.days)} days ago"
        
        # Quality indicators
        quality_indicators = []
        if video.get('is_official_source'):
            quality_indicators.append('✅ Official')
        if video.get('content_quality_score', 0) >= 7:
            quality_indicators.append('🔥 Trending')
        
        quality_text = ' | '.join(quality_indicators)
        if quality_text:
            quality_text = f"\n{quality_text}"
        
        message = f"""
{emoji} *{video['category'].replace('_', ' ').upper()}* | Telugu Cinema
{priority_stars} Priority: {video['priority']}/5{quality_text}

🎭 *{video['title']}*

📺 Channel: {video['channel']}
👀 Views: {video.get('view_count', 0):,}
👍 Likes: {video.get('like_count', 0):,}
💬 Comments: {video.get('comment_count', 0):,}
⏰ {time_str}

🔗 [Watch Now]({video.get('video_url', f'https://youtube.com/watch?v={video["id"]}')})

#TeluguCinema #{video['category'].replace('_', '').capitalize()} #Tollywood #Priority{video['priority']}
        """.strip()
        
        return message

    def send_to_telegram(self, video, is_auto=True):
        """Enhanced Telegram posting with better error handling"""
        if not CONFIG['TELEGRAM_BOT_TOKEN'] or not CONFIG['TELEGRAM_CHANNEL_ID']:
            logger.warning("Telegram credentials not configured")
            return False
        
        try:
            message = self.format_telegram_message(video)
            
            url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
            
            payload = {
                'chat_id': CONFIG['TELEGRAM_CHANNEL_ID'],
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False,
                'disable_notification': not is_auto  # Silent for manual posts
            }
            
            response = requests.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                post_type = "automatically" if is_auto else "manually"
                logger.info(f"Successfully sent video to Telegram ({post_type}): {video['title']}")
                
                # Update database
                conn = self.get_db_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE videos 
                    SET sent_to_telegram = TRUE, auto_posted = %s, admin_approved = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
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
        """Enhanced monitoring cycle with better error handling"""
        logger.info("Starting enhanced monitoring cycle...")
        
        try:
            # Reset daily quota if new day
            today = datetime.now().date()
            if hasattr(self, 'last_quota_reset') and self.last_quota_reset != today:
                self.api_quota_used = 0
            self.last_quota_reset = today
            
            # Check quota limit
            if self.api_quota_used >= CONFIG['MAX_DAILY_QUOTA']:
                logger.warning("Daily API quota exceeded, skipping cycle")
                return
            
            # Search for new videos
            videos = self.search_telugu_content()
            logger.info(f"Found {len(videos)} videos")
            
            # Save to database and get new ones
            new_videos = self.save_videos_to_db(videos)
            logger.info(f"Found {len(new_videos)} new videos")
            
            # Process new videos
            auto_sent_count = 0
            pending_manual_count = 0
            spam_filtered = 0
            
            for video in new_videos:
                # Skip if spam (additional check)
                if video.get('is_spam', False):
                    spam_filtered += 1
                    continue
                
                # Auto-post high priority videos
                if (video['priority'] >= CONFIG['AUTO_POST_THRESHOLD'] and 
                    video.get('auto_post', False)):
                    if self.send_to_telegram(video, is_auto=True):
                        auto_sent_count += 1
                    time.sleep(2)  # Rate limiting
                else:
                    pending_manual_count += 1
            
            # Update monitoring stats
            self.update_monitoring_stats(
                len(videos), auto_sent_count, 0, spam_filtered, len(videos) - len(new_videos)
            )
            
            logger.info(f"Monitoring cycle completed. Auto-sent: {auto_sent_count}, "
                       f"Pending manual: {pending_manual_count}, Spam filtered: {spam_filtered}")
            
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")

    def update_monitoring_stats(self, videos_found, auto_posted, manual_posted, spam_filtered, duplicates_filtered):
        """Update monitoring statistics in PostgreSQL"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            today = datetime.now().date()
            
            # Update or insert today's stats
            cursor.execute('''
                INSERT INTO monitoring_stats 
                (date, videos_found, auto_posted, manual_posted, api_calls, spam_filtered, duplicates_filtered)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                videos_found = monitoring_stats.videos_found + EXCLUDED.videos_found,
                auto_posted = monitoring_stats.auto_posted + EXCLUDED.auto_posted,
                manual_posted = monitoring_stats.manual_posted + EXCLUDED.manual_posted,
                api_calls = EXCLUDED.api_calls,
                spam_filtered = monitoring_stats.spam_filtered + EXCLUDED.spam_filtered,
                duplicates_filtered = monitoring_stats.duplicates_filtered + EXCLUDED.duplicates_filtered
            ''', (today, videos_found, auto_posted, manual_posted, self.api_quota_used, spam_filtered, duplicates_filtered))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error updating monitoring stats: {e}")

    def get_pending_videos(self, limit=20):
        """Get videos pending manual approval (only from last 2 days)"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Only show content from last 2 days
            cutoff_date = datetime.now() - timedelta(days=CONFIG['CONTENT_AGE_LIMIT_DAYS'])
            
            cursor.execute('''
                SELECT * FROM videos 
                WHERE sent_to_telegram = FALSE 
                AND priority < %s 
                AND published_at >= %s
                AND is_spam = FALSE
                ORDER BY priority DESC, content_quality_score DESC, published_at DESC
                LIMIT %s
            ''', (CONFIG['AUTO_POST_THRESHOLD'], cutoff_date, limit))
            
            videos = cursor.fetchall()
            conn.close()
            
            return [dict(video) for video in videos]
            
        except Exception as e:
            logger.error(f"Error getting pending videos: {e}")
            return []

    def get_recent_videos(self, limit=30):
        """Get recent videos (only from last 2 days)"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Only show content from last 2 days
            cutoff_date = datetime.now() - timedelta(days=CONFIG['CONTENT_AGE_LIMIT_DAYS'])
            
            cursor.execute('''
                SELECT * FROM videos 
                WHERE published_at >= %s
                ORDER BY published_at DESC 
                LIMIT %s
            ''', (cutoff_date, limit))
            
            videos = cursor.fetchall()
            conn.close()
            
            return [dict(video) for video in videos]
            
        except Exception as e:
            logger.error(f"Error getting recent videos: {e}")
            return []

    def approve_and_send_video(self, video_id):
        """Approve and send a video manually"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute('SELECT * FROM videos WHERE id = %s', (video_id,))
            video_data = cursor.fetchone()
            
            if not video_data:
                conn.close()
                return False, "Video not found"
            
            video = dict(video_data)
            
            if self.send_to_telegram(video, is_auto=False):
                # Update manual posted stats
                cursor.execute('''
                    UPDATE monitoring_stats 
                    SET manual_posted = manual_posted + 1 
                    WHERE date = CURRENT_DATE
                ''')
                conn.commit()
                conn.close()
                return True, "Video sent successfully"
            else:
                conn.close()
                return False, "Failed to send video"
                
        except Exception as e:
            logger.error(f"Error approving video: {e}")
            return False, f"Error: {str(e)}"

    def mark_as_spam(self, video_id):
        """Mark video as spam"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE videos 
                SET is_spam = TRUE, updated_at = CURRENT_TIMESTAMP 
                WHERE id = %s
            ''', (video_id,))
            
            conn.commit()
            conn.close()
            
            return True, "Video marked as spam"
            
        except Exception as e:
            logger.error(f"Error marking video as spam: {e}")
            return False, f"Error: {str(e)}"

    def add_channel_to_whitelist(self, channel_id, channel_name, priority_boost=1):
        """Add channel to whitelist"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO channel_whitelist (channel_id, channel_name, priority_boost, is_verified)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (channel_id) DO UPDATE SET
                channel_name = EXCLUDED.channel_name,
                priority_boost = EXCLUDED.priority_boost,
                is_verified = EXCLUDED.is_verified
            ''', (channel_id, channel_name, priority_boost, True))
            
            conn.commit()
            conn.close()
            
            return True, "Channel added to whitelist"
            
        except Exception as e:
            logger.error(f"Error adding channel to whitelist: {e}")
            return False, f"Error: {str(e)}"

    def get_dashboard_data(self):
        """Get comprehensive dashboard data"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Only show content from last 2 days
            cutoff_date = datetime.now() - timedelta(days=CONFIG['CONTENT_AGE_LIMIT_DAYS'])
            
            # Get recent videos
            recent_videos = self.get_recent_videos(20)
            
            # Get pending manual approval videos
            pending_videos = self.get_pending_videos(15)
            
            # Get category statistics (last 2 days)
            cursor.execute('''
                SELECT category, COUNT(*) as count 
                FROM videos 
                WHERE published_at >= %s
                GROUP BY category
                ORDER BY count DESC
            ''', (cutoff_date,))
            category_stats = dict(cursor.fetchall())
            
            # Get priority distribution (last 2 days)
            cursor.execute('''
                SELECT priority, COUNT(*) as count 
                FROM videos 
                WHERE published_at >= %s
                GROUP BY priority
                ORDER BY priority DESC
            ''', (cutoff_date,))
            priority_stats = dict(cursor.fetchall())
            
            # Get posting statistics (today)
            cursor.execute('''
                SELECT 
                    COUNT(CASE WHEN auto_posted = TRUE AND DATE(created_at) = CURRENT_DATE THEN 1 END) as auto_posted,
                    COUNT(CASE WHEN admin_approved = TRUE AND DATE(created_at) = CURRENT_DATE THEN 1 END) as manual_posted,
                    COUNT(CASE WHEN sent_to_telegram = FALSE AND priority < %s AND published_at >= %s THEN 1 END) as pending,
                    COUNT(CASE WHEN is_spam = TRUE AND DATE(created_at) = CURRENT_DATE THEN 1 END) as spam_filtered
                FROM videos
            ''', (CONFIG['AUTO_POST_THRESHOLD'], cutoff_date))
            
            posting_stats = cursor.fetchone()
            
            # Get monitoring stats for last 7 days
            cursor.execute('''
                SELECT * FROM monitoring_stats 
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                ORDER BY date DESC
            ''')
            monitoring_stats = cursor.fetchall()
            
            # Get top channels (last 2 days)
            cursor.execute('''
                SELECT channel, COUNT(*) as video_count, 
                       ROUND(AVG(content_quality_score), 2) as avg_quality,
                       BOOL_OR(is_official_source) as has_official
                FROM videos 
                WHERE published_at >= %s
                GROUP BY channel
                HAVING COUNT(*) > 1
                ORDER BY video_count DESC, avg_quality DESC
                LIMIT 10
            ''', (cutoff_date,))
            top_channels = cursor.fetchall()
            
            # Get quality distribution
            cursor.execute('''
                SELECT 
                    CASE 
                        WHEN content_quality_score >= 8 THEN 'High (8-10)'
                        WHEN content_quality_score >= 5 THEN 'Medium (5-7)'
                        ELSE 'Low (0-4)'
                    END as quality_range,
                    COUNT(*) as count
                FROM videos 
                WHERE published_at >= %s
                GROUP BY quality_range
            ''', (cutoff_date,))
            quality_distribution = dict(cursor.fetchall())
            
            conn.close()
            
            return {
                'recent_videos': recent_videos,
                'pending_videos': pending_videos,
                'category_stats': category_stats,
                'priority_stats': priority_stats,
                'posting_stats': {
                    'auto_posted': posting_stats['auto_posted'] if posting_stats else 0,
                    'manual_posted': posting_stats['manual_posted'] if posting_stats else 0,
                    'pending': posting_stats['pending'] if posting_stats else 0,
                    'spam_filtered': posting_stats['spam_filtered'] if posting_stats else 0
                },
                'monitoring_stats': [dict(stat) for stat in monitoring_stats],
                'top_channels': [dict(channel) for channel in top_channels],
                'quality_distribution': quality_distribution,
                'api_quota_used': self.api_quota_used,
                'max_daily_quota': CONFIG['MAX_DAILY_QUOTA'],
                'auto_post_threshold': CONFIG['AUTO_POST_THRESHOLD'],
                'content_age_limit_days': CONFIG['CONTENT_AGE_LIMIT_DAYS']
            }
            
        except Exception as e:
            logger.error(f"Error getting dashboard data: {e}")
            return {
                'recent_videos': [],
                'pending_videos': [],
                'category_stats': {},
                'priority_stats': {},
                'posting_stats': {'auto_posted': 0, 'manual_posted': 0, 'pending': 0, 'spam_filtered': 0},
                'monitoring_stats': [],
                'top_channels': [],
                'quality_distribution': {},
                'api_quota_used': self.api_quota_used,
                'max_daily_quota': CONFIG['MAX_DAILY_QUOTA'],
                'auto_post_threshold': CONFIG['AUTO_POST_THRESHOLD'],
                'content_age_limit_days': CONFIG['CONTENT_AGE_LIMIT_DAYS']
            }

# Initialize enhanced monitor
monitor = EnhancedTeluguCinemaMonitor()

def background_monitor():
    """Enhanced background monitoring thread"""
    logger.info("Starting background monitoring thread...")
    
    while True:
        try:
            # Check if it's a good time to run (avoid peak hours)
            current_hour = datetime.now().hour
            
            # Run monitoring
            monitor.run_monitoring_cycle()
            
            # Adaptive sleep based on time of day
            if 9 <= current_hour <= 21:  # Peak hours - more frequent
                sleep_time = CONFIG['MONITORING_INTERVAL']
            else:  # Off-peak hours - less frequent
                sleep_time = CONFIG['MONITORING_INTERVAL'] * 2
            
            logger.info(f"Next monitoring cycle in {sleep_time} seconds")
            time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"Background monitor error: {e}")
            time.sleep(300)  # Wait 5 minutes on error

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
            session['login_time'] = datetime.now().isoformat()
            return redirect(url_for('dashboard'))
        else:
            return render_template('login.html', error='Invalid credentials')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
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
    
    try:
        data = monitor.get_dashboard_data()
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting dashboard data: {e}")
        return jsonify({'error': 'Failed to load dashboard data'}), 500

@app.route('/api/pending-videos')
def api_pending_videos():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        limit = request.args.get('limit', 20, type=int)
        pending_videos = monitor.get_pending_videos(limit)
        return jsonify({'pending_videos': pending_videos})
    except Exception as e:
        logger.error(f"Error getting pending videos: {e}")
        return jsonify({'error': 'Failed to load pending videos'}), 500

@app.route('/api/approve-video/<video_id>', methods=['POST'])
def api_approve_video(video_id):
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        success, message = monitor.approve_and_send_video(video_id)
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        logger.error(f"Error approving video: {e}")
        return jsonify({'success': False, 'message': 'Internal error'}), 500

@app.route('/api/mark-spam/<video_id>', methods=['POST'])
def api_mark_spam(video_id):
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        success, message = monitor.mark_as_spam(video_id)
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        logger.error(f"Error marking as spam: {e}")
        return jsonify({'success': False, 'message': 'Internal error'}), 500

@app.route('/api/manual-check', methods=['POST'])
def api_manual_check():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        # Check quota
        if monitor.api_quota_used >= CONFIG['MAX_DAILY_QUOTA']:
            return jsonify({
                'success': False, 
                'message': f'Daily API quota ({CONFIG["MAX_DAILY_QUOTA"]}) exceeded'
            })
        
        # Run monitoring cycle in background
        Thread(target=monitor.run_monitoring_cycle).start()
        return jsonify({'success': True, 'message': 'Manual check initiated'})
    except Exception as e:
        logger.error(f"Error in manual check: {e}")
        return jsonify({'success': False, 'message': 'Failed to start manual check'}), 500

@app.route('/api/add-channel', methods=['POST'])
def api_add_channel():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        data = request.json
        channel_id = data.get('channel_id')
        channel_name = data.get('channel_name')
        priority_boost = data.get('priority_boost', 1)
        
        if not channel_id or not channel_name:
            return jsonify({'success': False, 'message': 'Channel ID and name required'})
        
        success, message = monitor.add_channel_to_whitelist(channel_id, channel_name, priority_boost)
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        logger.error(f"Error adding channel: {e}")
        return jsonify({'success': False, 'message': 'Internal error'}), 500

@app.route('/api/config', methods=['GET', 'POST'])
def api_config():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    if request.method == 'POST':
        try:
            data = request.json
            for key, value in data.items():
                if key in CONFIG and key not in ['DATABASE_URL', 'SECRET_KEY']:  # Protect sensitive configs
                    CONFIG[key] = value
            return jsonify({'success': True, 'message': 'Configuration updated'})
        except Exception as e:
            logger.error(f"Error updating config: {e}")
            return jsonify({'success': False, 'message': 'Failed to update configuration'}), 500
    
    # Return safe config values
    safe_config = {
        'youtube_api_key': CONFIG['YOUTUBE_API_KEY'][:10] + '...' if CONFIG['YOUTUBE_API_KEY'] else '',
        'telegram_bot_token': CONFIG['TELEGRAM_BOT_TOKEN'][:10] + '...' if CONFIG['TELEGRAM_BOT_TOKEN'] else '',
        'telegram_channel_id': CONFIG['TELEGRAM_CHANNEL_ID'],
        'monitoring_interval': CONFIG['MONITORING_INTERVAL'],
        'auto_post_threshold': CONFIG['AUTO_POST_THRESHOLD'],
        'content_age_limit_days': CONFIG['CONTENT_AGE_LIMIT_DAYS'],
        'max_daily_quota': CONFIG['MAX_DAILY_QUOTA']
    }
    
    return jsonify(safe_config)

@app.route('/api/stats')
def api_stats():
    if 'logged_in' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    try:
        # Get detailed statistics
        conn = monitor.get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get hourly distribution (last 24 hours)
        cursor.execute('''
            SELECT 
                EXTRACT(HOUR FROM created_at) as hour,
                COUNT(*) as count
            FROM videos 
            WHERE created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY EXTRACT(HOUR FROM created_at)
            ORDER BY hour
        ''')
        hourly_stats = cursor.fetchall()
        
        # Get performance metrics
        cursor.execute('''
            SELECT 
                DATE(date) as date,
                SUM(videos_found) as total_found,
                SUM(auto_posted) as total_auto_posted,
                SUM(manual_posted) as total_manual_posted,
                SUM(spam_filtered) as total_spam_filtered,
                ROUND(AVG(api_calls), 0) as avg_api_calls
            FROM monitoring_stats 
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(date)
            ORDER BY date DESC
        ''')
        performance_stats = cursor.fetchall()
        
        conn.close()
        
        return jsonify({
            'hourly_distribution': [dict(stat) for stat in hourly_stats],
            'performance_metrics': [dict(stat) for stat in performance_stats]
        })
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': 'Failed to load statistics'}), 500

# Health check endpoint for Render
@app.route('/health')
def health_check():
    try:
        # Check database connection
        conn = monitor.get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'api_quota_used': monitor.api_quota_used,
            'monitoring_active': monitoring_thread.is_alive()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("🎬 Enhanced Telugu Cinema Monitoring System Starting...")
    print("📺 Dashboard: http://localhost:5000")
    print("🔐 Login with configured credentials")
    print("⚙️  Using PostgreSQL on Render")
    print(f"🤖 Auto-posting: Priority {CONFIG['AUTO_POST_THRESHOLD']}/5 and above")
    print(f"📅 Showing content from last {CONFIG['CONTENT_AGE_LIMIT_DAYS']} days")
    print("🎯 Focusing on official sources and quality content")
    print("🛡️  Enhanced spam filtering enabled")
    
    # Use Render port or default to 5000
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)