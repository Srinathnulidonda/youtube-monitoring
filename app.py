import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, session
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from threading import Thread
import time
from googleapiclient.discovery import build

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'telugu_cinema_2025')

# Configuration
CONFIG = {
    'DATABASE_URL': os.getenv('DATABASE_URL'),
    'YOUTUBE_API_KEY': os.getenv('YOUTUBE_API_KEY'),
    'TELEGRAM_BOT_TOKEN': os.getenv('TELEGRAM_BOT_TOKEN'),
    'TELEGRAM_CHANNEL_ID': os.getenv('TELEGRAM_CHANNEL_ID'),
    'ADMIN_PASSWORD': os.getenv('ADMIN_PASSWORD', 'admin123'),
    'MONITORING_INTERVAL': int(os.getenv('MONITORING_INTERVAL', '1800')),  # 30 minutes
    'AUTO_POST_THRESHOLD': int(os.getenv('AUTO_POST_THRESHOLD', '4')),
}

class TeluguCinemaMonitor:
    def __init__(self):
        self.youtube = build('youtube', 'v3', developerKey=CONFIG['YOUTUBE_API_KEY']) if CONFIG['YOUTUBE_API_KEY'] else None
        self.init_database()
        
        # Official Telugu Cinema Channels (verified/popular channels only)
        self.official_channels = {
            'UC_x5XG1OV2P6uZZ5FSM9Ttw': 'Hombale Films',  # KGF, Salaar
            'UCjvgGbPPn-FgYeguc5nxG4A': 'Mythri Movie Makers',
            'UC-BUw1VrHKdKOZgp1Rqmriw': 'Sri Venkateswara Creations',
            'UCZZeT5u8GqR8HbQYWhjBXhw': 'Geetha Arts',
            'UCzf38Rf8FY_DaT8wLZJa4-g': 'UV Creations',
            'UC9EI99s4Zr_4zxHu7Qk6d9w': 'Red Giant Movies',
            'UCq8LlzB3x7pNB5WD1U9WxnQ': 'T-Series Telugu',
            'UC3tNpTOHsTnkmbwztCs30sA': 'Sony Music South',
            'UCr6HBKTtBMd7LdaAOTN1xgw': 'Saregama Telugu',
            'UCLwmGpYbpzDwelJNwFfT-Fg': 'AHA Video',
            'UCNjjYDKQ1xOoF94-rttBrng': 'Aha Telugu',
        }
        
        # Content categories with enhanced priority system
        self.categories = {
            'trailer': {'keywords': ['trailer', 'official trailer'], 'priority': 5},
            'teaser': {'keywords': ['teaser', 'first look'], 'priority': 5},
            'song': {'keywords': ['song', 'lyrical', 'video song'], 'priority': 4},
            'news': {'keywords': ['news', 'breaking', 'announcement'], 'priority': 3},
            'interview': {'keywords': ['interview', 'exclusive'], 'priority': 2},
        }

    def init_database(self):
        """Initialize PostgreSQL database"""
        conn = psycopg2.connect(CONFIG['DATABASE_URL'])
        cur = conn.cursor()
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                id VARCHAR(20) PRIMARY KEY,
                title TEXT NOT NULL,
                channel_id VARCHAR(50) NOT NULL,
                channel_name VARCHAR(100) NOT NULL,
                published_at TIMESTAMP NOT NULL,
                thumbnail TEXT,
                view_count BIGINT DEFAULT 0,
                category VARCHAR(20) NOT NULL,
                priority INTEGER NOT NULL,
                sent_to_telegram BOOLEAN DEFAULT FALSE,
                auto_posted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS monitoring_stats (
                id SERIAL PRIMARY KEY,
                date DATE DEFAULT CURRENT_DATE,
                videos_found INTEGER DEFAULT 0,
                auto_posted INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Create indexes for better performance
        cur.execute('CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_videos_priority ON videos(priority DESC)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_videos_sent ON videos(sent_to_telegram)')
        
        conn.commit()
        conn.close()

    def categorize_content(self, title):
        """Categorize content and assign priority"""
        title_lower = title.lower()
        
        for category, data in self.categories.items():
            if any(keyword in title_lower for keyword in data['keywords']):
                return category, data['priority']
        
        return 'other', 1

    def get_channel_videos(self, channel_id, max_results=10):
        """Get recent videos from a specific channel"""
        if not self.youtube:
            return []
        
        try:
            # Get videos from last 24 hours
            published_after = (datetime.utcnow() - timedelta(days=1)).isoformat() + 'Z'
            
            search_response = self.youtube.search().list(
                channelId=channel_id,
                part='id,snippet',
                type='video',
                order='date',
                publishedAfter=published_after,
                maxResults=max_results
            ).execute()
            
            videos = []
            video_ids = [item['id']['videoId'] for item in search_response['items']]
            
            if video_ids:
                # Get video statistics
                stats_response = self.youtube.videos().list(
                    part='statistics',
                    id=','.join(video_ids)
                ).execute()
                
                stats_dict = {item['id']: item['statistics'] for item in stats_response['items']}
                
                for item in search_response['items']:
                    video_id = item['id']['videoId']
                    snippet = item['snippet']
                    stats = stats_dict.get(video_id, {})
                    
                    category, priority = self.categorize_content(snippet['title'])
                    
                    # Boost priority for official channels
                    if priority >= 3:  # Only boost meaningful content
                        priority = min(5, priority + 1)
                    
                    video_data = {
                        'id': video_id,
                        'title': snippet['title'],
                        'channel_id': channel_id,
                        'channel_name': snippet['channelTitle'],
                        'published_at': snippet['publishedAt'],
                        'thumbnail': snippet['thumbnails'].get('medium', {}).get('url', ''),
                        'view_count': int(stats.get('viewCount', 0)),
                        'category': category,
                        'priority': priority
                    }
                    videos.append(video_data)
            
            return videos
            
        except Exception as e:
            logger.error(f"Error fetching videos for channel {channel_id}: {e}")
            return []

    def monitor_channels(self):
        """Monitor all official channels for new content"""
        all_videos = []
        
        for channel_id, channel_name in self.official_channels.items():
            videos = self.get_channel_videos(channel_id)
            all_videos.extend(videos)
            time.sleep(1)  # Rate limiting
        
        return sorted(all_videos, key=lambda x: x['priority'], reverse=True)

    def save_new_videos(self, videos):
        """Save new videos to database"""
        conn = psycopg2.connect(CONFIG['DATABASE_URL'])
        cur = conn.cursor()
        
        new_videos = []
        
        for video in videos:
            # Check if video exists
            cur.execute('SELECT id FROM videos WHERE id = %s', (video['id'],))
            if not cur.fetchone():
                cur.execute('''
                    INSERT INTO videos 
                    (id, title, channel_id, channel_name, published_at, thumbnail, 
                     view_count, category, priority)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    video['id'], video['title'], video['channel_id'], 
                    video['channel_name'], video['published_at'], video['thumbnail'],
                    video['view_count'], video['category'], video['priority']
                ))
                new_videos.append(video)
        
        conn.commit()
        conn.close()
        return new_videos

    def format_telegram_message(self, video):
        """Format video for Telegram"""
        emojis = {'trailer': '🎬', 'teaser': '📽️', 'song': '🎵', 'news': '📰', 'interview': '🎤'}
        emoji = emojis.get(video['category'], '📺')
        stars = '⭐' * video['priority']
        
        return f"""
{emoji} *{video['category'].upper()}* | Telugu Cinema
{stars} Priority: {video['priority']}/5

🎭 *{video['title']}*

📺 {video['channel_name']}
👀 Views: {video['view_count']:,}

🔗 [Watch Now](https://youtube.com/watch?v={video['id']})

#TeluguCinema #{video['category'].capitalize()} #Tollywood
        """.strip()

    def send_to_telegram(self, video):
        """Send video to Telegram channel"""
        if not CONFIG['TELEGRAM_BOT_TOKEN'] or not CONFIG['TELEGRAM_CHANNEL_ID']:
            return False
        
        url = f"https://api.telegram.org/bot{CONFIG['TELEGRAM_BOT_TOKEN']}/sendMessage"
        message = self.format_telegram_message(video)
        
        payload = {
            'chat_id': CONFIG['TELEGRAM_CHANNEL_ID'],
            'text': message,
            'parse_mode': 'Markdown',
            'disable_web_page_preview': False
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                # Mark as sent
                conn = psycopg2.connect(CONFIG['DATABASE_URL'])
                cur = conn.cursor()
                cur.execute('''
                    UPDATE videos 
                    SET sent_to_telegram = TRUE, auto_posted = TRUE 
                    WHERE id = %s
                ''', (video['id'],))
                conn.commit()
                conn.close()
                
                logger.info(f"Sent to Telegram: {video['title']}")
                return True
            return False
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False

    def run_monitoring_cycle(self):
        """Run complete monitoring cycle"""
        logger.info("🔍 Starting monitoring cycle...")
        
        try:
            # Monitor all channels
            videos = self.monitor_channels()
            new_videos = self.save_new_videos(videos)
            
            # Auto-post high priority videos
            auto_sent = 0
            for video in new_videos:
                if video['priority'] >= CONFIG['AUTO_POST_THRESHOLD']:
                    if self.send_to_telegram(video):
                        auto_sent += 1
                    time.sleep(3)  # Rate limiting
            
            # Update stats
            self.update_stats(len(new_videos), auto_sent)
            
            logger.info(f"✅ Cycle complete: {len(new_videos)} new, {auto_sent} auto-posted")
            
        except Exception as e:
            logger.error(f"❌ Monitoring error: {e}")

    def update_stats(self, videos_found, auto_posted):
        """Update monitoring statistics"""
        conn = psycopg2.connect(CONFIG['DATABASE_URL'])
        cur = conn.cursor()
        
        cur.execute('''
            INSERT INTO monitoring_stats (videos_found, auto_posted)
            VALUES (%s, %s)
            ON CONFLICT (date) DO UPDATE SET
            videos_found = monitoring_stats.videos_found + %s,
            auto_posted = monitoring_stats.auto_posted + %s
        ''', (videos_found, auto_posted, videos_found, auto_posted))
        
        conn.commit()
        conn.close()

    def get_dashboard_data(self):
        """Get dashboard data"""
        conn = psycopg2.connect(CONFIG['DATABASE_URL'], cursor_factory=RealDictCursor)
        cur = conn.cursor()
        
        # Recent videos
        cur.execute('''
            SELECT * FROM videos 
            ORDER BY published_at DESC 
            LIMIT 20
        ''')
        recent_videos = cur.fetchall()
        
        # Pending approval (low priority videos)
        cur.execute('''
            SELECT * FROM videos 
            WHERE sent_to_telegram = FALSE AND priority < %s
            ORDER BY priority DESC, published_at DESC
            LIMIT 10
        ''', (CONFIG['AUTO_POST_THRESHOLD'],))
        pending_videos = cur.fetchall()
        
        # Today's stats
        cur.execute('''
            SELECT 
                COUNT(*) as total_today,
                COUNT(CASE WHEN auto_posted = TRUE THEN 1 END) as auto_posted,
                COUNT(CASE WHEN sent_to_telegram = FALSE AND priority < %s THEN 1 END) as pending
            FROM videos 
            WHERE DATE(created_at) = CURRENT_DATE
        ''', (CONFIG['AUTO_POST_THRESHOLD'],))
        stats = cur.fetchone()
        
        conn.close()
        
        return {
            'recent_videos': recent_videos,
            'pending_videos': pending_videos,
            'stats': dict(stats) if stats else {'total_today': 0, 'auto_posted': 0, 'pending': 0}
        }

# Initialize monitor
monitor = TeluguCinemaMonitor()

def background_monitor():
    """Background monitoring thread"""
    while True:
        try:
            monitor.run_monitoring_cycle()
            time.sleep(CONFIG['MONITORING_INTERVAL'])
        except Exception as e:
            logger.error(f"Background monitor error: {e}")
            time.sleep(300)

# Start background monitoring
if CONFIG['YOUTUBE_API_KEY']:
    monitoring_thread = Thread(target=background_monitor, daemon=True)
    monitoring_thread.start()

# Flask Routes
@app.route('/')
def dashboard():
    if 'authenticated' not in session:
        return render_template('login.html')
    return render_template('dashboard.html')

@app.route('/login', methods=['POST'])
def login():
    if request.form.get('password') == CONFIG['ADMIN_PASSWORD']:
        session['authenticated'] = True
        return jsonify({'success': True})
    return jsonify({'success': False})

@app.route('/api/dashboard')
def api_dashboard():
    if 'authenticated' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    return jsonify(monitor.get_dashboard_data())

@app.route('/api/approve/<video_id>', methods=['POST'])
def api_approve(video_id):
    if 'authenticated' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conn = psycopg2.connect(CONFIG['DATABASE_URL'], cursor_factory=RealDictCursor)
    cur = conn.cursor()
    cur.execute('SELECT * FROM videos WHERE id = %s', (video_id,))
    video = cur.fetchone()
    conn.close()
    
    if video and monitor.send_to_telegram(dict(video)):
        return jsonify({'success': True})
    return jsonify({'success': False})

@app.route('/api/manual-check', methods=['POST'])
def api_manual_check():
    if 'authenticated' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    Thread(target=monitor.run_monitoring_cycle).start()
    return jsonify({'success': True, 'message': 'Manual check initiated'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)