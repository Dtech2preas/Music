from flask import Flask, render_template, request, jsonify, send_file, Response, stream_with_context
import json
import os
import yt_dlp
import logging
import requests
from threading import Lock, Thread, Event
from queue import Queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid
import psutil
import math
import glob
import subprocess
from functools import lru_cache
import sqlite3
from contextlib import contextmanager
import re
import threading
import datetime
import gzip
import io
import asyncio
import aiohttp
from bs4 import BeautifulSoup  # ADDED: Import for BeautifulSoup

app = Flask(__name__)
app.config['DOWNLOAD_FOLDER'] = 'downloads'
app.config['DATA_FILE'] = 'data.json'
app.config['SEARCH_API_URL'] = 'http://localhost:5001'
app.config['MAX_CONCURRENT_DOWNLOADS'] = 12
app.config['MIN_CONCURRENT_DOWNLOADS'] = 3
app.config['RESOURCE_CHECK_INTERVAL'] = 30
app.config['COOKIES_FILE'] = 'cookies.txt'
app.config['MAX_FILE_SIZE_MB'] = 8
app.config['DURATION_TOLERANCE'] = 10
app.config['STATUS_FILE'] = 'system_status.json'
app.config['STUCK_DOWNLOAD_TIMEOUT'] = 480
app.config['PROGRESSIVE_SEARCH_DELAY'] = 0.5
app.config['AUTO_DOWNLOAD_GENRE_SONGS'] = True
app.config['GENRE_EXPLORATION_TIMEOUT'] = 45
app.config['GENIUS_ACCESS_TOKEN'] = 'PHP6HsPhZfVagJWR6m61mvhi9LL3gMQ8IbKNsjnX0RdWC595UvhyDgWsBxFJ1fmf'  # New: Genius API token
app.config['LYRICS_EXTRACTOR_URL'] = 'https://free.dtech24.co.za'  # New: HTML extractor URL
app.config['LYRICS_REQUEST_TIMEOUT'] = 60  # New: Lyrics fetch timeout

# Ensure directories exist
os.makedirs(app.config['DOWNLOAD_FOLDER'], exist_ok=True)
os.makedirs('templates', exist_ok=True)

# Thread-safe structures
file_lock = Lock()
download_queue = Queue()
active_downloads = {}
download_complete_events = {}
download_status = {}
user_sessions = {}
download_start_times = {}
search_sessions = {}
genre_exploration_sessions = {}
lyrics_cache = {}  # New: Cache for lyrics
lyrics_cache_lock = Lock()  # New: Lock for lyrics cache

# System monitoring
system_stats = {
    'server_start_time': time.time(),
    'total_search_requests': 0,
    'total_songs_saved': 0,
    'total_downloads_completed': 0,
    'total_downloads_failed': 0,
    'total_bandwidth_used': 0,
    'peak_memory_usage': 0,
    'peak_cpu_usage': 0,
    'system_uptime': 0,
    'last_update': time.time(),
    'total_genre_explorations': 0,
    'total_auto_downloads': 0,
    'total_lyrics_requests': 0,  # New: Track lyrics requests
    'successful_lyrics_fetches': 0,  # New: Track successful lyrics
    'failed_lyrics_fetches': 0  # New: Track failed lyrics
}
stats_lock = Lock()

# Configure logging - MINIMAL LOGGING
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Gzip compression for responses
@app.after_request
def compress_response(response):
    if (request.path.startswith('/api/') or 
        request.path.startswith('/search') or 
        request.path.startswith('/songs')):
        accept_encoding = request.headers.get('Accept-Encoding', '')
        if 'gzip' in accept_encoding and response.status_code == 200:
            if (response.content_length and 
                response.content_length > 512 and 
                'Content-Encoding' not in response.headers):
                
                gzip_buffer = io.BytesIO()
                with gzip.GzipFile(mode='wb', fileobj=gzip_buffer) as gzip_file:
                    gzip_file.write(response.get_data())
                
                response.set_data(gzip_buffer.getvalue())
                response.headers['Content-Encoding'] = 'gzip'
                response.headers['Content-Length'] = len(response.get_data())
    
    return response

class SystemMonitor:
    def __init__(self):
        self.running = True
        self.monitor_thread = Thread(target=self._monitor_system, daemon=True)
        self.monitor_thread.start()
    
    def _monitor_system(self):
        """Continuously monitor system resources"""
        while self.running:
            try:
                # Get system metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                network = psutil.net_io_counters()
                boot_time = psutil.boot_time()
                
                with stats_lock:
                    # Update peak values
                    system_stats['peak_cpu_usage'] = max(system_stats['peak_cpu_usage'], cpu_percent)
                    system_stats['peak_memory_usage'] = max(system_stats['peak_memory_usage'], memory.percent)
                    
                    # Update current values
                    system_stats['current_cpu'] = cpu_percent
                    system_stats['current_memory'] = memory.percent
                    system_stats['current_memory_used_gb'] = memory.used / (1024**3)
                    system_stats['current_memory_total_gb'] = memory.total / (1024**3)
                    system_stats['disk_usage_percent'] = disk.percent
                    system_stats['disk_used_gb'] = disk.used / (1024**3)
                    system_stats['disk_total_gb'] = disk.total / (1024**3)
                    system_stats['system_uptime'] = time.time() - boot_time
                    system_stats['server_uptime'] = time.time() - system_stats['server_start_time']
                    system_stats['network_sent_mb'] = network.bytes_sent / (1024**2)
                    system_stats['network_recv_mb'] = network.bytes_recv / (1024**2)
                    system_stats['last_update'] = time.time()
                    
                    # Calculate structural integrity
                    system_stats['structural_integrity'] = self._calculate_structural_integrity(
                        cpu_percent, memory.percent, disk.percent
                    )
                
                # Save to file every 30 seconds
                if int(time.time()) % 30 == 0:
                    self._save_system_status()
                
                time.sleep(5)
                
            except Exception as e:
                time.sleep(10)
    
    def _calculate_structural_integrity(self, cpu_usage, memory_usage, disk_usage):
        """Calculate structural integrity percentage (0-100%)"""
        weights = {
            'cpu': 0.4,
            'memory': 0.3,
            'disk': 0.2,
            'downloads': 0.1
        }
        
        cpu_score = max(0, 100 - cpu_usage)
        memory_score = max(0, 100 - memory_usage)
        disk_score = max(0, 100 - disk_usage)
        
        download_health = 100
        if download_manager:
            stats = download_manager.get_queue_stats()
            active = stats['active_downloads']
            max_dl = stats['absolute_max']
            if max_dl > 0:
                download_health = max(0, 100 - (active / max_dl) * 50)
        
        integrity = (
            cpu_score * weights['cpu'] +
            memory_score * weights['memory'] +
            disk_score * weights['disk'] +
            download_health * weights['downloads']
        )
        
        return round(integrity, 1)
    
    def _save_system_status(self):
        """Save system status to JSON file"""
        try:
            with stats_lock:
                status_data = system_stats.copy()
                status_data['timestamp'] = datetime.datetime.now().isoformat()
                status_data['timestamp_readable'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                with open(app.config['STATUS_FILE'], 'w') as f:
                    json.dump(status_data, f, indent=2)
        except Exception as e:
            pass
    
    def load_system_status(self):
        """Load system status from JSON file"""
        try:
            if os.path.exists(app.config['STATUS_FILE']):
                with open(app.config['STATUS_FILE'], 'r') as f:
                    data = json.load(f)
                    with stats_lock:
                        system_stats.update({
                            k: v for k, v in data.items() 
                            if k not in ['current_cpu', 'current_memory', 'current_memory_used_gb', 
                                       'current_memory_total_gb', 'disk_usage_percent', 'disk_used_gb',
                                       'disk_total_gb', 'system_uptime', 'server_uptime', 'network_sent_mb',
                                       'network_recv_mb', 'last_update', 'structural_integrity']
                        })
        except:
            pass
    
    def increment_counter(self, counter_name, value=1):
        """Thread-safe counter increment"""
        with stats_lock:
            if counter_name in system_stats:
                system_stats[counter_name] += value
    
    def get_system_stats(self):
        """Get current system statistics"""
        with stats_lock:
            return system_stats.copy()
    
    def stop(self):
        self.running = False

# Initialize system monitor
system_monitor = SystemMonitor()
system_monitor.load_system_status()

class SearchCache:
    def __init__(self, ttl=300):
        self.ttl = ttl
        self.cache = {}
    
    def get(self, query):
        if query in self.cache:
            data, timestamp = self.cache[query]
            if time.time() - timestamp < self.ttl:
                return data
            else:
                del self.cache[query]
        return None
    
    def set(self, query, data):
        self.cache[query] = (data, time.time())

search_cache = SearchCache()

class GenreCache:
    def __init__(self, ttl=1800):
        self.ttl = ttl
        self.cache = {}
    
    def get(self, genre):
        if genre in self.cache:
            data, timestamp = self.cache[genre]
            if time.time() - timestamp < self.ttl:
                return data
            else:
                del self.cache[genre]
        return None
    
    def set(self, genre, data):
        self.cache[genre] = (data, time.time())

genre_cache = GenreCache()

class LyricsCache:
    """NEW: Cache for lyrics to avoid repeated API calls"""
    def __init__(self, ttl=86400):  # 24 hours TTL
        self.ttl = ttl
        self.cache = {}
    
    def get(self, song_name, artist):
        cache_key = f"{song_name.lower()}_{artist.lower()}"
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.ttl:
                return data
            else:
                del self.cache[cache_key]
        return None
    
    def set(self, song_name, artist, data):
        cache_key = f"{song_name.lower()}_{artist.lower()}"
        self.cache[cache_key] = (data, time.time())

lyrics_cache_manager = LyricsCache()

class DurationValidator:
    @staticmethod
    def parse_duration(duration_str):
        """Parse duration string to seconds"""
        if not duration_str:
            return None
            
        if ':' in duration_str and duration_str.count(':') == 1:
            try:
                minutes, seconds = map(int, duration_str.split(':'))
                return minutes * 60 + seconds
            except:
                return None
        elif ':' in duration_str and duration_str.count(':') == 2:
            try:
                hours, minutes, seconds = map(int, duration_str.split(':'))
                return hours * 3600 + minutes * 60 + seconds
            except:
                return None
        else:
            try:
                return int(duration_str)
            except:
                return None
    
    @staticmethod
    def is_duration_match(expected_duration_str, actual_duration_seconds, tolerance=10):
        """Check if actual duration matches expected within tolerance"""
        expected_seconds = DurationValidator.parse_duration(expected_duration_str)
        if not expected_seconds or not actual_duration_seconds:
            return True
        
        difference = abs(expected_seconds - actual_duration_seconds)
        return difference <= tolerance

class LyricsFetcher:
    """Fetcher that uses direct browser simulation + BeautifulSoup for perfect parsing"""
    
    @staticmethod
    async def search_songs_async(query):
        """Search for songs using Genius API"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.genius.com/search?q={query}",
                    headers={'Authorization': f"Bearer {app.config['GENIUS_ACCESS_TOKEN']}"},
                    timeout=30
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return None
        except Exception as e:
            return None
    
    @staticmethod
    def extract_lyrics_perfected(html):
        """Extract clean lyrics using BeautifulSoup"""
        try:
            if not html:
                return "❌ No HTML content retrieved"

            soup = BeautifulSoup(html, 'html.parser')
            
            # STRICT SELECTOR: Only get divs explicitly marked as lyrics containers
            # This filters out 'Contributors', 'Translations', 'Song Bio', etc.
            lyrics_containers = soup.select('div[data-lyrics-container="true"]')
            
            # Fallback if strict selector fails
            if not lyrics_containers:
                lyrics_containers = soup.select('div[class*="Lyrics__Container"]')

            if not lyrics_containers:
                return "❌ Lyrics structure not found"

            final_parts = []
            for container in lyrics_containers:
                # Handle <br> tags for correct newlines
                for br in container.find_all("br"):
                    br.replace_with("\n")
                
                # Get text (strip removes surrounding whitespace)
                text = container.get_text().strip()
                final_parts.append(text)

            # Join with double newlines to separate verses/choruses cleanly
            return "\n\n".join(final_parts).strip()
            
        except Exception as e:
            return f"❌ Error extracting lyrics: {str(e)}"

    @staticmethod
    async def get_lyrics_async(song_name, artist_name):
        """Main async function to get lyrics"""
        try:
            # Check cache first
            cached_lyrics = lyrics_cache_manager.get(song_name, artist_name)
            if cached_lyrics:
                return {
                    'title': song_name,
                    'artist': artist_name,
                    'lyrics': cached_lyrics,
                    'success': True,
                    'cached': True
                }
            
            # 1. Search Genius API
            search_query = f"{song_name} {artist_name}"
            search_results = await LyricsFetcher.search_songs_async(search_query)
            
            if not search_results or not search_results.get('response', {}).get('hits'):
                return {
                    'title': song_name,
                    'artist': artist_name,
                    'lyrics': "❌ No songs found on Genius",
                    'success': False
                }
            
            song = search_results['response']['hits'][0]['result']
            genius_url = song['url']
            
            # 2. Direct Fetch (Browser Simulation)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(genius_url, headers=headers, timeout=15) as response:
                    if response.status != 200:
                        return {
                            'title': song_name,
                            'artist': artist_name,
                            'lyrics': f"❌ Genius blocked connection (Status {response.status})",
                            'success': False
                        }
                    html = await response.text()

            # 3. Parse Lyrics
            lyrics = LyricsFetcher.extract_lyrics_perfected(html)
            
            result = {
                'title': song['title'],
                'artist': song['primary_artist']['name'],
                'url': genius_url,
                'lyrics': lyrics,
                'success': not lyrics.startswith('❌')
            }
            
            # Cache successful results
            if result['success']:
                lyrics_cache_manager.set(song_name, artist_name, lyrics)
                system_monitor.increment_counter('successful_lyrics_fetches')
            else:
                system_monitor.increment_counter('failed_lyrics_fetches')
            
            return result
            
        except Exception as e:
            system_monitor.increment_counter('failed_lyrics_fetches')
            return {
                'title': song_name,
                'artist': artist_name,
                'lyrics': f"❌ System Error: {str(e)}",
                'success': False
            }

class ResourceAwareDownloadManager:
    def __init__(self, max_workers=12, min_workers=3):
        self.max_workers = max_workers
        self.min_workers = min_workers
        self.current_max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = True
        self.thread = Thread(target=self._process_queue, daemon=True)
        self.resource_thread = Thread(target=self._monitor_resources, daemon=True)
        self.cleanup_thread = Thread(target=self._cleanup_stuck_downloads, daemon=True)
        self.thread.start()
        self.resource_thread.start()
        self.cleanup_thread.start()
        self.last_resource_check = 0
        self.duration_validator = DurationValidator()
        self.connection_pool = {}
    
    def _calculate_optimal_workers(self):
        """Calculate optimal number of workers based on system resources"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            available_memory_gb = memory.available / (1024 ** 3)
            
            cpu_workers = max(self.min_workers, 
                            min(self.max_workers, 
                                math.floor((100 - cpu_percent) / 15)))
            
            memory_workers = max(self.min_workers,
                               min(self.max_workers,
                                   math.floor(available_memory_gb / 0.3)))
            
            optimal_workers = min(cpu_workers, memory_workers)
            
            return optimal_workers
            
        except Exception as e:
            return self.min_workers
    
    def _monitor_resources(self):
        """Monitor system resources and adjust worker count every 30 seconds"""
        while self.running:
            try:
                current_time = time.time()
                if current_time - self.last_resource_check >= app.config['RESOURCE_CHECK_INTERVAL']:
                    new_max_workers = self._calculate_optimal_workers()
                    
                    if new_max_workers != self.current_max_workers:
                        self.current_max_workers = new_max_workers
                    
                    self.last_resource_check = current_time
                
                time.sleep(10)
                
            except Exception as e:
                time.sleep(15)
    
    def _cleanup_stuck_downloads(self):
        """Clean up downloads that are stuck for more than 8 minutes"""
        while self.running:
            try:
                current_time = time.time()
                stuck_downloads = []
                
                for download_id, start_time in download_start_times.items():
                    if current_time - start_time > app.config['STUCK_DOWNLOAD_TIMEOUT']:
                        stuck_downloads.append(download_id)
                
                for download_id in stuck_downloads:
                    if download_id in active_downloads:
                        future = active_downloads[download_id]
                        if not future.done():
                            future.cancel()
                    
                    if download_id in download_status:
                        download_status[download_id] = {
                            'status': 'failed', 
                            'message': 'Download stuck and was cleaned up',
                            'user_id': download_status[download_id].get('user_id', 'unknown')
                        }
                    
                    download_start_times.pop(download_id, None)
                    active_downloads.pop(download_id, None)
                    
                    logger.error(f"Cleaned up stuck download: {download_id}")
                
                time.sleep(60)
                
            except Exception as e:
                time.sleep(120)
    
    def _preemptive_queue_processing(self):
        """Pre-emptive processing: prepare next downloads while current ones are running"""
        current_active = len(active_downloads)
        available_slots = min(self.current_max_workers - current_active, 
                            self.max_workers - current_active)
        
        processed = 0
        while available_slots > 0 and not download_queue.empty() and processed < available_slots:
            download_id, song_name, artist, album, user_id, callback, expected_duration = download_queue.get()
            
            future = self.executor.submit(self._download_task, download_id, song_name, artist, album, expected_duration)
            active_downloads[download_id] = future
            download_status[download_id] = {'status': 'downloading', 'progress': 0, 'user_id': user_id}
            download_start_times[download_id] = time.time()
            
            def done_callback(f):
                try:
                    success, message = f.result()
                    download_status[download_id] = {
                        'status': 'completed' if success else 'failed',
                        'message': message,
                        'user_id': user_id
                    }
                    
                    if success:
                        system_monitor.increment_counter('total_downloads_completed')
                    else:
                        system_monitor.increment_counter('total_downloads_failed')
                    
                    if download_id in download_complete_events:
                        download_complete_events[download_id].set()
                    if callback:
                        callback(download_id, success, message)
                    
                except Exception as e:
                    download_status[download_id] = {
                        'status': 'failed', 
                        'message': str(e),
                        'user_id': user_id
                    }
                    system_monitor.increment_counter('total_downloads_failed')
                    if download_id in download_complete_events:
                        download_complete_events[download_id].set()
                finally:
                    active_downloads.pop(download_id, None)
                    download_start_times.pop(download_id, None)
                    if user_id in user_sessions:
                        user_sessions[user_id]['active_downloads'] = [
                            did for did in user_sessions[user_id]['active_downloads'] 
                            if did != download_id
                        ]
            
            future.add_done_callback(done_callback)
            download_queue.task_done()
            
            if user_id not in user_sessions:
                user_sessions[user_id] = {'active_downloads': [], 'completed_downloads': []}
            user_sessions[user_id]['active_downloads'].append(download_id)
            
            processed += 1
            available_slots -= 1
        
        return processed

    def _process_queue(self):
        """Process download queue with resource-aware limits and pre-emptive processing"""
        while self.running:
            try:
                processed = self._preemptive_queue_processing()
                
                if processed == 0:
                    time.sleep(0.1)
                
            except Exception as e:
                time.sleep(0.5)
    
    def _find_downloaded_file(self, expected_filename):
        """Find the actual downloaded file by searching for similar filenames"""
        expected_name_no_ext = os.path.splitext(expected_filename)[0]
        
        pattern = os.path.join(app.config['DOWNLOAD_FOLDER'], f"{expected_name_no_ext}*")
        matching_files = glob.glob(pattern)
        
        if matching_files:
            return matching_files[0]
        
        search_terms = expected_name_no_ext.split()
        for file in os.listdir(app.config['DOWNLOAD_FOLDER']):
            if all(term.lower() in file.lower() for term in search_terms if len(term) > 2):
                return os.path.join(app.config['DOWNLOAD_FOLDER'], file)
        
        return None

    def _cleanup_large_files(self, file_path):
        """Delete files that are larger than the maximum allowed size"""
        try:
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            if file_size > app.config['MAX_FILE_SIZE_MB']:
                os.remove(file_path)
                return False
            return True
        except Exception as e:
            return True

    def _compress_audio(self, file_path):
        """Compress audio file to 48kbps using ffmpeg, fallback to 64kbps"""
        try:
            if not os.path.exists(file_path):
                return False
                
            temp_path = file_path + ".temp.opus"
            
            cmd_48k = [
                'ffmpeg', '-y', '-i', file_path,
                '-c:a', 'libopus', '-b:a', '48k', '-vbr', 'on',
                '-application', 'audio', '-compression_level', '10',
                '-loglevel', 'quiet',
                temp_path
            ]
            
            result = subprocess.run(cmd_48k, capture_output=True, text=True, timeout=300)
            
            if result.returncode != 0 or not os.path.exists(temp_path):
                cmd_64k = [
                    'ffmpeg', '-y', '-i', file_path,
                    '-c:a', 'libopus', '-b:a', '64k', '-vbr', 'on',
                    '-application', 'audio', '-compression_level', '10',
                    '-loglevel', 'quiet',
                    temp_path
                ]
                
                result = subprocess.run(cmd_64k, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0 and os.path.exists(temp_path):
                os.remove(file_path)
                os.rename(temp_path, file_path)
                return True
            else:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return False
                
        except Exception as e:
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
            return False

    def _get_song_duration_from_file(self, file_path):
        """Get duration of audio file using ffprobe"""
        try:
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', file_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                duration = float(data['format']['duration'])
                return int(duration)
        except Exception as e:
            pass
        return None

    def _search_best_match(self, song_name, artist, expected_duration=None):
        """Search for the best matching video based on duration and title"""
        search_query = f"{song_name} {artist}"
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'forcejson': True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                search_results = ydl.extract_info(f"ytsearch10:{search_query}", download=False)
                
                if not search_results or 'entries' not in search_results:
                    return None
                
                entries = [e for e in search_results['entries'] if e]
                
                if expected_duration:
                    expected_seconds = self.duration_validator.parse_duration(expected_duration)
                    if expected_seconds:
                        best_score = -1
                        best_url = None
                        
                        for entry in entries:
                            title = entry.get('title', '').lower()
                            duration = entry.get('duration')
                            
                            title_score = 0
                            song_lower = song_name.lower()
                            artist_lower = artist.lower()
                            
                            if song_lower in title:
                                title_score += 3
                            if artist_lower in title:
                                title_score += 2
                            
                            duration_score = 0
                            if duration and expected_seconds:
                                duration_diff = abs(duration - expected_seconds)
                                if duration_diff <= app.config['DURATION_TOLERANCE']:
                                    duration_score = 5
                                elif duration_diff <= 30:
                                    duration_score = 3
                                elif duration_diff <= 60:
                                    duration_score = 1
                            
                            total_score = title_score + duration_score
                            
                            if total_score > best_score:
                                best_score = total_score
                                best_url = entry['url']
                        
                        if best_url and best_score >= 3:
                            return best_url
                
                return entries[0]['url'] if entries else None
                
        except Exception as e:
            return None

    def _download_task_with_retry(self, download_id, song_name, artist, album=None, expected_duration=None, max_retries=3):
        """Download task with retry mechanism and duration validation"""
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                return self._download_task_impl(download_id, song_name, artist, album, expected_duration)
            except yt_dlp.DownloadError as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count
                    time.sleep(wait_time)
                    continue
                else:
                    return False, f"Download failed after {max_retries} attempts"
            except Exception as e:
                return False, f"Unexpected error: {str(e)}"
        
        return False, "Max retries exceeded"
    
    def _download_task_impl(self, download_id, song_name, artist, album=None, expected_duration=None):
        """Download song using yt-dlp with duration validation - NO METADATA"""
        expected_filename = get_song_filename(song_name, artist)
        existing_file = self._find_downloaded_file(expected_filename)
        if existing_file:
            if expected_duration:
                actual_duration = self._get_song_duration_from_file(existing_file)
                if actual_duration and not self.duration_validator.is_duration_match(
                    expected_duration, actual_duration, app.config['DURATION_TOLERANCE']):
                    os.remove(existing_file)
                else:
                    return True, "Already downloaded"
            else:
                return True, "Already downloaded"
        
        try:
            video_url = self._search_best_match(song_name, artist, expected_duration)
            if not video_url:
                return False, "No suitable video found"
            
            safe_song_name = "".join(c for c in song_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_artist = "".join(c for c in artist if c.isalnum() or c in (' ', '-', '_')).rstrip()
            base_filename = f"{safe_song_name} - {safe_artist}"
            output_template = os.path.join(app.config['DOWNLOAD_FOLDER'], f"{base_filename}.%(ext)s")
            
            def progress_hook(d):
                if d['status'] == 'downloading':
                    if 'total_bytes' in d and d['total_bytes']:
                        percent = int((d['downloaded_bytes'] / d['total_bytes']) * 100)
                        download_status[download_id]['progress'] = percent
                    elif 'total_bytes_estimate' in d and d['total_bytes_estimate']:
                        percent = int((d['downloaded_bytes'] / d['total_bytes_estimate']) * 100)
                        download_status[download_id]['progress'] = percent
                
                if download_id in download_start_times:
                    download_start_times[download_id] = time.time()
            
            ydl_opts = {
                'format': 'bestaudio[ext=m4a][abr<=64]/bestaudio[ext=webm][abr<=64]/bestaudio/best',
                'postprocessors': [
                    {
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'opus',
                        'preferredquality': '48',
                    }
                ],
                'outtmpl': output_template,
                'quiet': True,
                'no_warnings': True,
                'progress_hooks': [progress_hook],
                'extractaudio': True,
                'audioformat': 'opus',
                'prefer_ffmpeg': True,
                'keepvideo': False,
                'writethumbnail': False,
                'embedsubtitles': False,
                'noplaylist': True,
                'extract_flat': False,
                'http_chunk_size': 2097152,
                'buffersize': 65536,
                'continuedl': True,
                
                'cookiefile': app.config['COOKIES_FILE'] if os.path.exists(app.config['COOKIES_FILE']) else None,
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'referer': 'https://www.youtube.com/',
                'socket_timeout': 15,
                'retries': 15,
                'fragment_retries': 15,
                'skip_unavailable_fragments': True,
                'continue_dl': True,
                'no_part': False,
                'ignoreerrors': False,
                'ratelimit': None,
                'throttledratelimit': None,
                'consoletitle': False,
            }
            
            if not os.path.exists(app.config['COOKIES_FILE']):
                ydl_opts.pop('cookiefile', None)
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            
            downloaded_file = self._find_downloaded_file(expected_filename)
            
            if downloaded_file:
                if expected_duration:
                    actual_duration = self._get_song_duration_from_file(downloaded_file)
                    if actual_duration and not self.duration_validator.is_duration_match(
                        expected_duration, actual_duration, app.config['DURATION_TOLERANCE']):
                        
                        os.remove(downloaded_file)
                        return False, f"Duration mismatch: got {actual_duration}s, expected {expected_duration}"
                
                compression_success = self._compress_audio(downloaded_file)
                if compression_success:
                    downloaded_file = self._find_downloaded_file(expected_filename)
                
                if downloaded_file and os.path.exists(downloaded_file):
                    if not self._cleanup_large_files(downloaded_file):
                        return False, "File too large and was deleted"
                    
                    file_size = os.path.getsize(downloaded_file) / (1024 * 1024)
                    
                    system_monitor.increment_counter('total_bandwidth_used', file_size)
                    
                    return True, f"Download successful ({file_size:.2f} MB)"
                else:
                    return False, "File lost during compression"
            else:
                return False, "Download completed but file not found"
            
        except Exception as e:
            error_msg = str(e)
            if "Sign in to confirm you're not a bot" in error_msg:
                return False, "YouTube blocked. Add cookies.txt to bypass."
            elif "Private video" in error_msg or "Video unavailable" in error_msg:
                return False, "Video is unavailable."
            elif "Unable to download webpage" in error_msg:
                return False, "Cannot access YouTube."
            else:
                return False, f"Download failed: {str(e)}"
    
    def _download_task(self, download_id, song_name, artist, album=None, expected_duration=None):
        """Wrapper for download task with retry"""
        return self._download_task_with_retry(download_id, song_name, artist, album, expected_duration)
    
    def submit_download(self, song_name, artist, album=None, user_id="anonymous", callback=None, expected_duration=None):
        """Submit a download with user tracking and duration validation"""
        download_id = str(uuid.uuid4())
        download_queue.put((download_id, song_name, artist, album, user_id, callback, expected_duration))
        download_status[download_id] = {'status': 'queued', 'progress': 0, 'user_id': user_id}
        download_complete_events[download_id] = Event()
        
        return download_id
    
    def get_user_downloads(self, user_id):
        """Get all downloads for a specific user"""
        user_downloads = []
        for download_id, status in download_status.items():
            if status.get('user_id') == user_id:
                user_downloads.append({
                    'download_id': download_id,
                    'status': status['status'],
                    'progress': status.get('progress', 0),
                    'message': status.get('message', '')
                })
        return user_downloads
    
    def get_queue_stats(self):
        """Get detailed queue statistics"""
        return {
            'queue_size': download_queue.qsize(),
            'active_downloads': len(active_downloads),
            'max_concurrent': self.current_max_workers,
            'absolute_max': self.max_workers,
            'user_sessions': len(user_sessions),
            'stuck_downloads': len([did for did, start_time in download_start_times.items() 
                                  if time.time() - start_time > 300])
        }
    
    def stop(self):
        self.running = False
        self.executor.shutdown(wait=False)

# Initialize download manager
try:
    download_manager = ResourceAwareDownloadManager(
        max_workers=app.config['MAX_CONCURRENT_DOWNLOADS'],
        min_workers=app.config['MIN_CONCURRENT_DOWNLOADS']
    )
except Exception as e:
    download_manager = None

def load_songs():
    try:
        with open(app.config['DATA_FILE'], 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_songs(songs):
    with file_lock:
        with open(app.config['DATA_FILE'], 'w') as f:
            json.dump(songs, f, indent=2)

def get_song_filename(song_name, artist):
    safe_name = "".join(c for c in f"{song_name} - {artist}" if c.isalnum() or c in (' ', '-', '_')).rstrip()
    return f"{safe_name}.opus"

def get_video_url_for_streaming(song_name, artist):
    """Get the YouTube Web URL for a song"""
    search_query = f"{song_name} {artist}"
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': True, # Just get metadata, don't analyze formats yet
        'forcejson': True,
        'cookiefile': app.config['COOKIES_FILE'] if os.path.exists(app.config['COOKIES_FILE']) else None,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    if not os.path.exists(app.config['COOKIES_FILE']):
        ydl_opts.pop('cookiefile', None)

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Search for the video
            search_results = ydl.extract_info(f"ytsearch1:{search_query}", download=False)
            if not search_results or 'entries' not in search_results or not search_results['entries']:
                return None

            # Get the first result
            video_info = search_results['entries'][0]
            return video_info.get('url') # This is the web URL (e.g. https://www.youtube.com/watch?v=...)

    except Exception as e:
        logger.error(f"Video search failed: {str(e)}")
        return None

def find_song_file(song_name, artist):
    if download_manager:
        expected_filename = get_song_filename(song_name, artist)
        return download_manager._find_downloaded_file(expected_filename)
    return None

def is_song_downloaded(song_name, artist):
    return find_song_file(song_name, artist) is not None

def get_user_id():
    return request.headers.get('X-User-ID', request.remote_addr)

def search_spotify_api(query, max_results=10):
    try:
        response = requests.get(
            f"{app.config['SEARCH_API_URL']}/api/search",
            params={'query': query, 'max_results': max_results},
            timeout=20
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return {
                    'success': True,
                    'results': data.get('all_tracks', []),
                    'message': data.get('message')
                }
            else:
                return {
                    'success': False,
                    'error': data.get('message', 'Search failed'),
                    'results': []
                }
        else:
            return {
                'success': False,
                'error': f"Search API error: {response.status_code}",
                'results': []
            }
            
    except requests.exceptions.ConnectionError:
        return {
            'success': False,
            'error': 'Cannot connect to search service.',
            'results': []
        }
    except requests.exceptions.Timeout:
        return {
            'success': False,
            'error': 'Search request timed out.',
            'results': []
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Search error: {str(e)}',
            'results': []
        }

def explore_genre_api(genre):
    """Call the FastAPI genre exploration endpoint"""
    try:
        response = requests.get(
            f"{app.config['SEARCH_API_URL']}/api/genre/explore",
            params={'genre': genre},
            timeout=app.config['GENRE_EXPLORATION_TIMEOUT']
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return {
                    'success': True,
                    'genre_data': data,
                    'message': data.get('message', 'Genre exploration successful')
                }
            else:
                return {
                    'success': False,
                    'error': data.get('message', 'Genre exploration failed'),
                    'genre_data': None
                }
        else:
            return {
                'success': False,
                'error': f"Genre API error: {response.status_code}",
                'genre_data': None
            }
            
    except requests.exceptions.ConnectionError:
        return {
            'success': False,
            'error': 'Cannot connect to genre exploration service.',
            'genre_data': None
        }
    except requests.exceptions.Timeout:
        return {
            'success': False,
            'error': 'Genre exploration request timed out.',
            'genre_data': None
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Genre exploration error: {str(e)}',
            'genre_data': None
        }

def auto_download_genre_songs(genre_data, user_id="system"):
    """Automatically download all songs from genre exploration"""
    if not genre_data or not download_manager:
        return 0
    
    downloaded_count = 0
    
    # Download top charts songs
    top_charts = genre_data.get('top_charts', [])
    for chart in top_charts:
        song_name = chart.get('song_name')
        artist = chart.get('artist')
        duration = chart.get('duration')
        
        if song_name and artist:
            # Check if already exists
            if not is_song_downloaded(song_name, artist):
                download_id = download_manager.submit_download(
                    song_name, artist, 'Top Charts', user_id, expected_duration=duration
                )
                downloaded_count += 1
    
    # Download artist songs
    artist_dives = genre_data.get('artist_deep_dives', {})
    for artist_name, artist_data in artist_dives.items():
        top_tracks = artist_data.get('top_tracks', [])
        for track in top_tracks:
            song_name = track.get('song_name')
            artist = track.get('artist')
            duration = track.get('duration')
            
            if song_name and artist:
                if not is_song_downloaded(song_name, artist):
                    download_id = download_manager.submit_download(
                        song_name, artist, f"{artist_name} Collection", user_id, expected_duration=duration
                    )
                    downloaded_count += 1
    
    return downloaded_count

def add_genre_songs_to_library(genre_data):
    """Add all discovered songs from genre exploration to the library"""
    if not genre_data:
        return 0
    
    songs = load_songs()
    existing_songs = {(s['song_name'].lower(), s['artist'].lower()) for s in songs}
    added_count = 0
    
    # Add top charts songs
    top_charts = genre_data.get('top_charts', [])
    for chart in top_charts:
        song_name = chart.get('song_name')
        artist = chart.get('artist')
        duration = chart.get('duration')
        
        if song_name and artist:
            key = (song_name.lower(), artist.lower())
            if key not in existing_songs:
                new_index = max([s['index'] for s in songs], default=0) + 1
                new_song = {
                    'index': new_index,
                    'song_name': song_name,
                    'artist': artist,
                    'album': 'Top Charts',
                    'duration': duration
                }
                songs.append(new_song)
                existing_songs.add(key)
                added_count += 1
    
    # Add artist songs
    artist_dives = genre_data.get('artist_deep_dives', {})
    for artist_name, artist_data in artist_dives.items():
        top_tracks = artist_data.get('top_tracks', [])
        for track in top_tracks:
            song_name = track.get('song_name')
            artist = track.get('artist')
            duration = track.get('duration')
            
            if song_name and artist:
                key = (song_name.lower(), artist.lower())
                if key not in existing_songs:
                    new_index = max([s['index'] for s in songs], default=0) + 1
                    new_song = {
                        'index': new_index,
                        'song_name': song_name,
                        'artist': artist,
                        'album': f"{artist_name} Collection",
                        'duration': duration
                    }
                    songs.append(new_song)
                    existing_songs.add(key)
                    added_count += 1
    
    if added_count > 0:
        save_songs(songs)
        system_monitor.increment_counter('total_songs_saved', added_count)
    
    return added_count

# NEW: Lyrics fetching endpoint
@app.route('/api/lyrics', methods=['GET'])
def get_lyrics():
    """Get lyrics for a specific song"""
    song_name = request.args.get('song')
    artist = request.args.get('artist')
    
    if not song_name or not artist:
        return jsonify({'error': 'Song name and artist are required'}), 400
    
    system_monitor.increment_counter('total_lyrics_requests')
    
    # Run the async lyrics fetch in a thread
    def run_async_lyrics_fetch():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(LyricsFetcher.get_lyrics_async(song_name, artist))
        finally:
            loop.close()
    
    try:
        # Use ThreadPoolExecutor to run the async function
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_async_lyrics_fetch)
            result = future.result(timeout=app.config['LYRICS_REQUEST_TIMEOUT'])
        
        return jsonify(result)
    
    except TimeoutError:
        system_monitor.increment_counter('failed_lyrics_fetches')
        return jsonify({
            'title': song_name,
            'artist': artist,
            'lyrics': '❌ Lyrics request timed out',
            'success': False
        }), 408
    except Exception as e:
        system_monitor.increment_counter('failed_lyrics_fetches')
        return jsonify({
            'title': song_name,
            'artist': artist,
            'lyrics': f'❌ Failed to fetch lyrics: {str(e)}',
            'success': False
        }), 500

# NEW: Clear lyrics cache endpoint
@app.route('/api/lyrics/clear-cache', methods=['POST'])
def clear_lyrics_cache():
    """Clear the lyrics cache"""
    try:
        lyrics_cache_manager.cache.clear()
        return jsonify({
            'success': True,
            'message': 'Lyrics cache cleared successfully'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to clear cache: {str(e)}'
        }), 500

# NEW: Smart Shuffle endpoint
@app.route('/api/smart-shuffle/start', methods=['POST'])
def start_smart_shuffle():
    """Start Smart Shuffle based on the current song/artist"""
    data = request.json
    song_name = data.get('song_name')
    artist = data.get('artist')
    user_id = get_user_id()

    if not artist:
        return jsonify({'error': 'Artist is required for Smart Shuffle'}), 400

    # Use a separate thread to not block the response
    def smart_shuffle_task():
        try:
            # We use the artist name as the 'genre' seed.
            # This works because the underlying search on Spotify
            # will return the artist's top tracks and similar artists/albums
            # when searching for the artist name.
            logger.info(f"Starting Smart Shuffle for artist: {artist}")

            # Using artist + " Mix" might get better results for a vibe,
            # or just the artist name for their top tracks.
            # Let's try appending " Mix" to capture the "vibe" as requested.
            query = f"{artist} Mix"

            result = explore_genre_api(query)

            if result['success']:
                genre_data = result['genre_data']

                # Add songs to library
                add_genre_songs_to_library(genre_data)

                # Auto-download songs - DISABLED for Smart Shuffle (Streaming First)
                # if download_manager:
                #     downloads_triggered = auto_download_genre_songs(genre_data, user_id)
                #     system_monitor.increment_counter('total_auto_downloads', downloads_triggered)
                #     logger.info(f"Smart Shuffle triggered {downloads_triggered} downloads for {query}")
            else:
                logger.error(f"Smart Shuffle exploration failed: {result.get('error')}")

        except Exception as e:
            logger.error(f"Smart Shuffle task error: {str(e)}")

    Thread(target=smart_shuffle_task, daemon=True).start()

    return jsonify({
        'success': True,
        'message': f'Smart Shuffle started for {artist}',
        'vibe': f"{artist} Mix"
    })

# NEW: Streaming Endpoint
@app.route('/stream')
def stream_audio():
    """Stream audio directly from YouTube via server proxy (piping yt-dlp stdout)"""
    song_name = request.args.get('song')
    artist = request.args.get('artist')

    if not song_name:
        return jsonify({'error': 'Song name is required'}), 400

    try:
        # 1. Find the video URL (Web URL)
        video_url = get_video_url_for_streaming(song_name, artist or "")

        if not video_url:
            return jsonify({'error': 'Could not find video'}), 404

        # 2. Pipe yt-dlp output to client
        # This handles HLS/DASH/Formats automatically and outputs linear bytes

        cmd = [
            'yt-dlp',
            '-f', 'bestaudio', # Let yt-dlp pick the absolute best audio
            '-o', '-',         # Output to stdout
            '--quiet',
            '--no-warnings',
            video_url
        ]

        # Add cookies if available
        if os.path.exists(app.config['COOKIES_FILE']):
            cmd.extend(['--cookies', app.config['COOKIES_FILE']])

        logger.info(f"Streaming: {' '.join(cmd)}")

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1024 * 32
        )

        def generate():
            try:
                # Read until process ends
                while True:
                    chunk = process.stdout.read(1024 * 32) # 32KB chunks
                    if not chunk:
                        break
                    yield chunk

                # Check for errors after stream ends
                return_code = process.poll()
                if return_code and return_code != 0:
                    stderr = process.stderr.read().decode()
                    logger.error(f"Stream process failed: {stderr}")

            except Exception as e:
                logger.error(f"Stream generation error: {str(e)}")
                process.terminate()
            finally:
                if process.poll() is None:
                    process.terminate()

        # We assume WebM/Opus or M4A/AAC.
        # Since we don't know exactly what yt-dlp picks without probing,
        # 'audio/mpeg' or 'application/octet-stream' is a safe bet for browsers to sniff.
        # But 'audio/webm' is the most likely output of 'bestaudio'.
        return Response(stream_with_context(generate()),
                      content_type='audio/webm')

    except Exception as e:
        logger.error(f"Streaming error: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Progressive search implementation
@app.route('/search/progressive', methods=['POST'])
def progressive_search():
    data = request.json
    query = data.get('query').lower().strip()
    session_id = data.get('session_id', str(uuid.uuid4()))
    
    if not query:
        return jsonify({'error': 'Search query is required'}), 400
    
    search_sessions[session_id] = {
        'query': query,
        'started': time.time(),
        'results': [],
        'completed': False
    }
    
    def search_task():
        try:
            search_result = search_spotify_api(query)
            
            if search_result['success']:
                results = []
                for track in search_result['results']:
                    result = {
                        'song_name': track['song_name'],
                        'artist': track['artist'],
                        'album': track.get('album', ''),
                        'title': f"{track['song_name']} - {track['artist']}",
                        'spotify_data': True
                    }
                    results.append(result)
                    
                    search_sessions[session_id]['results'] = results
                    time.sleep(app.config['PROGRESSIVE_SEARCH_DELAY'])
                
                search_sessions[session_id]['completed'] = True
                search_sessions[session_id]['final_results'] = results
            else:
                search_sessions[session_id]['error'] = search_result['error']
                search_sessions[session_id]['completed'] = True
                
        except Exception as e:
            search_sessions[session_id]['error'] = str(e)
            search_sessions[session_id]['completed'] = True
    
    Thread(target=search_task, daemon=True).start()
    
    return jsonify({
        'session_id': session_id,
        'status': 'started',
        'message': 'Search started'
    })

@app.route('/search/progressive/<session_id>')
def get_progressive_results(session_id):
    if session_id not in search_sessions:
        return jsonify({'error': 'Invalid session ID'}), 404
    
    session = search_sessions[session_id]
    
    response = {
        'session_id': session_id,
        'completed': session.get('completed', False),
        'results': session.get('results', [])
    }
    
    if session.get('completed'):
        if 'error' in session:
            response['error'] = session['error']
        else:
            response['results'] = session.get('final_results', [])
        if time.time() - session['started'] > 300:
            search_sessions.pop(session_id, None)
    
    return jsonify(response)

@app.route('/api/genre/explore', methods=['POST'])
def explore_genre():
    """Explore a genre and return structured data for homepage"""
    data = request.json
    genre = data.get('genre', '').strip()
    auto_download = data.get('auto_download', app.config['AUTO_DOWNLOAD_GENRE_SONGS'])
    
    if not genre:
        return jsonify({'error': 'Genre is required'}), 400
    
    # Check cache first
    cached_result = genre_cache.get(genre)
    if cached_result:
        return jsonify(cached_result)
    
    session_id = str(uuid.uuid4())
    genre_exploration_sessions[session_id] = {
        'genre': genre,
        'started': time.time(),
        'completed': False,
        'data': None
    }
    
    def genre_exploration_task():
        try:
            # Call the FastAPI genre exploration
            result = explore_genre_api(genre)
            
            if result['success']:
                genre_data = result['genre_data']
                
                # Add songs to library
                songs_added = add_genre_songs_to_library(genre_data)
                
                # Auto-download songs if enabled
                downloads_triggered = 0
                if auto_download and download_manager:
                    downloads_triggered = auto_download_genre_songs(genre_data, 'genre_explorer')
                    system_monitor.increment_counter('total_auto_downloads', downloads_triggered)
                
                # Update session
                genre_exploration_sessions[session_id]['completed'] = True
                genre_exploration_sessions[session_id]['data'] = {
                    'success': True,
                    'genre': genre,
                    'exploration_time': genre_data.get('exploration_time', 'Unknown'),
                    'top_charts': genre_data.get('top_charts', []),
                    'new_releases': genre_data.get('new_releases', []),
                    'featured_artists': genre_data.get('featured_artists', []),
                    'artist_deep_dives': genre_data.get('artist_deep_dives', {}),
                    'songs_added_to_library': songs_added,
                    'downloads_triggered': downloads_triggered,
                    'total_songs_discovered': len(genre_data.get('top_charts', [])) + 
                                            sum(len(artist_data.get('top_tracks', [])) 
                                                for artist_data in genre_data.get('artist_deep_dives', {}).values())
                }
                
                # Cache the result
                genre_cache.set(genre, genre_exploration_sessions[session_id]['data'])
                
                # Update counters
                system_monitor.increment_counter('total_genre_explorations')
                
            else:
                genre_exploration_sessions[session_id]['completed'] = True
                genre_exploration_sessions[session_id]['data'] = {
                    'success': False,
                    'error': result['error']
                }
                
        except Exception as e:
            genre_exploration_sessions[session_id]['completed'] = True
            genre_exploration_sessions[session_id]['data'] = {
                'success': False,
                'error': f'Genre exploration failed: {str(e)}'
            }
    
    Thread(target=genre_exploration_task, daemon=True).start()
    
    return jsonify({
        'session_id': session_id,
        'status': 'started',
        'message': f'Genre exploration started for: {genre}',
        'auto_download': auto_download
    })

@app.route('/api/genre/status/<session_id>')
def get_genre_exploration_status(session_id):
    """Get the status of a genre exploration session"""
    if session_id not in genre_exploration_sessions:
        return jsonify({'error': 'Invalid session ID'}), 404
    
    session = genre_exploration_sessions[session_id]
    
    response = {
        'session_id': session_id,
        'genre': session['genre'],
        'completed': session.get('completed', False)
    }
    
    if session.get('completed') and session.get('data'):
        response.update(session['data'])
        # Clean up old session after completion
        if time.time() - session['started'] > 600:  # 10 minutes
            genre_exploration_sessions.pop(session_id, None)
    
    return jsonify(response)

@app.route('/api/genre/popular')
def get_popular_genres():
    """Get list of popular genres for the start screen"""
    popular_genres = [
        # --- SOUTH AFRICAN GENRES ---
        "lekompo",
        "amapiano",
        "piano",
        "bolobedu music",
        "bad company",
        "tribal",
        "gqom",
        "kwaito",
        "maskandi",
        "mbaqanga",
        "mbube",
        "afro house",
        "deep house sa",
        "afro pop sa",
        "rhythm & praise (sa)",
        "afrikaans pop",
        "afrikaans rock",
        "cape jazz",
        "cape ghoema",
        "sa hip hop",
        "sa rap",
        "sa r&b",
        "venrap",
        "tsonga electro",
        "bacardi music",

        # --- YOUR ORIGINAL GENRES (UNCHANGED) ---
        "pop", "rock", "hip hop", "jazz", "classical", "electronic",
        "r&b", "country", "reggae", "latin", "k-pop", "indie",
        "afrobeats", "reggaeton", "dance", "blues", "soul", "folk",
        "metal", "punk", "funk", "disco", "house", "techno",

        # --- MORE GLOBAL GENRES ---
        "trap",
        "drill",
        "lofi",
        "edm",
        "dubstep",
        "trance",
        "synthwave",
        "chillwave",
        "alt rock",
        "grunge",
        "hard rock",
        "j-pop",
        "c-pop",
        "mandopop",
        "emo",
        "ska",
        "opera",
        "world music",
        "afro fusion",
        "dancehall",
        "soca",
        "zouk",
        "bossa nova",
        "flamenco",
        "ambient",
        "cinematic",
        "new age",
    ]

    return jsonify({
        'success': True,
        'genres': popular_genres,
        'total': len(popular_genres)
    })

# HTML Page Routes
@app.route('/')
def index():
    return send_file('templates/index.html')

@app.route('/library.html')
def library():
    return send_file('templates/library.html')

@app.route('/albums.html')
def albums():
    return send_file('templates/albums.html')

@app.route('/playlists.html')
def playlists():
    return send_file('templates/playlists.html')

@app.route('/stats.html')
def stats():
    return send_file('templates/stats.html')

@app.route('/start.html')
def start_screen():
    return send_file('templates/start.html')

# API Routes
@app.route('/songs')
def get_songs():
    songs = load_songs()
    for song in songs:
        song['downloaded'] = is_song_downloaded(song['song_name'], song['artist'])
    return jsonify(songs)

@app.route('/download/<int:song_index>')
def download_song_by_index(song_index):
    user_id = get_user_id()
    songs = load_songs()
    song = next((s for s in songs if s['index'] == song_index), None)
    
    if not song:
        return jsonify({'error': 'Song not found'}), 404
    
    existing_file = find_song_file(song['song_name'], song['artist'])
    if existing_file:
        return send_file(existing_file, as_attachment=True, download_name=os.path.basename(existing_file))
    
    if download_manager is None:
        return jsonify({'error': 'Download service unavailable'}), 503
    
    download_id = download_manager.submit_download(
        song['song_name'], 
        song['artist'], 
        song.get('album'),
        user_id,
        expected_duration=song.get('duration')
    )
    
    return jsonify({
        'download_id': download_id,
        'message': 'Download queued',
        'status': 'queued',
        'user_id': user_id,
        'queue_position': download_manager.get_queue_stats()['queue_size'],
        'duration_validation': bool(song.get('duration')),
        'quality': '48kbps (64kbps fallback)'
    })

@app.route('/download/trigger/<int:song_index>')
def trigger_download(song_index):
    user_id = get_user_id()
    songs = load_songs()
    song = next((s for s in songs if s['index'] == song_index), None)
    
    if not song:
        return jsonify({'error': 'Song not found'}), 404
    
    existing_file = find_song_file(song['song_name'], song['artist'])
    if existing_file:
        file_size = os.path.getsize(existing_file) / (1024 * 1024)
        return jsonify({
            'status': 'already_downloaded',
            'message': 'File already exists on server',
            'file_url': f'/download/file/{song_index}',
            'file_size_mb': round(file_size, 2)
        })
    
    if download_manager is None:
        return jsonify({'error': 'Download service unavailable'}), 503
    
    download_id = download_manager.submit_download(
        song['song_name'], 
        song['artist'], 
        song.get('album'),
        user_id,
        expected_duration=song.get('duration')
    )
    
    timeout = 300
    start_time = time.time()
    check_interval = 1
    
    while time.time() - start_time < timeout:
        status = download_status.get(download_id, {})
        
        if status.get('status') == 'completed':
            final_file = find_song_file(song['song_name'], song['artist'])
            if final_file and os.path.exists(final_file):
                file_size = os.path.getsize(final_file) / (1024 * 1024)
                return jsonify({
                    'status': 'completed',
                    'download_id': download_id,
                    'file_url': f'/download/file/{song_index}',
                    'file_size_mb': round(file_size, 2),
                    'message': status.get('message', 'Download completed')
                })
            else:
                return jsonify({
                    'status': 'failed',
                    'error': 'Download completed but file not found'
                }), 500
                
        elif status.get('status') == 'failed':
            error_msg = status.get('message', 'Download failed')
            return jsonify({
                'status': 'failed',
                'error': error_msg
            }), 500
        
        time.sleep(check_interval)
    
    return jsonify({
        'status': 'timeout',
        'error': 'Download timed out after 5 minutes'
    }), 500

@app.route('/download/file/<int:song_index>')
def download_song_file(song_index):
    user_id = get_user_id()
    songs = load_songs()
    song = next((s for s in songs if s['index'] == song_index), None)
    
    if not song:
        return jsonify({'error': 'Song not found'}), 404
    
    file_path = find_song_file(song['song_name'], song['artist'])
    
    if file_path and os.path.exists(file_path):
        return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path))
    else:
        return jsonify({'error': 'File not found or not yet downloaded'}), 404

@app.route('/download/status/<download_id>')
def get_download_status(download_id):
    user_id = get_user_id()
    status = download_status.get(download_id, {'status': 'unknown'})
    
    if status.get('user_id') != user_id and user_id != 'anonymous':
        return jsonify({'error': 'Access denied'}), 403
    
    return jsonify(status)

@app.route('/user/downloads')
def get_user_downloads():
    user_id = get_user_id()
    if download_manager:
        user_downloads = download_manager.get_user_downloads(user_id)
        return jsonify({
            'user_id': user_id,
            'downloads': user_downloads,
            'total': len(user_downloads)
        })
    else:
        return jsonify({'error': 'Download service unavailable'}), 503

@app.route('/add_song', methods=['POST'])
def add_song():
    data = request.json
    song_name = data.get('song_name')
    artist = data.get('artist')
    album = data.get('album', '')
    duration = data.get('duration', '')
    
    if not song_name or not artist:
        return jsonify({'error': 'Song name and artist are required'}), 400
    
    songs = load_songs()
    
    for song in songs:
        if song['song_name'].lower() == song_name.lower() and song['artist'].lower() == artist.lower():
            return jsonify({'error': 'Song already exists in library'}), 400
    
    new_index = max([s['index'] for s in songs], default=0) + 1
    
    new_song = {
        'index': new_index,
        'song_name': song_name,
        'artist': artist,
        'album': album,
        'duration': duration
    }
    
    songs.append(new_song)
    save_songs(songs)
    
    system_monitor.increment_counter('total_songs_saved')
    
    return jsonify({'success': True, 'song': new_song})

@app.route('/search', methods=['POST'])
def search_songs():
    data = request.json
    query = data.get('query').lower().strip()
    
    if not query:
        return jsonify({'error': 'Search query is required'}), 400
    
    system_monitor.increment_counter('total_search_requests')
    
    cached_result = search_cache.get(query)
    if cached_result:
        return jsonify(cached_result)
    
    search_result = search_spotify_api(query)
    
    if search_result['success']:
        results = []
        for track in search_result['results']:
            results.append({
                'song_name': track['song_name'],
                'artist': track['artist'],
                'album': track.get('album', ''),
                'title': f"{track['song_name']} - {track['artist']}",
                'spotify_data': True
            })
        
        response_data = {
            'success': True,
            'results': results,
            'message': search_result.get('message', 'Search completed')
        }
        
        search_cache.set(query, response_data)
        
        return jsonify(response_data)
    else:
        return jsonify({
            'success': False,
            'error': search_result['error'],
            'results': []
        }), 500

@app.route('/featured')
def get_featured_songs():
    songs = load_songs()
    import random
    featured = random.sample(songs, min(8, len(songs)))
    for song in featured:
        song['downloaded'] = is_song_downloaded(song['song_name'], song['artist'])
    return jsonify(featured)

@app.route('/queue/status')
def get_queue_status():
    if download_manager:
        stats = download_manager.get_queue_stats()
        try:
            stats['cpu_percent'] = psutil.cpu_percent()
            stats['memory_percent'] = psutil.virtual_memory().percent
            stats['available_memory_gb'] = psutil.virtual_memory().available / (1024 ** 3)
        except:
            stats['system_info'] = 'unavailable'
        
        return jsonify(stats)
    else:
        return jsonify({'error': 'Download service unavailable'}), 503

@app.route('/api/system-status')
def get_system_status():
    """Get comprehensive system status"""
    system_data = system_monitor.get_system_stats()
    
    if download_manager:
        dl_stats = download_manager.get_queue_stats()
        system_data.update(dl_stats)
    
    songs = load_songs()
    system_data.update({
        'library_size': len(songs),
        'downloaded_files': len([f for f in os.listdir(app.config['DOWNLOAD_FOLDER']) if f.endswith('.opus')]),
        'download_folder_size': sum(os.path.getsize(os.path.join(app.config['DOWNLOAD_FOLDER'], f)) 
                                  for f in os.listdir(app.config['DOWNLOAD_FOLDER']) 
                                  if os.path.isfile(os.path.join(app.config['DOWNLOAD_FOLDER'], f))) / (1024**2)
    })
    
    return jsonify(system_data)

@app.route('/admin/stats')
def admin_stats():
    if download_manager:
        stats = download_manager.get_queue_stats()
        
        system_data = system_monitor.get_system_stats()
        
        stats.update({
            'system': {
                'cpu_percent': system_data.get('current_cpu', 0),
                'memory_percent': system_data.get('current_memory', 0),
                'disk_usage': system_data.get('disk_usage_percent', 0),
                'active_processes': len(psutil.pids())
            },
            'downloads': {
                'total_completed': system_data.get('total_downloads_completed', 0),
                'total_failed': system_data.get('total_downloads_failed', 0),
                'total_queued': download_queue.qsize(),
                'total_active': len(active_downloads)
            },
            'files': {
                'download_folder': app.config['DOWNLOAD_FOLDER'],
                'total_files': len(os.listdir(app.config['DOWNLOAD_FOLDER'])),
                'total_size_mb': system_data.get('download_folder_size', 0)
            },
            'counters': {
                'total_search_requests': system_data.get('total_search_requests', 0),
                'total_songs_saved': system_data.get('total_songs_saved', 0),
                'total_bandwidth_used_mb': system_data.get('total_bandwidth_used', 0),
                'structural_integrity': system_data.get('structural_integrity', 0),
                'total_genre_explorations': system_data.get('total_genre_explorations', 0),
                'total_auto_downloads': system_data.get('total_auto_downloads', 0),
                'total_lyrics_requests': system_data.get('total_lyrics_requests', 0),
                'successful_lyrics_fetches': system_data.get('successful_lyrics_fetches', 0),
                'failed_lyrics_fetches': system_data.get('failed_lyrics_fetches', 0)
            },
            'optimizations': {
                'concurrent_downloads': app.config['MAX_CONCURRENT_DOWNLOADS'],
                'audio_quality': '48kbps (64kbps fallback)',
                'gzip_compression': True,
                'progressive_search': True,
                'download_resumption': True,
                'stuck_download_cleanup': f"{app.config['STUCK_DOWNLOAD_TIMEOUT']}s",
                'duration_validation': f"±{app.config['DURATION_TOLERANCE']} seconds",
                'auto_download_genre_songs': app.config['AUTO_DOWNLOAD_GENRE_SONGS'],
                'genre_exploration_timeout': f"{app.config['GENRE_EXPLORATION_TIMEOUT']}s",
                'lyrics_caching': '24 hours TTL',  # NEW
                'lyrics_timeout': f"{app.config['LYRICS_REQUEST_TIMEOUT']}s"  # NEW
            }
        })
        
        return jsonify(stats)
    else:
        return jsonify({'error': 'Download service unavailable'}), 503

@app.route('/health')
def health_check():
    system_data = system_monitor.get_system_stats()
    
    return jsonify({
        'status': 'healthy', 
        'service': 'D-TECH MUSIC',
        'version': '2.8-LYRICS-ENHANCED',
        'concurrent_downloads': app.config['MAX_CONCURRENT_DOWNLOADS'],
        'audio_format': 'Opus 48kbps (64kbps fallback)',
        'duration_validation': f"±{app.config['DURATION_TOLERANCE']} seconds",
        'structural_integrity': f"{system_data.get('structural_integrity', 0)}%",
        'statistics': {
            'total_search_requests': system_data.get('total_search_requests', 0),
            'total_songs_saved': system_data.get('total_songs_saved', 0),
            'library_size': len(load_songs()),
            'server_uptime': round(system_data.get('server_uptime', 0), 2),
            'genre_explorations': system_data.get('total_genre_explorations', 0),
            'auto_downloads': system_data.get('total_auto_downloads', 0),
            'lyrics_requests': system_data.get('total_lyrics_requests', 0),
            'successful_lyrics': system_data.get('successful_lyrics_fetches', 0)
        },
        'features': [
            '12x concurrent downloads',
            '48kbps audio quality', 
            'Gzip compression',
            'Progressive search',
            'Download resumption',
            'Stuck download cleanup',
            'Pre-emptive queue processing',
            'SMART Genre Discovery',
            'Auto-download genre songs',
            'Popular genres library',
            'Homepage content generation',
            '🎵 Genius Lyrics Integration',  # NEW
            '📝 Real-time Lyrics Fetching',  # NEW
            '💾 Lyrics Caching (24h TTL)',   # NEW
            '⚡ Async Lyrics Processing'     # NEW
        ]
    })

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Resource not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(503)
def service_unavailable(error):
    return jsonify({'error': 'Service temporarily unavailable'}), 503

if __name__ == '__main__':
    # Create initial data.json if it doesn't exist
    if not os.path.exists(app.config['DATA_FILE']):
        initial_data = [
            {"index": 1, "song_name": "Shabeen", "artist": "Thomas Mapfumo And The Blacks Unlimited", "album": "Unknown Album", "duration": "4:49"},
            {"index": 2, "song_name": "Rato Laka", "artist": "Shebeshxt, Naqua SA, Zee Nxumalo, Slidoo Man", "album": "Unknown Album", "duration": "5:34"},
        ]
        save_songs(initial_data)
        system_monitor.increment_counter('total_songs_saved', len(initial_data))
    
    print("🚀 Starting D-TECH MUSIC v2.8-LYRICS-ENHANCED on http://localhost:5000")
    print("✅ ADVANCED FEATURES ENABLED:")
    print("   • 12 concurrent downloads")
    print("   • 48kbps audio (64kbps fallback)") 
    print("   • Gzip compression")
    print("   • Progressive search loading")
    print("   • Download resumption")
    print("   • Stuck download cleanup (8min timeout)")
    print("   • Pre-emptive queue processing")
    print("🎵 NEW GENRE DISCOVERY SYSTEM:")
    print("   • Smart genre exploration")
    print("   • Auto-download genre songs")
    print("   • Popular genres library")
    print("   • Homepage content generation")
    print("   • 24 pre-defined popular genres")
    print("🎤 NEW LYRICS INTEGRATION:")
    print("   • Genius API lyrics fetching")
    print("   • Real-time lyrics extraction")
    print("   • 24-hour lyrics caching")
    print("   • Async processing for performance")
    print("   • Lyrics endpoints: /api/lyrics")
    print("📊 System monitoring enabled with structural integrity scoring")
    print("🏠 Start screen available at http://localhost:5000/start.html")
    print("📈 Status dashboard available at http://localhost:5000/stats.html")
    
    try:
        app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
    except KeyboardInterrupt:
        if download_manager:
            download_manager.stop()
        system_monitor.stop()
        print("👋 D-TECH MUSIC stopped")