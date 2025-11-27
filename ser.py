#!/usr/bin/env python3
from playwright.async_api import async_playwright
import time
import json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
from typing import List, Dict
import asyncio
from contextlib import asynccontextmanager
import logging
import re
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_FILE = "data.json"

class BrowserPool:
    def __init__(self, pool_size: int = 5):
        self.pool_size = pool_size
        self.available_pages = asyncio.Queue()
        self.in_use_pages = set()
        self.playwright = None
        self.browser = None
        self._initialized = False

    async def initialize(self):
        """Initialize the browser pool"""
        if self._initialized:
            return
            
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        
        # Create initial pages
        for _ in range(self.pool_size):
            page = await self.browser.new_page()
            # Set longer timeout for page operations
            page.set_default_timeout(30000)
            await self.available_pages.put(page)
        
        self._initialized = True
        logger.info(f"🎯 Browser pool initialized with {self.pool_size} pages")

    async def get_page(self):
        """Get a page from the pool"""
        if not self._initialized:
            await self.initialize()
        
        page = await self.available_pages.get()
        self.in_use_pages.add(page)
        return page

    async def return_page(self, page):
        """Return a page to the pool"""
        if page in self.in_use_pages:
            self.in_use_pages.remove(page)
        
        # Clear any navigation or dialogs
        try:
            # Try to go to blank page to reset state
            await page.goto('about:blank', wait_until='domcontentloaded', timeout=5000)
        except:
            pass
            
        await self.available_pages.put(page)

    async def close(self):
        """Close the entire pool"""
        if self.browser:
            # Close all pages
            while not self.available_pages.empty():
                try:
                    page = await self.available_pages.get()
                    await page.close()
                except:
                    pass
            
            await self.browser.close()
        
        if self.playwright:
            await self.playwright.stop()
        
        self._initialized = False
        logger.info("🔚 Browser pool closed")

# Global browser pool
browser_pool = BrowserPool(pool_size=10)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await browser_pool.initialize()
    logger.info("🚀 D-TECH Music Search API Starting with Browser Pool...")
    yield
    await browser_pool.close()
    logger.info("🔚 D-TECH Music Search API Stopped")

app = FastAPI(title="D-TECH Music Search API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SpotifyScraper:
    def __init__(self, page):
        self.page = page

    async def search_tracks(self, song_name: str, max_results: int = 10) -> List[Dict]:
        """Search Spotify and return track metadata including duration."""
        search_url = f"https://open.spotify.com/search/{song_name.replace(' ', '%20')}"
        logger.info(f"🔍 Searching for: {song_name}")
        
        try:
            await self.page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
            
            for attempt in range(3):
                try:
                    await self.page.wait_for_selector('div[data-testid="tracklist-row"]', timeout=10000)
                    break
                except Exception as e:
                    if attempt == 2:
                        raise e
                    await asyncio.sleep(1)
            
            track_rows = await self.page.query_selector_all('div[data-testid="tracklist-row"]')
            logger.info(f"📊 Found {len(track_rows)} track rows for '{song_name}'")
            
            tracks_data = []
            for i, row in enumerate(track_rows[:max_results]):
                try:
                    # Song name - get the first [data-encore-id="text"] element
                    song_elem = await row.query_selector('[data-encore-id="text"]')
                    song_name_clean = "Unknown Song"
                    if song_elem:
                        song_text = await song_elem.text_content()
                        song_name_clean = song_text.strip() if song_text else "Unknown Song"

                    # Artist - get the second [data-encore-id="text"] element
                    artist_elems = await row.query_selector_all('[data-encore-id="text"]')
                    artist_name = "Unknown Artist"
                    if len(artist_elems) >= 2:
                        artist_text = await artist_elems[1].text_content()
                        artist_name = artist_text.strip() if artist_text else "Unknown Artist"

                    # Album name - try to get from album link
                    album_elem = await row.query_selector('a[href*="/album/"]')
                    album_name = "Unknown Album"
                    if album_elem:
                        album_text = await album_elem.text_content()
                        album_name = album_text.strip() if album_text else "Unknown Album"

                    # Duration - get the LAST [data-encore-id="text"] element
                    duration = "Unknown Duration"
                    if artist_elems:
                        duration_elem = artist_elems[-1]  # Last element is the duration
                        duration_text = await duration_elem.text_content()
                        if duration_text:
                            duration = duration_text.strip()
                            # Validate it's actually a duration (mm:ss format)
                            if not any(char.isdigit() for char in duration) or ':' not in duration:
                                duration = "Unknown Duration"

                    tracks_data.append({
                        'index': i + 1,
                        'song_name': song_name_clean,
                        'artist': artist_name,
                        'album': album_name,
                        'duration': duration
                    })
                    
                    logger.info(f"🎵 Track {i+1}: {song_name_clean} - {artist_name} - Duration: {duration}")
                    
                except Exception as e:
                    logger.warning(f"❌ Error processing track {i+1}: {e}")
                    continue

            return tracks_data
        
        except Exception as e:
            logger.error(f"❌ Error during search for '{song_name}': {e}")
            return []

class SmartGenreExplorer:
    def __init__(self, page):
        self.page = page

    async def explore_genre_complete(self, genre: str):
        """Complete smart genre exploration for homepage content"""
        logger.info(f"🎵 SMART GENRE EXPLORATION: '{genre.upper()}'")
        
        start_time = time.time()
        
        # Navigate to genre search
        search_url = f"https://open.spotify.com/search/{genre.replace(' ', '%20')}"
        await self.page.goto(search_url, wait_until="domcontentloaded")
        await self.page.wait_for_timeout(5000)
        
        complete_results = {
            'genre': genre,
            'timestamp': datetime.now().isoformat(),
            'exploration_time_seconds': 0,
            'top_charts': await self._get_top_charts(),
            'new_releases': await self._get_new_releases(),
            'featured_artists': await self._get_featured_artists(),
            'artist_deep_dives': {}
        }
        
        # Do deep dives on real artists
        logger.info("📍 Starting artist deep dives...")
        real_artists = self._filter_real_artists(complete_results['featured_artists'])
        
        for i, artist in enumerate(real_artists[:6]):  # Limit to 6 real artists
            logger.info(f"   🎤 Exploring {artist}...")
            try:
                artist_data = await self._get_artist_deep_dive(artist)
                if artist_data and artist_data.get('top_tracks'):
                    complete_results['artist_deep_dives'][artist] = artist_data
            except Exception as e:
                logger.warning(f"   ❌ Failed to explore {artist}: {e}")
        
        complete_results['exploration_time_seconds'] = round(time.time() - start_time, 2)
        return complete_results

    async def _get_top_charts(self) -> List[Dict]:
        """Get actual top charts"""
        charts = []
        
        try:
            await self.page.wait_for_selector('div[data-testid="tracklist-row"]', timeout=10000)
            track_rows = await self.page.query_selector_all('div[data-testid="tracklist-row"]')
            
            for i, row in enumerate(track_rows[:8]):  # Top 8 tracks
                try:
                    text_elems = await row.query_selector_all('[data-encore-id="text"]')
                    
                    if len(text_elems) >= 2:
                        song_data = {
                            'chart_position': i + 1,
                            'song_name': await text_elems[0].text_content(),
                            'artist': await text_elems[1].text_content(),
                            'duration': 'Unknown',
                            'explicit': False,
                            'popularity': 'high' if i < 4 else 'medium'
                        }
                        
                        # Get duration
                        if text_elems:
                            duration = await text_elems[-1].text_content()
                            if ':' in duration and any(c.isdigit() for c in duration):
                                song_data['duration'] = duration.strip()
                        
                        # Check explicit
                        explicit_elem = await row.query_selector('[data-testid="explicit-track"]')
                        if explicit_elem:
                            song_data['explicit'] = True
                        
                        charts.append(song_data)
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            logger.error(f"Error getting top charts: {e}")
            
        return charts

    async def _get_new_releases(self) -> List[Dict]:
        """Get actual new releases with proper date detection"""
        new_releases = []
        
        try:
            album_links = await self.page.query_selector_all('a[href*="/album/"]')
            
            for card in album_links[:12]:
                try:
                    card_context = await card.evaluate_handle('(el) => el.closest("div")')
                    card_text = await card_context.as_element().text_content()
                    
                    if len(card_text) > 20:  # Real albums have more context
                        album_title = await card.text_content()
                        
                        if (album_title and 
                            len(album_title) > 3 and 
                            not album_title.lower().replace(' ', '').isalpha() and
                            not any(word in album_title.lower() for word in ['playlist', 'mix', 'hits'])):
                            
                            release_data = {
                                'title': album_title.strip(),
                                'url': await card.get_attribute('href'),
                                'recency': self._detect_recency(card_text)
                            }
                            
                            # Try to extract artist
                            artist_elem = await card_context.as_element().query_selector('a[href*="/artist/"]')
                            if artist_elem:
                                release_data['artist'] = await artist_elem.text_content()
                            
                            # Avoid duplicates
                            if (release_data.get('artist') and 
                                not any(r['title'] == release_data['title'] for r in new_releases)):
                                new_releases.append(release_data)
                                
                except Exception as e:
                    continue
            
            # Sort by recency and take top 6
            new_releases.sort(key=lambda x: {'brand_new': 0, 'recent': 1, 'unknown': 2}.get(x['recency'], 3))
            
        except Exception as e:
            logger.error(f"Error getting new releases: {e}")
            
        return new_releases[:6]

    def _detect_recency(self, text: str) -> str:
        """Smart recency detection based on text patterns"""
        text_lower = text.lower()
        current_year = datetime.now().year
        
        if str(current_year) in text:
            return 'brand_new'
        elif str(current_year - 1) in text:
            return 'recent'
        
        recency_indicators = ['new', 'just released', 'latest', 'fresh', 'recent']
        if any(indicator in text_lower for indicator in recency_indicators):
            return 'brand_new'
        
        return 'unknown'

    async def _get_featured_artists(self) -> List[str]:
        """Get actual featured artists, not just text matches"""
        artists = []
        
        try:
            artist_links = await self.page.query_selector_all('a[href*="/artist/"]')
            
            for link in artist_links:
                try:
                    artist_name = await link.text_content()
                    
                    if self._is_real_artist(artist_name) and artist_name not in artists:
                        artists.append(artist_name)
                        
                except Exception as e:
                    continue
                
        except Exception as e:
            logger.error(f"Error getting featured artists: {e}")
            
        return artists[:8]

    def _is_real_artist(self, name: str) -> bool:
        """Determine if this is a real artist name, not a genre/category"""
        name_lower = name.lower()
        
        false_positives = [
            'artist', 'playlist', 'mix', 'hits', 'album', 'single', 
            'ep', 'compilation', 'collection', 'radio', 'station'
        ]
        
        if (len(name) < 2 or 
            any(fp in name_lower for fp in false_positives) or
            name_lower.isnumeric() or
            len(name.split()) > 4):
            return False
            
        if name.isupper() or name.islower():
            return False
            
        return True

    def _filter_real_artists(self, artists: List[str]) -> List[str]:
        """Filter out non-artist entries from the list"""
        return [artist for artist in artists if self._is_real_artist(artist)]

    async def _get_artist_deep_dive(self, artist_name: str) -> Dict:
        """Deep dive into a specific real artist"""
        artist_data = {
            'top_tracks': [],
            'popularity': 'unknown'
        }
        
        try:
            # Search for the artist specifically
            search_url = f"https://open.spotify.com/search/{artist_name.replace(' ', '%20')}"
            await self.page.goto(search_url, wait_until="domcontentloaded")
            await self.page.wait_for_timeout(3000)
            
            # Look for tracks by this artist
            track_rows = await self.page.query_selector_all('div[data-testid="tracklist-row"]')
            
            for row in track_rows[:8]:  # Get top 8 tracks
                try:
                    text_elems = await row.query_selector_all('[data-encore-id="text"]')
                    
                    if len(text_elems) >= 2:
                        track_artist = await text_elems[1].text_content()
                        
                        # Verify this track is actually by our artist
                        if (artist_name.lower() in track_artist.lower() or
                            self._artist_name_match(artist_name, track_artist)):
                            
                            song_data = {
                                'song_name': await text_elems[0].text_content(),
                                'artist': track_artist,
                                'duration': 'Unknown'
                            }
                            
                            # Get duration
                            if text_elems:
                                duration = await text_elems[-1].text_content()
                                if ':' in duration and any(c.isdigit() for c in duration):
                                    song_data['duration'] = duration.strip()
                            
                            # Check explicit
                            explicit_elem = await row.query_selector('[data-testid="explicit-track"]')
                            if explicit_elem:
                                song_data['explicit'] = True
                            
                            artist_data['top_tracks'].append(song_data)
                            
                except Exception as e:
                    continue
                    
        except Exception as e:
            logger.error(f"Error in artist deep dive for {artist_name}: {e}")
            
        return artist_data

    def _artist_name_match(self, search_artist: str, track_artist: str) -> bool:
        """Smart artist name matching"""
        search_lower = search_artist.lower()
        track_lower = track_artist.lower()
        
        if search_lower in track_lower or track_lower in search_lower:
            return True
        
        search_base = search_lower.split(' feat')[0].split(' with')[0].split(' &')[0].strip()
        track_base = track_lower.split(' feat')[0].split(' with')[0].split(' &')[0].strip()
        
        return search_base == track_base

def load_existing_data() -> List[Dict]:
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return []
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return []

def save_to_data_json(tracks: List[Dict]):
    """Save tracks to data.json, avoiding duplicates by song+artist+album"""
    existing_data = load_existing_data()
    existing_keys = {(t['song_name'], t['artist'], t['album']) for t in existing_data}
    
    new_tracks = []
    for track in tracks:
        key = (track['song_name'], track['artist'], track['album'])
        if key not in existing_keys:
            track['index'] = len(existing_data) + len(new_tracks) + 1
            new_tracks.append(track)
            existing_keys.add(key)
    
    if new_tracks:
        all_tracks = existing_data + new_tracks
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_tracks, f, indent=2, ensure_ascii=False)
        logger.info(f"💾 Added {len(new_tracks)} new tracks to {DATA_FILE}")
        return new_tracks
    else:
        logger.info("ℹ️ No new tracks to add")
        return []

@app.get("/")
async def root():
    return {"message": "D-TECH Music Search API is running with Smart Genre Discovery"}

@app.get("/api/search")
async def search_songs(query: str, max_results: int = 10):
    """Search for songs and add to data.json"""
    if not query or len(query.strip()) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")
    
    page = await browser_pool.get_page()
    
    try:
        scraper = SpotifyScraper(page)
        tracks = await scraper.search_tracks(query, max_results)
        
        if not tracks:
            return {"success": False, "message": "No tracks found", "new_tracks": []}
        
        new_tracks = save_to_data_json(tracks)
        return {
            "success": True,
            "message": f"Found {len(tracks)} tracks, added {len(new_tracks)} new ones",
            "total_found": len(tracks),
            "new_tracks": new_tracks,
            "all_tracks": tracks
        }
        
    except Exception as e:
        logger.error(f"Search error for '{query}': {e}")
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")
    finally:
        await browser_pool.return_page(page)

@app.get("/api/genre/explore")
async def explore_genre(genre: str):
    """Smart genre exploration for homepage content - discovers top charts, new releases, and artist deep dives"""
    if not genre or len(genre.strip()) < 2:
        raise HTTPException(status_code=400, detail="Genre must be at least 2 characters")
    
    page = await browser_pool.get_page()
    
    try:
        explorer = SmartGenreExplorer(page)
        genre_data = await explorer.explore_genre_complete(genre)
        
        # Save all discovered songs to data.json
        all_songs = []
        
        # Save top charts songs
        for chart in genre_data['top_charts']:
            all_songs.append({
                'song_name': chart['song_name'],
                'artist': chart['artist'],
                'album': 'Top Charts',
                'duration': chart['duration']
            })
        
        # Save artist songs
        for artist, data in genre_data['artist_deep_dives'].items():
            for song in data['top_tracks']:
                all_songs.append({
                    'song_name': song['song_name'],
                    'artist': song['artist'],
                    'album': f"{artist} Collection",
                    'duration': song['duration']
                })
        
        # Save to main database
        new_tracks = save_to_data_json(all_songs)
        
        return {
            "success": True,
            "genre": genre,
            "exploration_time": f"{genre_data['exploration_time_seconds']} seconds",
            "top_charts": genre_data['top_charts'],
            "new_releases": genre_data['new_releases'],
            "featured_artists": genre_data['featured_artists'],
            "artist_deep_dives": genre_data['artist_deep_dives'],
            "new_tracks_added": len(new_tracks),
            "total_songs_discovered": len(all_songs)
        }
        
    except Exception as e:
        logger.error(f"Genre exploration error for '{genre}': {e}")
        raise HTTPException(status_code=500, detail=f"Genre exploration error: {str(e)}")
    finally:
        await browser_pool.return_page(page)

@app.get("/api/songs")
async def get_all_songs():
    """Get all songs from data.json"""
    try:
        songs = load_existing_data()
        return {"success": True, "songs": songs, "total": len(songs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading songs: {str(e)}")

@app.delete("/api/songs/{index}")
async def delete_song(index: int):
    """Remove a song by its index"""
    try:
        songs = load_existing_data()
        if index < 1 or index > len(songs):
            raise HTTPException(status_code=404, detail="Song not found")
        
        removed = songs.pop(index - 1)
        for i, song in enumerate(songs):
            song['index'] = i + 1
        
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(songs, f, indent=2, ensure_ascii=False)
        
        return {"success": True, "message": f"Removed {removed['song_name']} by {removed['artist']}", "total_remaining": len(songs)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error removing song: {str(e)}")

if __name__ == "__main__":
    print("🚀 D-TECH Music Search API Starting with Smart Genre Discovery...")
    print("🔍 Search Backend running on: http://localhost:5001")
    print("📁 Data file: data.json")
    print("🎯 Browser pool size: 10")
    print("🎵 NEW: Smart Genre Exploration endpoint at /api/genre/explore")
    print("🏠 Perfect for homepage content generation!")
    uvicorn.run(app, host="0.0.0.0", port=5001, log_level="info")
