"""
XLAB ShortClipper - Railway Backend
Flask app with in-memory job tracking, background processing,
direct ZIP download. No Firebase or GCS needed.
"""

import os, json, shutil, glob, zipfile, subprocess, sys, time
import threading, uuid, logging, warnings
import hashlib, secrets
from functools import wraps
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file, render_template

import numpy as np

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ============================================================
# Config - set these as Railway environment variables
# ============================================================
CLAUDE_API_KEY   = os.environ.get('CLAUDE_API_KEY', '')
YT_CLIENT_ID     = os.environ.get('YT_CLIENT_ID', '')
YT_CLIENT_SECRET = os.environ.get('YT_CLIENT_SECRET', '')

# ============================================================
# In-memory job store
# ============================================================
JOBS = {}  # job_id -> dict
SCHEDULES = {}  # schedule_id -> dict

def run_scheduled_jobs():
    """Background thread that checks and runs scheduled jobs every minute."""
    import time as t
    while True:
        try:
            now = datetime.now()
            for sid, schedule in list(SCHEDULES.items()):
                if not schedule.get('active'):
                    continue
                run_hour = int(schedule.get('hour', 9))
                run_minute = int(schedule.get('minute', 0))
                last_run = schedule.get('last_run', '')
                today = now.strftime('%Y-%m-%d')
                if (now.hour == run_hour and now.minute == run_minute and last_run != today):
                    logger.info(f'Running schedule {sid}: {schedule["query"]}')
                    SCHEDULES[sid]['last_run'] = today
                    job_id = str(uuid.uuid4())
                    params = {
                        'mode': schedule.get('mode', 'search'),
                        'search_query': schedule.get('query', ''),
                        'trending_topic': schedule.get('query', ''),
                        'date_filter': 'This Week',
                        'max_videos': int(schedule.get('max_videos', 3)),
                        'clip_length': int(schedule.get('clip_length', 45)),
                        'clips_per_video': int(schedule.get('clips_per_video', 3)),
                        'captions': schedule.get('captions', 'No'),
                        'caption_lang': 'en',
                        'watermark_enabled': schedule.get('watermark_enabled', 'No'),
                        'watermark_text': schedule.get('watermark_text', ''),
                        'watermark_position': 'bottom_right',
                        'ai_metadata': schedule.get('ai_metadata', 'No'),
                        'claude_api_key': schedule.get('claude_api_key', '') or CLAUDE_API_KEY,
                        'topic': schedule.get('query', ''),
                        'auto_upload': schedule.get('auto_upload', 'No'),
                        'yt_access_token': schedule.get('yt_access_token', ''),
                        'uploads_ready': True,
                    }
                    JOBS[job_id] = {
                        'id': job_id, 'status': 'queued',
                        'created_at': now.isoformat(),
                        'progress': 0, 'logs': [],
                        'uploads_ready': True,
                        'schedule_id': sid,
                        'schedule_name': schedule.get('name', '')
                    }
                    SCHEDULES[sid].setdefault('job_history', []).append(job_id)
                    mode = schedule.get('mode', 'search')
                    if mode == 'ai_content':
                        ai_params = {**params, 'topic': schedule.get('query', '')}
                        t2 = threading.Thread(target=process_ai_content_job, args=(job_id, ai_params), daemon=True)
                    elif mode == 'ai_news':
                        t2 = threading.Thread(target=process_ai_news_studio, args=(job_id, params), daemon=True)
                    else:
                        t2 = threading.Thread(target=process_job, args=(job_id, params), daemon=True)
                    t2.start()
        except Exception as e:
            logger.error(f'Scheduler error: {e}')
        t.sleep(60)

# Start scheduler background thread
_scheduler = threading.Thread(target=run_scheduled_jobs, daemon=True)
_scheduler.start()

def update_job(job_id, data):
    if job_id not in JOBS:
        JOBS[job_id] = {}
    JOBS[job_id].update(data)

def get_job(job_id):
    return JOBS.get(job_id)

def add_log(job_id, msg):
    job = JOBS.setdefault(job_id, {})
    logs = job.get('logs', [])
    logs.append(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}')
    job['logs'] = logs[-200:]
    logger.info(f'[{job_id[:8]}] {msg}')

# ============================================================
# Platform detection
# ============================================================
def detect_platform(url):
    url = url.lower()
    if 'tiktok.com' in url:     return 'tiktok'
    if 'instagram.com' in url:  return 'instagram'
    if 'twitter.com' in url or 'x.com' in url: return 'twitter'
    if 'facebook.com' in url or 'fb.watch' in url: return 'facebook'
    return 'youtube'

def is_vertical(video_path):
    probe = subprocess.run(
        ['ffprobe','-v','quiet','-print_format','json','-show_streams', video_path],
        capture_output=True, text=True)
    streams = json.loads(probe.stdout).get('streams', [])
    vs = next((s for s in streams if s['codec_type'] == 'video'), None)
    if vs:
        w, h = int(vs.get('width', 1)), int(vs.get('height', 1))
        return h > w
    return False

# ============================================================
# Video info fetch (no download)
# ============================================================
def fetch_video_info(mode, search_query, youtube_url, other_urls,
                     date_filter, max_videos, trending_topic):
    videos = []
    date_map = {'Today':1,'This Week':7,'This Month':30,'This Year':365,'Any Time':None}
    days = date_map.get(date_filter)

    if mode in ['search', 'trending']:
        queries = (
            [f'{trending_topic} highlights today',
             f'best {trending_topic} this week',
             f'{trending_topic} viral moments']
            if mode == 'trending' else [search_query]
        )
        for q in queries:
            proxy = os.environ.get('PROXY_URL', '')
            cmd = ['yt-dlp','--dump-json','--no-playlist','--no-warnings',
                   '--flat-playlist',
                   '--no-check-certificates',
                   '--extractor-args', 'youtube:player_client=web',
                   ] + (['--proxy', proxy] if proxy else [])
            if days:
                cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
                cmd += ['--dateafter', cutoff]
            cmd.append(f'ytsearch{max_videos}:{q}')
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
            for line in r.stdout.strip().split('\n'):
                if not line.strip(): continue
                try:
                    info = json.loads(line)
                    vid_id = info.get('id', '')
                    if vid_id and not any(v['id'] == vid_id for v in videos):
                        videos.append({
                            'id': vid_id,
                            'title': info.get('title', 'Unknown')[:60],
                            'duration': int(info.get('duration') or 0),
                            'channel': info.get('uploader', 'Unknown'),
                            'thumbnail': info.get('thumbnail', ''),
                            'url': info.get('webpage_url') or f'https://youtube.com/watch?v={vid_id}',
                            'platform': 'youtube',
                            'view_count': info.get('view_count', 0) or 0
                        })
                except: continue
        if mode == 'trending':
            videos.sort(key=lambda x: x.get('view_count', 0), reverse=True)

    elif mode == 'url':
        proxy = os.environ.get('PROXY_URL', '')
        cmd = ['yt-dlp','--dump-json','--no-playlist','--no-warnings',
               '--no-check-certificates'] + (['--proxy', proxy] if proxy else []) + [youtube_url]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        for line in r.stdout.strip().split('\n'):
            if not line.strip(): continue
            try:
                info = json.loads(line)
                videos.append({
                    'id': info.get('id',''),
                    'title': info.get('title','Unknown')[:60],
                    'duration': int(info.get('duration') or 0),
                    'channel': info.get('uploader','Unknown'),
                    'thumbnail': info.get('thumbnail',''),
                    'url': youtube_url,
                    'platform': 'youtube',
                    'view_count': 0
                })
            except: continue

    elif mode == 'other':
        for url in other_urls:
            if not url.strip(): continue
            proxy = os.environ.get('PROXY_URL', '')
            proxy_args = ['--proxy', proxy] if proxy else []
            r = subprocess.run(['yt-dlp','--dump-json','--no-playlist','--no-warnings','--no-check-certificates'] + proxy_args + [url.strip()],
                capture_output=True, text=True, timeout=60)
            for line in r.stdout.strip().split('\n'):
                if not line.strip(): continue
                try:
                    info = json.loads(line)
                    videos.append({
                        'id': info.get('id',''),
                        'title': info.get('title','Unknown')[:60],
                        'duration': int(info.get('duration') or 0),
                        'channel': info.get('uploader','Unknown'),
                        'thumbnail': info.get('thumbnail',''),
                        'url': url.strip(),
                        'platform': detect_platform(url),
                        'view_count': 0
                    })
                except: continue

    return videos[:max_videos]

# ============================================================
# Core processing
# ============================================================
def get_duration(path):
    r = subprocess.run(['ffprobe','-v','quiet','-print_format','json','-show_format', path],
        capture_output=True, text=True)
    return float(json.loads(r.stdout)['format']['duration'])

def extract_audio(video_path, audio_path):
    result = subprocess.run([
        'ffmpeg','-i',video_path,'-vn','-ar','16000','-ac','1',
        '-t','300','-y',audio_path,'-loglevel','quiet'
    ], capture_output=True, timeout=120)
    if result.returncode != 0:
        # Re-encode fallback for corrupt containers
        subprocess.run([
            'ffmpeg','-i',video_path,'-vn',
            '-acodec','pcm_s16le','-ar','16000','-ac','1',
            '-t','300','-y',audio_path,'-loglevel','quiet'
        ], capture_output=True, timeout=120)

def detect_peaks(audio_path, sensitivity=0.75, min_gap=30):
    import librosa
    y, sr = librosa.load(audio_path, sr=16000, mono=True)
    frame_length = int(sr * 0.5)
    hop_length   = int(sr * 0.25)
    rms = librosa.feature.rms(y=y, frame_length=frame_length, hop_length=hop_length)[0]
    rms_norm  = (rms - rms.min()) / (rms.max() - rms.min() + 1e-8)
    threshold = np.percentile(rms_norm, sensitivity * 100)
    times = librosa.frames_to_time(np.arange(len(rms)), sr=sr, hop_length=hop_length)
    peaks, last_peak = [], -min_gap
    for i, (t, rv) in enumerate(zip(times, rms_norm)):
        if rv >= threshold and (t - last_peak) >= min_gap:
            window = rms_norm[max(0, i-4):i+4]
            if np.mean(window) >= threshold * 0.7:
                peaks.append(float(t))
                last_peak = t
    return peaks

def cut_vertical(video_path, start, length, out_path,
                 already_vertical=False, watermark_text='', watermark_position='bottom_right'):
    start = max(0, start - length * 0.3)
    if already_vertical:
        vf_base = 'scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black'
    else:
        probe = subprocess.run(['ffprobe','-v','quiet','-print_format','json','-show_streams', video_path],
            capture_output=True, text=True)
        streams = json.loads(probe.stdout).get('streams', [])
        vs = next((s for s in streams if s['codec_type'] == 'video'), None)
        w, h = (int(vs['width']), int(vs['height'])) if vs else (1920, 1080)
        crop = min(w, h)
        cx, cy = (w - crop) // 2, (h - crop) // 2
        vf_base = f'crop={crop}:{crop}:{cx}:{cy},scale=1080:1080,pad=1080:1920:0:(oh-ih)/2:black'

    # Always add XLAB brand - subtle top right
    xlab_brand = "drawtext=text='XLAB':fontsize=24:fontcolor=white:alpha=0.4:x=w-tw-16:y=16:fontweight=bold"

    if watermark_text.strip():
        vf   = f'{vf_base},{xlab_brand},{wm}'
    else:
        vf   = f'{vf_base},{xlab_brand}'
    subprocess.run([
        'ffmpeg', '-ss', str(start), '-i', video_path,
        '-t', str(length), '-vf', vf,
        '-c:v', 'libx264', '-preset', 'fast', '-crf', '23',
        '-c:a', 'aac', '-b:a', '128k',
        '-y', out_path, '-loglevel', 'quiet'
    ], check=True)

def format_ts(s):
    h, m, sec, ms = int(s//3600), int((s%3600)//60), int(s%60), int((s%1)*1000)
    return f'{h:02d}:{m:02d}:{sec:02d},{ms:03d}'

def burn_captions(clip_path, out_path, lang='en'):
    import whisper
    model  = whisper.load_model('base')
    result = model.transcribe(clip_path, language=lang)
    srt    = clip_path.replace('.mp4', '.srt')
    with open(srt, 'w') as f:
        for i, seg in enumerate(result['segments']):
            f.write(f"{i+1}\n{format_ts(seg['start'])} --> {format_ts(seg['end'])}\n{seg['text'].strip()}\n\n")
    subprocess.run([
        'ffmpeg', '-i', clip_path,
        '-vf', f"subtitles={srt}:force_style='Fontsize=18,Bold=1,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,Outline=2'",
        '-c:a', 'copy', '-y', out_path, '-loglevel', 'quiet'
    ], check=True)
    os.remove(srt)

def add_trending_music(clip_path, out_path, music_style='energetic', volume=0.3):
    """Add royalty-free trending music to a clip using ffmpeg.
    Music styles: energetic, hype, chill, dramatic
    Volume: 0.0-1.0 (default 0.3 to keep original audio audible)
    """
    import urllib.request as urlreq

    # Royalty-free music URLs by style (from Free Music Archive / Pixabay)
    music_tracks = {
        'energetic': [
            'https://cdn.pixabay.com/download/audio/2022/01/18/audio_d0c6ff1bab.mp3',
            'https://cdn.pixabay.com/download/audio/2021/11/25/audio_5bdc1b2b5b.mp3',
        ],
        'hype': [
            'https://cdn.pixabay.com/download/audio/2022/03/15/audio_942571cd04.mp3',
            'https://cdn.pixabay.com/download/audio/2022/08/02/audio_884fe92c21.mp3',
        ],
        'chill': [
            'https://cdn.pixabay.com/download/audio/2022/05/27/audio_1808fbf07a.mp3',
            'https://cdn.pixabay.com/download/audio/2021/08/04/audio_0625a2a5d8.mp3',
        ],
        'dramatic': [
            'https://cdn.pixabay.com/download/audio/2022/10/25/audio_aee0a35f23.mp3',
            'https://cdn.pixabay.com/download/audio/2022/01/20/audio_d39a4c7eb6.mp3',
        ],
    }

    tracks = music_tracks.get(music_style, music_tracks['energetic'])
    music_url = tracks[0]
    music_path = f'/tmp/music_{music_style}.mp3'

    try:
        # Download music if not cached
        if not os.path.exists(music_path):
            urlreq.urlretrieve(music_url, music_path)

        # Mix music with original audio
        # -filter_complex: mix original audio + music at set volume
        cmd = [
            'ffmpeg', '-i', clip_path, '-i', music_path,
            '-filter_complex',
            f'[0:a]volume=1.0[orig];[1:a]volume={volume}[music];[orig][music]amix=inputs=2:duration=first:dropout_transition=2[aout]',
            '-map', '0:v', '-map', '[aout]',
            '-c:v', 'copy', '-c:a', 'aac', '-b:a', '192k',
            '-shortest', '-y', out_path, '-loglevel', 'quiet'
        ]
        subprocess.run(cmd, check=True, timeout=120)

        if os.path.exists(out_path) and os.path.getsize(out_path) > 10000:
            return True
        return False

    except Exception as e:
        logger.error(f'Music error: {e}')
        return False


# ============================================================
# LEVEL 3 - AI Content Creation
# ============================================================

def generate_ai_script(topic, num_points, duration_per_point, api_key):
    """Use Claude to write a structured video script for any topic."""
    import requests as req

    if not api_key:
        # Fallback script
        return {
            'title': topic,
            'hook': f'You need to know about {topic}',
            'points': [{'title': f'Point {i+1}', 'search_query': topic, 'narration': f'Here is point {i+1} about {topic}'} for i in range(num_points)],
            'outro': 'Follow for more content like this'
        }

    prompt = f"""You are a viral YouTube Shorts scriptwriter. Write a script for this topic:

Topic: "{topic}"
Number of points/clips: {num_points}
Duration per clip: {duration_per_point} seconds

The script should be punchy, engaging, and optimized for YouTube Shorts virality.

Respond ONLY with valid JSON, no markdown:
{{
  "title": "catchy video title with emoji under 60 chars",
  "hook": "opening line spoken in first 2 seconds, must grab attention immediately",
  "points": [
    {{
      "title": "point title",
      "search_query": "specific YouTube search query to find matching footage (be specific e.g. 'Ronaldo bicycle kick Champions League 2018')",
      "narration": "2-3 sentences spoken over this clip, punchy and engaging",
      "duration": {duration_per_point}
    }}
  ],
  "outro": "closing call to action, max 1 sentence",
  "hashtags": ["#Shorts", "5 more relevant hashtags"]
}}"""

    try:
        # Use Grok for script generation
        grok_key = os.environ.get('GROK_API_KEY', '').strip()
        if grok_key:
            text = call_grok(prompt, grok_key)
        else:
            text = None
        
        if not text:
            # Fallback to Gemini/Claude API key if provided
            r = req.post('https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent',
                params={'key': api_key},
                json={'contents': [{'parts': [{'text': prompt}]}]},
                timeout=30)
            data = r.json()
            text = data['candidates'][0]['content']['parts'][0]['text']
        
        text = text.replace('```json','').replace('```','').strip()
        start = text.find('{')
        end = text.rfind('}') + 1
        if start >= 0 and end > start:
            text = text[start:end]
        return json.loads(text)
    except Exception as e:
        logger.error(f'Script generation error: {e}')
        # Return fallback script so generation continues
        return {
            'title': topic[:60],
            'hook': f'You need to see this about {topic}',
            'points': [{'title': f'Scene {i+1}', 'search_query': topic, 'narration': f'Here is what you need to know about {topic}', 'duration': duration_per_point} for i in range(num_points)],
            'outro': 'Follow for more',
            'hashtags': ['#Shorts', '#AI', '#Tech']
        }


def fetch_pexels_footage(query, work_dir, idx, api_key=None):
    """Fetch free HD video footage from Pexels.
    API key free at pexels.com/api - 200 requests/hour free.
    """
    import requests as req
    key = api_key or os.environ.get('PEXELS_API_KEY', '')
    if not key:
        return None, None
    try:
        r = req.get(
            'https://api.pexels.com/videos/search',
            headers={'Authorization': key},
            params={'query': query, 'per_page': 5, 'orientation': 'portrait'},
            timeout=10
        )
        data = r.json()
        videos = data.get('videos', [])
        if not videos:
            # Try landscape and crop to portrait
            r2 = req.get(
                'https://api.pexels.com/videos/search',
                headers={'Authorization': key},
                params={'query': query, 'per_page': 5},
                timeout=10
            )
            videos = r2.json().get('videos', [])

        for video in videos:
            # Get best quality file
            files = sorted(video.get('video_files', []),
                          key=lambda x: x.get('height', 0), reverse=True)
            for f in files:
                if f.get('height', 0) >= 720:
                    vid_url = f.get('link', '')
                    if vid_url:
                        # Download
                        dl_path = os.path.join(work_dir, f'pexels_{idx}.mp4')
                        r3 = req.get(vid_url, stream=True, timeout=60)
                        with open(dl_path, 'wb') as fp:
                            for chunk in r3.iter_content(chunk_size=65536):
                                if chunk: fp.write(chunk)
                        if os.path.exists(dl_path) and os.path.getsize(dl_path) > 100000:
                            logger.info(f'Pexels footage: {dl_path}')
                            return dl_path, 'pexels'
        return None, None
    except Exception as e:
        logger.error(f'Pexels error: {e}')
        return None, None


def fetch_internet_archive_footage(query, work_dir, idx):
    """Fetch free public domain footage from Internet Archive.
    Perfect for historical conspiracy content.
    No API key needed.
    """
    import requests as req
    try:
        # Search Internet Archive
        r = req.get(
            'https://archive.org/advancedsearch.php',
            params={
                'q': f'{query} mediatype:movies',
                'fl': 'identifier,title',
                'rows': 5,
                'output': 'json'
            },
            timeout=10
        )
        docs = r.json().get('response', {}).get('docs', [])
        
        for doc in docs:
            identifier = doc.get('identifier', '')
            if not identifier:
                continue
            
            # Get metadata for this item
            meta_r = req.get(
                f'https://archive.org/metadata/{identifier}',
                timeout=10
            )
            meta = meta_r.json()
            files = meta.get('files', [])
            
            # Find best video file
            for f in files:
                name = f.get('name', '')
                if name.endswith('.mp4') and int(f.get('size', 0)) < 100*1024*1024:  # under 100MB
                    vid_url = f'https://archive.org/download/{identifier}/{name}'
                    dl_path = os.path.join(work_dir, f'archive_{idx}.mp4')
                    
                    r2 = req.get(vid_url, stream=True, timeout=120)
                    downloaded = 0
                    with open(dl_path, 'wb') as fp:
                        for chunk in r2.iter_content(chunk_size=65536):
                            if chunk:
                                fp.write(chunk)
                                downloaded += len(chunk)
                            if downloaded > 50*1024*1024:  # Stop at 50MB
                                break
                    
                    if os.path.exists(dl_path) and os.path.getsize(dl_path) > 100000:
                        logger.info(f'Archive footage: {dl_path}')
                        return dl_path, 'archive'
        return None, None
    except Exception as e:
        logger.error(f'Archive error: {e}')
        return None, None


def fetch_pexels_images(query, work_dir, idx, api_key=None):
    """Fetch free images from Pexels for Ken Burns slideshow."""
    import requests as req
    key = api_key or os.environ.get('PEXELS_API_KEY', '')
    if not key:
        return []
    try:
        r = req.get(
            'https://api.pexels.com/v1/search',
            headers={'Authorization': key},
            params={'query': query, 'per_page': 6},
            timeout=10
        )
        photos = r.json().get('photos', [])
        urls = [p['src']['large'] for p in photos if p.get('src')]
        logger.info(f'Pexels images: {len(urls)} for "{query}"')
        return urls
    except Exception as e:
        logger.error(f'Pexels images error: {e}')
        return []


def fetch_github_images(repo_url):
    """Fetch demo images and GIFs from a GitHub repo README."""
    import requests as req
    import re

    try:
        # Extract owner/repo from URL
        match = re.search(r'github\.com/([^/]+/[^/\s]+)', repo_url)
        if not match:
            return []
        repo = match.group(1).rstrip('/')

        # Fetch README via GitHub API
        r = req.get(
            f'https://api.github.com/repos/{repo}/readme',
            headers={'Accept': 'application/vnd.github.raw'},
            timeout=10
        )
        if r.status_code != 200:
            return []

        readme = r.text
        images = []

        # Find all image URLs in README
        # Match markdown images ![...](url)
        for match in re.finditer(r'!\[.*?\]\((https?://[^\)]+)\)', readme):
            url = match.group(1)
            if any(ext in url.lower() for ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp']):
                images.append(url)

        # Also find raw GitHub image URLs
        for match in re.finditer(r'(https://(?:raw\.githubusercontent\.com|github\.com/[^/]+/[^/]+/(?:raw|blob))/[^\s\)"]+\.(?:png|jpg|gif|jpeg))', readme):
            url = match.group(1).replace('/blob/', '/raw/')
            if url not in images:
                images.append(url)

        logger.info(f'GitHub images found for {repo}: {len(images)}')
        return images[:6]  # max 6 images

    except Exception as e:
        logger.error(f'GitHub image fetch error: {e}')
        return []


# Standard output specs for all XLAB videos
XLAB_WIDTH = 1080
XLAB_HEIGHT = 1920  # 9:16 vertical for Shorts
XLAB_FPS = 30
XLAB_CRF = 22


def normalize_clip(input_path, output_path, color_grade=True):
    """Normalize to XLAB standard specs with optional color grading.
    720p for avatar clips (Aurora native), 1080p for demo footage.
    """
    try:
        # Color grading filter for premium look
        # Slight contrast boost, warm shadows, cool highlights
        grade = (
            "eq=contrast=1.08:brightness=0.02:saturation=1.1,"
            "curves=r='0/0 0.5/0.52 1/1':g='0/0 0.5/0.5 1/1':b='0/0 0.5/0.48 1/0.97'"
        ) if color_grade else ""

        vf_parts = [
            f"scale={XLAB_WIDTH}:{XLAB_HEIGHT}:force_original_aspect_ratio=increase",
            f"crop={XLAB_WIDTH}:{XLAB_HEIGHT}",
            f"fps={XLAB_FPS}",
        ]
        if grade:
            vf_parts.append(grade)
        
        vf = ",".join(vf_parts)

        result = subprocess.run([
            'ffmpeg', '-i', input_path,
            '-vf', vf,
            '-c:v', 'libx264', '-preset', 'fast', '-crf', str(XLAB_CRF),
            '-c:a', 'aac', '-b:a', '128k', '-ar', '44100', '-ac', '2',
            '-movflags', '+faststart',
            '-pix_fmt', 'yuv420p',
            '-y', output_path, '-loglevel', 'quiet'
        ], capture_output=True, timeout=120)

        if os.path.exists(output_path) and os.path.getsize(output_path) > 50000:
            return True
        logger.error(f'Normalize failed: {result.stderr[:100]}')
        return False
    except Exception as e:
        logger.error(f'Normalize error: {e}')
        return False


def compile_highlights_from_multiple(search_queries, work_dir, output_path, target_duration=30):
    """Download multiple videos and compile the most engaging moments.
    This is the Opus Clip approach - find viral moments from existing content."""
    import requests as req

    rapidapi_key = os.environ.get('RAPIDAPI_KEY', '')
    if not rapidapi_key:
        return False

    all_clips = []
    seconds_per_source = target_duration // len(search_queries)

    # Rewrite queries to specifically find screen recordings
    screen_queries = []
    for q in search_queries[:3]:
        # Strip generic words and add screen recording terms
        base = q.replace(' tutorial', '').replace(' demo', '').replace(' review 2025', '').replace(' how to use', '').strip()
        screen_queries += [
            f'{base} screen recording walkthrough',
            f'{base} how to use tutorial 2025',
            f'{base} demo review',
        ]
    search_queries = screen_queries[:3]

    for qi, query in enumerate(search_queries[:3]):
        try:
            logger.info(f'Searching: {query[:50]}')
            # Search YouTube via RapidAPI
            r = req.get(
                'https://youtube-media-downloader.p.rapidapi.com/v2/search/videos',
                params={'query': query, 'hl': 'en', 'gl': 'US'},
                headers={
                    'x-rapidapi-key': rapidapi_key,
                    'x-rapidapi-host': 'youtube-media-downloader.p.rapidapi.com'
                },
                timeout=15
            )
            data = r.json()
            items = data.get('items', [])
            logger.info(f'Search "{query[:30]}": {len(items)} results')

            # Try top 3 results until one downloads
            for item in items[:3]:
                vid_id = item.get('id', '')
                if not vid_id:
                    continue

                vid_url = f'https://youtube.com/watch?v={vid_id}'
                dl_path = download_via_rapidapi(vid_url, work_dir, f'src_{qi}_{vid_id[:6]}')

                if not dl_path:
                    continue

                # Find the most engaging moment using peak audio detection
                dur = get_duration(dl_path)
                if dur < 10:
                    continue

                try:
                    # Extract audio for peak detection
                    audio_path = f'{work_dir}/audio_{qi}.wav'
                    subprocess.run([
                        'ffmpeg', '-i', dl_path, '-vn', '-ar', '16000', '-ac', '1',
                        '-t', '300', '-y', audio_path, '-loglevel', 'quiet'
                    ], capture_output=True, timeout=60)

                    best_start = 0
                    if os.path.exists(audio_path):
                        peaks = detect_peaks(audio_path, sensitivity=0.7)
                        if peaks:
                            # Skip first 20% (intro) and last 10% (outro)
                            min_start = dur * 0.20
                            max_start = dur * 0.75
                            good_peaks = [p for p in peaks 
                                         if min_start < p < max_start]
                            if good_peaks:
                                # Pick peak with highest energy - most action
                                best_start = good_peaks[0]
                            elif peaks:
                                best_start = max(peaks[0], min_start)
                    
                    # For screen recording tutorials, middle section has best demo
                    if best_start == 0 and dur > 60:
                        best_start = dur * 0.25  # start at 25%

                    # Cut the highlight clip
                    clip_path = f'{work_dir}/highlight_{qi}.mp4'
                    cut_vertical(dl_path, best_start, seconds_per_source + 2, clip_path, is_vertical(dl_path))

                    if os.path.exists(clip_path) and os.path.getsize(clip_path) > 100000:
                        all_clips.append(clip_path)
                        logger.info(f'✅ Highlight from "{item.get("title","")[:30]}" at {best_start:.0f}s')
                        break  # Got a clip from this query, move to next

                except Exception as e:
                    logger.error(f'Highlight extraction error: {e}')
                    continue

        except Exception as e:
            logger.error(f'Search error for "{query}": {e}')

    if not all_clips:
        return False

    # Concatenate all highlights
    logger.info(f'Combining {len(all_clips)} highlight clips')
    return concatenate_clips(all_clips, output_path)


def fetch_best_visuals(item, work_dir, idx):
    """Get the best visuals for an AI news item - tries multiple sources."""
    import requests as req

    # Source 1: Compile highlights - search specifically for screen recordings
    title = item.get('title', '')
    base_query = item.get('search_query', title)
    search_queries = [
        base_query + ' screen recording walkthrough',
        base_query + ' tutorial how to use 2025',
        title + ' demo review screen',
    ]
    logger.info(f'Compiling highlights from multiple sources: {title[:40]}')
    compiled_path = f'{work_dir}/compiled_{idx}.mp4'
    if compile_highlights_from_multiple(search_queries, work_dir, compiled_path):
        return compiled_path, 'youtube'

    # Source 2: X post images
    x_images = item.get('images', [])
    if x_images:
        img_vid = f'{work_dir}/ximg_{idx}.mp4'
        if create_video_from_images(x_images, img_vid):
            return img_vid, 'x_images'

    # Source 3: GitHub README images
    text = item.get('script', '') + item.get('hook', '') + item.get('x_source', '')
    github_match = __import__('re').search(r'github\.com/[^\s\)"]+', text)
    if github_match:
        github_url = 'https://' + github_match.group(0)
        gh_images = fetch_github_images(github_url)
        if gh_images:
            gh_vid = f'{work_dir}/ghimg_{idx}.mp4'
            if create_video_from_images(gh_images, gh_vid):
                return gh_vid, 'github'

    return None, None


def create_video_from_images(image_urls, output_path, duration_each=8):
    """Create a video from X post images using Ken Burns effect (zoom/pan).
    Each image gets animated to create engaging motion."""
    import requests as req
    import tempfile

    if not image_urls:
        return False

    try:
        work_dir = tempfile.mkdtemp()
        img_paths = []

        # Download images
        for i, url in enumerate(image_urls[:4]):
            try:
                r = req.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
                if r.status_code == 200:
                    ext = 'jpg' if 'jpg' in url or 'jpeg' in url else 'png'
                    img_path = os.path.join(work_dir, f'img_{i}.{ext}')
                    with open(img_path, 'wb') as f:
                        f.write(r.content)
                    img_paths.append(img_path)
            except Exception as e:
                logger.error(f'Image download error: {e}')

        if not img_paths:
            return False

        # Create video clips from each image with Ken Burns effect
        clip_paths = []
        for i, img_path in enumerate(img_paths):
            clip_path = os.path.join(work_dir, f'clip_{i}.mp4')
            # Ken Burns: slow zoom in effect
            vf = (
                f"scale=1080:1920:force_original_aspect_ratio=increase,"
                f"crop=1080:1920,"
                f"zoompan=z='min(zoom+0.0015,1.3)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
                f":d={duration_each*25}:s=1080x1920:fps=25"
            )
            result = subprocess.run([
                'ffmpeg', '-loop', '1', '-i', img_path,
                '-vf', vf,
                '-t', str(duration_each),
                '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
                '-pix_fmt', 'yuv420p',
                '-y', clip_path, '-loglevel', 'quiet'
            ], capture_output=True, timeout=60)
            if os.path.exists(clip_path) and os.path.getsize(clip_path) > 10000:
                clip_paths.append(clip_path)

        if not clip_paths:
            return False

        # Concatenate all image clips
        return concatenate_clips(clip_paths, output_path)

    except Exception as e:
        logger.error(f'Image video creation error: {e}')
        return False


def add_text_overlays(video_path, output_path, title, hook, key_points=None, cta="Follow for daily AI tools"):
    """Add TikTok-style text overlays to video using ffmpeg drawtext."""
    try:
        # Build drawtext filters
        filters = []
        
        # Hook text - bold, top of screen, first 5 seconds
        safe_hook = re.sub(r"[':]", '', hook[:60])[:60]
        filters.append(
            f"drawtext=text='{safe_hook}':"
            f"fontsize=42:fontcolor=white:fontweight=bold:"
            f"x=(w-text_w)/2:y=h*0.12:"
            f"box=1:boxcolor=black@0.6:boxborderw=8:"
            f"enable='between(t,0,4)'"
        )
        
        # Title - middle of screen, seconds 1-3
        safe_title = re.sub(r"[':]", '', title[:50])[:60]
        filters.append(
            f"drawtext=text='{safe_title}':"
            f"fontsize=36:fontcolor=yellow:fontweight=bold:"
            f"x=(w-text_w)/2:y=h*0.45:"
            f"box=1:boxcolor=black@0.5:boxborderw=6:"
            f"enable='between(t,1,3)'"
        )
        
        # Key points - appear mid video
        if key_points:
            for i, point in enumerate(key_points[:3]):
                safe_point = re.sub(r"[':]", '', point[:50])[:60]
                t_start = 5 + (i * 7)
                t_end = t_start + 6
                filters.append(
                    f"drawtext=text='{safe_point}':"
                    f"fontsize=34:fontcolor=white:fontweight=bold:"
                    f"x=(w-text_w)/2:y=h*0.75:"
                    f"box=1:boxcolor=black@0.65:boxborderw=8:"
                    f"enable='between(t,{t_start},{t_end})'"
                )
        
        # CTA - last 4 seconds
        safe_cta = re.sub(r"[':]", '', cta[:50])[:60]
        filters.append(
            f"drawtext=text='{safe_cta}':"
            f"fontsize=36:fontcolor=white:fontweight=bold:"
            f"x=(w-text_w)/2:y=h*0.82:"
            f"box=1:boxcolor=rgba(230\,51\,41\,0.85):boxborderw=12:"
            f"enable='gte(t,26)'"
        )
        
        # XLAB watermark
        filters.append(
            "drawtext=text='XLAB':"
            "fontsize=22:fontcolor=white:alpha=0.4:"
            "x=w-tw-16:y=16:fontweight=bold"
        )
        
        vf = ','.join(filters)
        
        result = subprocess.run([
            'ffmpeg', '-i', video_path,
            '-vf', vf,
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
            '-c:a', 'copy',
            '-movflags', '+faststart',
            '-y', output_path, '-loglevel', 'quiet'
        ], capture_output=True, timeout=120)
        
        if os.path.exists(output_path) and os.path.getsize(output_path) > 100000:
            logger.info(f'Text overlays added successfully')
            return True
        else:
            logger.error(f'Text overlay failed: {result.stderr[:200]}')
            return False
    except Exception as e:
        logger.error(f'Text overlay error: {e}')
        return False


def post_to_x(video_path, text, api_key, api_secret, access_token, access_token_secret):
    """Post video to X (Twitter) using tweepy - free tier supports 1500 posts/month."""
    try:
        import tweepy

        # OAuth1 for media upload (v1.1 endpoint)
        auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
        api_v1 = tweepy.API(auth)

        # Upload video
        if video_path and os.path.exists(video_path):
            media = api_v1.media_upload(
                filename=video_path,
                media_category='tweet_video',
                chunked=True
            )
            media_id = media.media_id_string
            # Wait for video processing
            for _ in range(30):
                status = api_v1.get_media_upload_status(media_id)
                if status.processing_info.get('state') == 'succeeded':
                    break
                elif status.processing_info.get('state') == 'failed':
                    logger.error('X video processing failed')
                    media_id = None
                    break
                time.sleep(5)
        else:
            media_id = None

        # Post tweet with OAuth2 client
        client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret
        )

        tweet_params = {'text': text[:280]}
        if media_id:
            tweet_params['media_ids'] = [media_id]

        response = client.create_tweet(**tweet_params)
        tweet_id = response.data['id']
        logger.info(f'Posted to X: https://x.com/i/web/status/{tweet_id}')
        return tweet_id

    except Exception as e:
        logger.error(f'X post error: {e}')
        return None


def call_groq_free(prompt, api_key=None):
    """Call Groq API for text generation — free tier, no credits needed.
    Uses Llama 3.3 70B — excellent quality, 6000 requests/day free.
    """
    import requests as req
    key = (api_key or os.environ.get('GROQ_API_KEY', '')).strip()
    if not key:
        return None
    try:
        r = req.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
            json={
                'model': 'llama-3.3-70b-versatile',
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 2000,
                'temperature': 0.7
            },
            timeout=30
        )
        data = r.json()
        if 'choices' in data:
            return data['choices'][0]['message']['content']
        logger.error(f'Groq free error: {str(data)[:100]}')
        return None
    except Exception as e:
        logger.error(f'Groq free error: {e}')
        return None


def groq_tts(text, output_path, api_key=None, voice='zeus'):
    """Generate high quality TTS using Groq Orpheus — free tier.
    Supports emotion tags: [dramatic], [whisper], [cheerful], [dark chuckle]
    Voices: troy, austin, zeus, nova (English)
    """
    import requests as req
    key = (api_key or os.environ.get('GROQ_API_KEY', '')).strip()
    if not key:
        return False
    try:
        r = req.post(
            'https://api.groq.com/openai/v1/audio/speech',
            headers={'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'},
            json={
                'model': 'canopylabs/orpheus-v1-english',
                'input': text[:1000],
                'voice': voice,
                'response_format': 'wav'
            },
            timeout=30
        )
        if r.status_code == 200:
            with open(output_path, 'wb') as f:
                f.write(r.content)
            return os.path.exists(output_path) and os.path.getsize(output_path) > 1000
        logger.error(f'Groq TTS error: {r.status_code} {r.text[:100]}')
        return False
    except Exception as e:
        logger.error(f'Groq TTS error: {e}')
        return False


def call_gemini_free(prompt, api_key=None):
    """Call Gemini API - completely free tier available.
    Get key at aistudio.google.com
    """
    import requests as req
    key = (api_key or os.environ.get('GEMINI_API_KEY') or 
           os.environ.get('CLAUDE_API_KEY', '')).strip()
    if not key:
        return None
    try:
        r = req.post(
            f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={key}',
            json={'contents': [{'parts': [{'text': prompt}]}],
                  'generationConfig': {'maxOutputTokens': 2000, 'temperature': 0.7}},
            timeout=30
        )
        data = r.json()
        if 'candidates' in data:
            return data['candidates'][0]['content']['parts'][0]['text']
        logger.error(f'Gemini error: {str(data)[:100]}')
        return None
    except Exception as e:
        logger.error(f'Gemini error: {e}')
        return None


def call_grok(prompt, api_key):
    """Call Grok API with Gemini fallback when credits run out."""
    import requests as req
    if not api_key:
        return call_gemini_free(prompt)
    api_key = api_key.strip()
    
    # Try to get available models first
    for model in ['grok-3', 'grok-4', 'grok-2-1212', 'grok-beta']:
        try:
            r = req.post(
                'https://api.x.ai/v1/chat/completions',
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                json={'model': model, 'messages': [{'role': 'user', 'content': prompt}],
                      'max_tokens': 2000, 'temperature': 0.7},
                timeout=30
            )
            data = r.json()
            if 'choices' in data:
                return data['choices'][0]['message']['content']
            err = data.get('error', {})
            err_code = err.get('code', '') if isinstance(err, dict) else ''
            if err_code == 'permission-denied':
                # Credits exhausted - fall back to Gemini
                logger.warning('Grok credits exhausted - falling back to Gemini')
                return call_gemini_free(prompt)
            logger.error(f'Grok {model}: {str(data)[:100]}')
        except Exception as e:
            logger.error(f'Grok {model} error: {e}')
    
    # Try Groq free (groq.com — different from xAI Grok)
    groq_result = call_groq_free(prompt)
    if groq_result:
        return groq_result

    # Final fallback to Gemini
    logger.info('Falling back to Gemini for text generation')
    return call_gemini_free(prompt)


def grok_tts(text, output_path, api_key, voice='ara'):
    """Grok TTS - natural voice. $4.20/1M chars."""
    import requests as req
    if not api_key:
        return False
    try:
        r = req.post('https://api.x.ai/v1/audio/speech',
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
            json={'model': 'grok-tts', 'input': text, 'voice': voice, 'response_format': 'mp3'},
            timeout=30)
        if r.status_code == 200:
            with open(output_path, 'wb') as f:
                f.write(r.content)
            return os.path.exists(output_path) and os.path.getsize(output_path) > 1000
        logger.error(f'Grok TTS: {r.status_code} {r.text[:100]}')
        return False
    except Exception as e:
        logger.error(f'Grok TTS error: {e}')
        return False


def musetalk_generate_avatar(avatar_image_path, audio_path, output_path):
    """Generate talking avatar video using MuseTalk on Hugging Face Spaces.
    Completely free - no API key needed, just HF account optional.
    Input: avatar image + audio file
    Output: lip-synced talking video
    """
    try:
        from gradio_client import Client, handle_file
        import shutil

        # Try multiple MuseTalk spaces in order
        spaces = [
            "TMElyralab/MuseTalk",
            "kevinwang676/MuseTalk1.5", 
            "fffiloni/MuseTalk",
        ]
        
        hf_token = os.environ.get('HF_TOKEN', '')  # Optional - works without token too
        
        for space in spaces:
            try:
                logger.info(f'Trying MuseTalk space: {space}')
                client = Client(space, hf_token=hf_token if hf_token else None)
                
                result = client.predict(
                    avatar_image=handle_file(avatar_image_path),
                    audio_path=handle_file(audio_path),
                    api_name="/generate"
                )
                
                # Result is path to generated video
                if result and os.path.exists(str(result)):
                    shutil.copy2(str(result), output_path)
                    logger.info(f'MuseTalk success via {space}')
                    return True
                elif isinstance(result, (list, tuple)) and len(result) > 0:
                    vid = result[0] if isinstance(result[0], str) else result[-1]
                    if os.path.exists(str(vid)):
                        shutil.copy2(str(vid), output_path)
                        return True
                        
            except Exception as e:
                logger.warning(f'MuseTalk {space} failed: {str(e)[:80]}')
                continue
        
        return False
    except Exception as e:
        logger.error(f'MuseTalk error: {e}')
        return False


def generate_free_avatar_clip(text, avatar_image_path, output_path, work_dir):
    """Generate talking avatar clip completely free:
    1. gTTS converts text to speech (free)
    2. MuseTalk animates avatar with lip sync (free via HF Spaces)
    """
    try:
        # Step 1: Generate speech with gTTS
        audio_path = os.path.join(work_dir, 'avatar_speech.mp3')
        from gtts import gTTS
        tts = gTTS(text=text[:500], lang='en', slow=False)
        tts.save(audio_path)
        
        if not os.path.exists(audio_path):
            return False
        
        logger.info(f'Avatar speech generated: {os.path.getsize(audio_path)} bytes')
        
        # Step 2: Animate with MuseTalk
        if musetalk_generate_avatar(avatar_image_path, audio_path, output_path):
            return True
        
        # Fallback: just use audio over static image
        logger.info('MuseTalk failed - using static avatar with audio')
        result = subprocess.run([
            'ffmpeg',
            '-loop', '1', '-i', avatar_image_path,
            '-i', audio_path,
            '-vf', f'scale=540:960:force_original_aspect_ratio=increase,crop=540:960,fps=30',
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
            '-c:a', 'aac', '-b:a', '128k',
            '-shortest', '-movflags', '+faststart',
            '-pix_fmt', 'yuv420p',
            '-y', output_path, '-loglevel', 'quiet'
        ], capture_output=True, timeout=60)
        
        return os.path.exists(output_path) and os.path.getsize(output_path) > 10000
        
    except Exception as e:
        logger.error(f'Free avatar error: {e}')
        return False


def aurora_generate_image(prompt, api_key):
    """Generate an avatar image using Grok Aurora image generation."""
    import requests as req
    api_key = api_key.strip()
    try:
        r = req.post(
            'https://api.x.ai/v1/images/generations',
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
            json={
                'model': 'grok-2-image',
                'prompt': prompt,
                'n': 1,
                'response_format': 'url'
            },
            timeout=30
        )
        data = r.json()
        logger.info(f'Aurora image: {r.status_code} {str(data)[:100]}')
        if 'data' in data and data['data']:
            return data['data'][0].get('url')
        return None
    except Exception as e:
        logger.error(f'Aurora image error: {e}')
        return None


def aurora_image_to_video(image_path_or_url, audio_path, output_path, api_key, prompt="Person talking to camera, natural lip sync"):
    """Animate an avatar image with lip-synced speech using Grok Aurora image-to-video.
    Uses grok-imagine-video with image input for consistent character.
    Cost: ~$0.15 per 10s clip.
    """
    import requests as req
    import base64
    api_key = api_key.strip()
    if not api_key:
        return False
    try:
        # Read image as base64 if local file
        if os.path.exists(str(image_path_or_url)):
            with open(image_path_or_url, 'rb') as f:
                img_b64 = base64.b64encode(f.read()).decode()
            image_data = f'data:image/jpeg;base64,{img_b64}'
        else:
            image_data = image_path_or_url

        # Submit image-to-video generation
        r = req.post(
            'https://api.x.ai/v1/videos/generations',
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
            json={
                'model': 'grok-imagine-video',
                'prompt': prompt,
                'image': image_data,
                'duration': 10,
                'aspect_ratio': '9:16',
                'with_audio': True,
            },
            timeout=30
        )
        logger.info(f'Aurora i2v submit: {r.status_code} {r.text[:200]}')
        if r.status_code not in (200, 201, 202):
            return False

        data = r.json()
        request_id = data.get('request_id') or data.get('id')
        if not request_id:
            return False

        # Poll until ready
        for attempt in range(40):
            time.sleep(8)
            r2 = req.get(
                f'https://api.x.ai/v1/videos/{request_id}',
                headers={'Authorization': f'Bearer {api_key}'},
                timeout=15
            )
            if r2.status_code != 200:
                continue
            result = r2.json()
            status = result.get('status', '')
            logger.info(f'Aurora i2v status {attempt}: {status}')
            if status in ('done', 'succeeded', 'completed'):
                video_url = (result.get('video') or {}).get('url') or result.get('url')
                if video_url:
                    r3 = req.get(video_url, stream=True, timeout=120)
                    with open(output_path, 'wb') as f:
                        for chunk in r3.iter_content(chunk_size=65536):
                            if chunk: f.write(chunk)
                    return os.path.exists(output_path) and os.path.getsize(output_path) > 10000
                return False
            elif status in ('failed', 'cancelled', 'error'):
                logger.error(f'Aurora i2v failed: {result}')
                return False
        return False
    except Exception as e:
        logger.error(f'Aurora i2v error: {e}')
        return False


def create_split_screen(avatar_path, demo_path, output_path, ratio=0.35):
    """Create split screen: avatar on left, demo on right.
    ratio = fraction of width for avatar (0.35 = 35% avatar, 65% demo)
    """
    try:
        avatar_w = int(1080 * ratio)
        demo_w = 1080 - avatar_w

        result = subprocess.run([
            'ffmpeg',
            '-i', avatar_path,
            '-i', demo_path,
            '-filter_complex',
            f'[0:v]scale={avatar_w}:1920:force_original_aspect_ratio=increase,crop={avatar_w}:1920[av];'
            f'[1:v]scale={demo_w}:1920:force_original_aspect_ratio=increase,crop={demo_w}:1920[dv];'
            f'[av][dv]hstack=inputs=2[out]',
            '-map', '[out]',
            '-map', '0:a?',
            '-map', '1:a?',
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            '-y', output_path, '-loglevel', 'quiet'
        ], capture_output=True, timeout=120)

        return os.path.exists(output_path) and os.path.getsize(output_path) > 50000
    except Exception as e:
        logger.error(f'Split screen error: {e}')
        return False


def grok_generate_video(prompt, output_path, api_key, duration=10, aspect_ratio='9:16'):
    """Generate video using Grok Aurora. ~$0.15/clip.
    Correct endpoint: POST /v1/videos/generations, poll with request_id.
    """
    import requests as req
    api_key = api_key.strip()
    if not api_key:
        return False
    try:
        # Submit generation
        payload = {
            'model': 'grok-imagine-video',
            'prompt': prompt,
            'duration': duration,
            'aspect_ratio': aspect_ratio,
        }
        r = req.post(
            'https://api.x.ai/v1/videos/generations',
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
            json=payload,
            timeout=30
        )
        logger.info(f'Aurora submit: {r.status_code} {r.text[:200]}')
        if r.status_code not in (200, 201, 202):
            logger.error(f'Aurora submit failed: {r.status_code} {r.text[:200]}')
            return False
        data = r.json()
        # Get request_id for polling
        request_id = data.get('request_id') or data.get('id') or data.get('generation_id')
        if not request_id:
            logger.error(f'No request_id in Aurora response: {data}')
            return False
        logger.info(f'Aurora generation started: {request_id}')
        # Poll until done - usually 17-60 seconds
        for attempt in range(40):
            time.sleep(8)
            r2 = req.get(
                f'https://api.x.ai/v1/videos/{request_id}',
                headers={'Authorization': f'Bearer {api_key}'},
                timeout=15
            )
            if r2.status_code != 200:
                logger.warning(f'Aurora poll {attempt}: {r2.status_code}')
                continue
            result = r2.json()
            status = result.get('status', '')
            logger.info(f'Aurora status {attempt}: {status}')
            if status in ('done', 'succeeded', 'completed'):
                video_url = (result.get('video') or {}).get('url') or result.get('url')
                if not video_url:
                    # Try other response fields
                    for key in ['video_url', 'output', 'result']:
                        if result.get(key):
                            video_url = result[key] if isinstance(result[key], str) else result[key].get('url')
                            break
                if video_url:
                    logger.info(f'Aurora video ready: {video_url[:60]}')
                    r3 = req.get(video_url, stream=True, timeout=120,
                                headers={'Authorization': f'Bearer {api_key}'})
                    with open(output_path, 'wb') as f:
                        for chunk in r3.iter_content(chunk_size=65536):
                            if chunk: f.write(chunk)
                    size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                    logger.info(f'Aurora downloaded: {size/1024:.0f}KB')
                    if size > 10000:
                        # Re-encode for iPhone/mobile compatibility
                        compat_path = output_path + '.compat.mp4'
                        result = subprocess.run([
                            'ffmpeg', '-i', output_path,
                            '-c:v', 'libx264', '-preset', 'fast', '-crf', '23',
                            '-c:a', 'aac', '-b:a', '128k',
                            '-movflags', '+faststart',
                            '-pix_fmt', 'yuv420p',
                            '-y', compat_path, '-loglevel', 'quiet'
                        ], capture_output=True, timeout=120)
                        if os.path.exists(compat_path) and os.path.getsize(compat_path) > 10000:
                            os.replace(compat_path, output_path)
                            logger.info('Aurora re-encoded for iPhone compatibility')
                        return True
                    return False
                logger.error(f'Aurora done but no URL: {result}')
                return False
            elif status in ('failed', 'cancelled', 'error'):
                logger.error(f'Aurora generation failed: {result}')
                return False
        logger.error('Aurora timed out after 40 attempts')
        return False
    except Exception as e:
        logger.error(f'Aurora error: {e}')
        return False


def grok_trending_topics(niche, api_key):
    """Get trending topics from X using Grok real-time data."""
    prompt = f"""You have access to real-time X data.
What are the top 5 trending topics related to "{niche}" right now?
Respond ONLY with valid JSON:
{{"topics": [{{"trend": "topic", "video_idea": "specific video title", "search_query": "YouTube search", "why": "why trending"}}]}}"""
    text = call_grok(prompt, api_key)
    if not text:
        return None
    try:
        return json.loads(text.replace('```json','').replace('```','').strip())
    except:
        return None


def search_x_for_ai_news(categories=None):
    """Search X API for real trending AI posts from last 24hrs."""
    import requests as req
    from datetime import datetime, timedelta, timezone

    bearer = os.environ.get('X_BEARER_TOKEN', '') or os.environ.get('X_API_KEY', '')
    if not bearer:
        logger.warning('No X Bearer Token - skipping X search')
        return []

    queries = {
        'ai_tools': 'new AI tool launched -is:retweet lang:en',
        'github_hacks': 'github AI automation -is:retweet lang:en',
        'hustles': 'make money AI -is:retweet lang:en',
        'productivity': 'AI productivity hack -is:retweet lang:en',
        'tech_news': 'AI just released -is:retweet lang:en',
    }

    if not categories:
        categories = list(queries.keys())

    start_time = (datetime.now(timezone.utc) - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%SZ')
    posts = []

    for cat in categories[:3]:  # max 3 categories to stay in free tier
        query = queries.get(cat, 'new AI tool -is:retweet lang:en')
        try:
            r = req.get(
                'https://api.twitter.com/2/tweets/search/recent',
                headers={'Authorization': f'Bearer {bearer}'},
                params={
                    'query': query,
                    'max_results': 10,
                    'start_time': start_time,
                    'tweet.fields': 'public_metrics,created_at,text,attachments',
                    'expansions': 'author_id,attachments.media_keys',
                    'media.fields': 'url,preview_image_url,type',
                    'user.fields': 'username,public_metrics'
                },
                timeout=15
            )
            data = r.json()
            if 'data' in data:
                # Sort by engagement
                tweets = sorted(data['data'],
                    key=lambda t: t.get('public_metrics', {}).get('like_count', 0),
                    reverse=True)
                # Build media lookup from includes
                media_lookup = {}
                for m in data.get('includes', {}).get('media', []):
                    media_lookup[m['media_key']] = m.get('url') or m.get('preview_image_url', '')

                for t in tweets[:2]:  # top 2 per category
                    # Get image URLs from tweet
                    media_keys = t.get('attachments', {}).get('media_keys', [])
                    images = [media_lookup[k] for k in media_keys if k in media_lookup and media_lookup[k]]
                    posts.append({
                        'category': cat,
                        'text': t.get('text', ''),
                        'likes': t.get('public_metrics', {}).get('like_count', 0),
                        'retweets': t.get('public_metrics', {}).get('retweet_count', 0),
                        'images': images,
                    })
            elif 'errors' in data:
                logger.error(f'X API error for {cat}: {data["errors"]}')
        except Exception as e:
            logger.error(f'X search error for {cat}: {e}')

    logger.info(f'X search found {len(posts)} posts')
    return posts


# ============================================================
# UNIVERSAL CONTENT FINDER
# ============================================================

NICHE_CONFIGS = {
    'ai_tools': {
        'name': 'AI Tools',
        'emoji': '🤖',
        'hooks': ['This AI just dropped and nobody knows about it', 'Big tech doesnt want you using this', 'This free AI replaces a $500/month tool'],
        'x_queries': ['new AI tool launched', 'AI just released', 'free AI tool'],
        'youtube_queries': ['AI tool tutorial 2025', 'new AI software demo'],
        'angle': 'productivity and money making'
    },
    'conspiracy': {
        'name': 'Hidden History',
        'emoji': '👀',
        'hooks': [
            'NOBODY WANTS TO TALK ABOUT THIS',
            'SINCE NOBODY IS SAYING IT I WILL',
            'THEY NEVER TAUGHT YOU THIS IN SCHOOL',
            'THIS WAS HIDDEN FOR 50 YEARS',
            'THE HISTORY BOOKS LIED TO YOU'
        ],
        'x_queries': ['declassified history secret', 'hidden historical fact'],
        'youtube_queries': ['hidden history documentary', 'declassified secrets explained'],
        'angle': 'shocking suppressed historical truths and facts nobody talks about',
        'topic_categories': [
            'suppressed historical inventions and discoveries',
            'declassified government secrets and operations',
            'historical events mainstream media ignores',
            'ancient civilizations and lost knowledge',
            'corporate and government cover ups',
            'banned books and censored information',
            'colonial America shocking laws and facts',
            'immigration and diversity hidden history',
            'famous people dark secrets history never teaches',
            'money and banking system hidden truths',
            'food and pharmaceutical industry secrets',
            'space and NASA declassified documents',
            'war secrets governments never admitted',
            'religion and church hidden history',
        ]
    },
    'finance': {
        'name': 'Finance Secrets',
        'emoji': '💰',
        'hooks': ['The rich use this and never talk about it', 'This financial trick is completely legal', 'Banks dont want you to know this'],
        'x_queries': ['financial hack money saving', 'investing strategy nobody talks about'],
        'youtube_queries': ['financial secret explained', 'money hack tutorial'],
        'angle': 'making and saving money'
    },
    'hustle': {
        'name': 'AI Hustles',
        'emoji': '🚀',
        'hooks': ['I made $X using this in 24 hours', 'This side hustle prints money', 'Nobody is doing this yet'],
        'x_queries': ['AI side hustle money', 'make money online AI 2025'],
        'youtube_queries': ['AI money making tutorial', 'side hustle with AI'],
        'angle': 'making money with AI tools'
    },
    'tech_news': {
        'name': 'Tech News',
        'emoji': '📡',
        'hooks': ['This just happened and nobody noticed', 'Big tech just changed everything', 'This tech dropped yesterday'],
        'x_queries': ['tech announcement just released', 'startup launched product'],
        'youtube_queries': ['tech news today', 'new technology 2025'],
        'angle': 'underreported tech stories'
    },
    'health': {
        'name': 'Health Facts',
        'emoji': '🧠',
        'hooks': ['Scientists just discovered this', 'Your doctor wont tell you this', 'This changes everything we knew'],
        'x_queries': ['health study results 2025', 'medical discovery'],
        'youtube_queries': ['health fact explained', 'science discovery 2025'],
        'angle': 'health and wellness facts'
    },
    'crypto': {
        'name': 'Crypto Alpha',
        'emoji': '₿',
        'hooks': ['This crypto move nobody is making', 'The next 100x nobody sees coming', 'Insiders are quietly buying this'],
        'x_queries': ['crypto alpha signal', 'DeFi opportunity 2025'],
        'youtube_queries': ['crypto tutorial 2025', 'blockchain explained'],
        'angle': 'crypto opportunities'
    },
    'gaming': {
        'name': 'Gaming Secrets',
        'emoji': '🎮',
        'hooks': ['This game secret was hidden for years', 'Developers dont want you doing this', 'This trick breaks the game'],
        'x_queries': ['game secret discovered', 'gaming hack trick'],
        'youtube_queries': ['game secret explained', 'hidden gaming trick'],
        'angle': 'gaming secrets and tricks'
    }
}


def find_content_for_niche(niche_key, api_key, max_items=3):
    """Find fresh viral content for any niche using Grok + X API."""
    config = NICHE_CONFIGS.get(niche_key, NICHE_CONFIGS['ai_tools'])
    
    # Search X for real posts in this niche
    x_posts = []
    bearer = os.environ.get('X_BEARER_TOKEN', '').strip()
    if bearer:
        import requests as req
        from datetime import datetime, timedelta, timezone
        start_time = (datetime.now(timezone.utc) - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%SZ')
        
        for query in config['x_queries'][:2]:
            try:
                r = req.get(
                    'https://api.twitter.com/2/tweets/search/recent',
                    headers={'Authorization': f'Bearer {bearer}'},
                    params={
                        'query': query + ' -is:retweet lang:en',
                        'max_results': 10,
                        'start_time': start_time,
                        'tweet.fields': 'public_metrics,text,attachments',
                        'expansions': 'attachments.media_keys',
                        'media.fields': 'url,preview_image_url,type'
                    },
                    timeout=15
                )
                data = r.json()
                if 'data' in data:
                    media_lookup = {m['media_key']: m.get('url') or m.get('preview_image_url','')
                                   for m in data.get('includes',{}).get('media',[])}
                    for t in sorted(data['data'],
                                   key=lambda x: x.get('public_metrics',{}).get('like_count',0),
                                   reverse=True)[:2]:
                        media_keys = t.get('attachments',{}).get('media_keys',[])
                        images = [media_lookup[k] for k in media_keys if k in media_lookup]
                        x_posts.append({
                            'text': t.get('text',''),
                            'images': images,
                            'likes': t.get('public_metrics',{}).get('like_count',0)
                        })
            except Exception as e:
                logger.error(f'X search error: {e}')

    # Build context
    x_context = ''
    if x_posts:
        x_context = '\nReal trending ' + config['name'] + ' posts from X (last 48hrs):\n'
        for p in x_posts[:4]:
            img_note = ' [has ' + str(len(p.get('images',[]))) + ' images]' if p.get('images') else ''
            x_context += '- ' + p['text'][:150] + ' (likes:' + str(p['likes']) + ')' + img_note + '\n'

    # Generate content with Grok
    hooks_example = config['hooks'][0]
    prompt = f"""You are a viral content creator for the "{config['name']}" niche.
Find {max_items} pieces of content that would go viral on TikTok/YouTube Shorts.
Focus on: {config['angle']}
Hook style: "{hooks_example}"
{x_context}

Reply ONLY with valid JSON:
{{"items": [{{"category": "{niche_key}", "title": "HOOK IN CAPS under 60 chars", "hook": "Opening line that grabs in 2 seconds", "script": "Punchy 30 second script, factual, exciting", "search_query": "Specific YouTube search for demo/footage", "images": [], "key_facts": ["fact1", "fact2", "fact3"], "hashtags": ["#{config['name'].replace(' ','')}","#Shorts","#viral"], "why_viral": "Why this will get views"}}]}}"""

    # Try Grok first, then Gemini free fallback
    text = call_grok(prompt, api_key)
    if not text:
        logger.info('Grok failed - trying Gemini free')
        text = call_gemini_free(prompt)
    logger.info(f'Content finder response: {text[:200] if text else "None"}')
    
    if not text:
        return None
    
    try:
        text = text.replace('```json','').replace('```','').strip()
        start = text.find('{')
        end = text.rfind('}') + 1
        if start >= 0 and end > start:
            text = text[start:end]
        result = json.loads(text)
        
        # Enrich with X images
        if result.get('items'):
            for item in result['items']:
                if not item.get('images') and x_posts:
                    for xp in x_posts:
                        if xp.get('images'):
                            item['images'] = xp['images']
                            break
            return result
    except Exception as e:
        logger.error(f'Content parse error: {e}')
    return None


def process_universal_studio(job_id, params):
    """Generate viral content for ANY niche - the universal content machine."""
    work_dir = f'/tmp/universal_{job_id}'
    os.makedirs(work_dir, exist_ok=True)

    try:
        update_job(job_id, {'status': 'processing', 'started_at': datetime.now().isoformat()})

        grok_key = os.environ.get('GROK_API_KEY', '').strip()
        niche = params.get('niche', 'ai_tools')
        max_videos = int(params.get('max_videos', 3))
        auto_upload = params.get('auto_upload') == 'Yes'
        yt_token = params.get('yt_access_token', '')
        config = NICHE_CONFIGS.get(niche, NICHE_CONFIGS['ai_tools'])

        if not grok_key:
            raise Exception('Grok API key required')

        add_log(job_id, f'{config["emoji"]} Finding viral {config["name"]} content...')
        update_job(job_id, {'progress': 5})

        data = find_content_for_niche(niche, grok_key, max_videos)
        if not data or not data.get('items'):
            raise Exception(f'No content found for {config["name"]}')

        items = data['items'][:max_videos]
        add_log(job_id, f'✅ Found {len(items)} stories')
        for item in items:
            add_log(job_id, f'   • {item["title"][:55]}')

        produced = []
        avatar_path = '/tmp/xlab_avatar.jpg'
        if not os.path.exists(avatar_path):
            avatar_path = None

        for idx, item in enumerate(items):
            add_log(job_id, config['emoji'] + ' Story ' + str(idx+1) + ': ' + item['title'][:50])
            update_job(job_id, {'progress': 10 + int((idx/len(items))*80)})

            # Build conspiracy Short with mysterious character format
            character_path = params.get('character_path', '/tmp/xlab_character.jpg')
            if not os.path.exists(character_path):
                character_path = '/tmp/xlab_avatar.jpg'
            final_path = build_conspiracy_short(job_id, item, work_dir, idx, grok_key,
                                               character_path if os.path.exists(character_path) else None)

            if not final_path:
                add_log(job_id, f'   ❌ Failed - skipping')
                continue

            # Save to library
            save_to_library(final_path, item['title'], niche, 'universal', job_id)

            # Upload to YouTube
            yt_id = ''
            if auto_upload and yt_token:
                add_log(job_id, f'   📤 Uploading to YouTube...')
                desc = item['hook'] + '\n\n' + ' '.join(item.get('hashtags', ['#Shorts']))
                yt_id = upload_to_youtube(final_path, item['title'], desc,
                                         item.get('hashtags', ['#Shorts']), yt_token)
                if yt_id:
                    add_log(job_id, f'   ✅ https://youtube.com/shorts/{yt_id}')

            # Post to X
            x_key = os.environ.get('X_API_KEY', '')
            x_secret = os.environ.get('X_API_SECRET', '')
            x_token = os.environ.get('X_ACCESS_TOKEN', '')
            x_token_secret = os.environ.get('X_ACCESS_TOKEN_SECRET', '')
            if all([x_key, x_secret, x_token, x_token_secret]):
                x_text = item['title'] + '\n\n' + ' '.join(item.get('hashtags', [])[:4])
                post_to_x(final_path, x_text, x_key, x_secret, x_token, x_token_secret)

            produced.append({'path': final_path, 'title': item['title'], 'yt_id': yt_id})
            add_log(job_id, f'   ✅ Done')

        if not produced:
            raise Exception('No videos produced')

        # ZIP all
        zip_name = f'{work_dir}/content_{job_id[:8]}.zip'
        with zipfile.ZipFile(zip_name, 'w') as zf:
            for v in produced:
                if os.path.exists(v['path']):
                    zf.write(v['path'], os.path.basename(v['path']))

        zip_size = os.path.getsize(zip_name) / (1024*1024)
        add_log(job_id, f'\n🎉 Done! {len(produced)} videos ready ({zip_size:.1f}MB)')
        update_job(job_id, {
            'status': 'done', 'progress': 100,
            'completed_at': datetime.now().isoformat(),
            'zip_path': zip_name, 'zip_size_mb': round(zip_size, 1),
            'total_clips': len(produced)
        })

    except Exception as e:
        add_log(job_id, f'❌ Fatal error: {e}')
        update_job(job_id, {'status': 'error', 'error': str(e)})


def research_with_claude_search(topic, api_key=None):
    """Use Claude API with web search tool - searches the real web.
    Uses the same Anthropic API key if available.
    Returns rich web research results.
    """
    import requests as req
    key = api_key or os.environ.get('ANTHROPIC_API_KEY', '') or os.environ.get('CLAUDE_API_KEY', '')
    # Try Gemini key format too - sometimes stored as CLAUDE_API_KEY
    if not key or key.startswith('AIza'):
        key = None
    
    if not key:
        return None
    
    try:
        r = req.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json'
            },
            json={
                'model': 'claude-sonnet-4-6',
                'max_tokens': 1024,
                'tools': [{'type': 'web_search_20250305', 'name': 'web_search'}],
                'messages': [{
                    'role': 'user',
                    'content': f'Search the web and find shocking, interesting, little-known facts about: {topic}. Focus on things most people dont know. Return the key facts you find.'
                }]
            },
            timeout=30
        )
        data = r.json()
        if 'content' in data:
            text = ' '.join([
                block.get('text', '') 
                for block in data['content'] 
                if block.get('type') == 'text'
            ])
            if text:
                logger.info(f'Claude web search: {len(text)} chars for "{topic}"')
                return text
        logger.error(f'Claude search error: {str(data)[:100]}')
        return None
    except Exception as e:
        logger.error(f'Claude search error: {e}')
        return None


def search_brave(query, api_key=None):
    """Search web using Brave Search API - 2000 free searches/month.
    Get free key at api.search.brave.com
    """
    import requests as req
    key = api_key or os.environ.get('BRAVE_API_KEY', '')
    if not key:
        return []
    try:
        r = req.get(
            'https://api.search.brave.com/res/v1/web/search',
            headers={
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip',
                'X-Subscription-Token': key
            },
            params={'q': query, 'count': 5, 'search_lang': 'en'},
            timeout=10
        )
        results = r.json().get('web', {}).get('results', [])
        snippets = []
        for r in results:
            title = r.get('title', '')
            desc = r.get('description', '')
            url = r.get('url', '')
            if desc:
                snippets.append(f"{title}: {desc} ({url})")
        logger.info(f'Brave search: {len(snippets)} results for "{query}"')
        return snippets
    except Exception as e:
        logger.error(f'Brave search error: {e}')
        return []


def search_gemini_grounded(query, api_key=None):
    """Use Gemini with Google Search grounding for real web results.
    Same free API key, returns current web information.
    """
    import requests as req
    key = api_key or os.environ.get('GEMINI_API_KEY', '') or os.environ.get('CLAUDE_API_KEY', '')
    if not key:
        return None
    try:
        r = req.post(
            f'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={key}',
            json={
                'contents': [{'parts': [{'text': f'Research this topic thoroughly using current web information: {query}. Provide key facts, recent developments, and interesting details.'}]}],
                'tools': [{'google_search_retrieval': {}}],
                'generationConfig': {'maxOutputTokens': 1500}
            },
            timeout=30
        )
        data = r.json()
        if 'candidates' in data:
            text = data['candidates'][0]['content']['parts'][0]['text']
            logger.info(f'Gemini grounded: {len(text)} chars for "{query}"')
            return text
        return None
    except Exception as e:
        logger.error(f'Gemini grounded error: {e}')
        return None


def research_topic_web(topic):
    """Research using real web search - multiple free sources."""
    import requests as req
    results = {'topic': topic, 'content': '', 'sources': [], 'images': []}

    # Source 1: Claude with web search tool (real web search)
    claude_result = research_with_claude_search(topic)
    if claude_result:
        results['content'] += claude_result + '\n\n'
        logger.info(f'Using Claude web search for "{topic}"')

    # Source 2: Gemini with Google Search grounding
    if not results['content']:
        grounded = search_gemini_grounded(topic)
        if grounded:
            results['content'] += grounded + '\n\n'
            logger.info(f'Using Gemini grounded search for "{topic}"')

    # Source 2: Brave Search (if API key available)
    brave_results = search_brave(topic)
    if brave_results:
        results['content'] += 'Web results:\n'
        for r in brave_results[:3]:
            results['content'] += f'- {r}\n'

    # Source 3: DuckDuckGo Instant Answer (no key needed)
    if not results['content']:
        try:
            r = req.get(
                'https://api.duckduckgo.com/',
                params={'q': topic, 'format': 'json', 'no_html': 1},
                timeout=10
            )
            data = r.json()
            abstract = data.get('AbstractText', '')
            if abstract:
                results['content'] += 'Overview: ' + abstract + '\n\n'
            for rt in data.get('RelatedTopics', [])[:3]:
                if isinstance(rt, dict) and rt.get('Text'):
                    results['content'] += '- ' + rt['Text'][:150] + '\n'
        except Exception as e:
            logger.error(f'DuckDuckGo error: {e}')

    # Source 4: Wikipedia always as backup
    if len(results['content']) < 200:
        try:
            r = req.get(
                'https://en.wikipedia.org/w/api.php',
                params={'action': 'query', 'list': 'search', 'srsearch': topic,
                        'format': 'json', 'srlimit': 1},
                timeout=10
            )
            wiki_results = r.json().get('query', {}).get('search', [])
            if wiki_results:
                page_title = wiki_results[0]['title']
                cr = req.get(
                    'https://en.wikipedia.org/w/api.php',
                    params={'action': 'query', 'titles': page_title,
                            'prop': 'extracts', 'exintro': True,
                            'explaintext': True, 'format': 'json'},
                    timeout=10
                )
                pages = cr.json().get('query', {}).get('pages', {})
                page = next(iter(pages.values()))
                extract = page.get('extract', '')[:1000]
                if extract:
                    results['content'] += f'Wikipedia - {page_title}:\n{extract}\n'
                    results['sources'].append(
                        f'https://en.wikipedia.org/wiki/{page_title.replace(" ", "_")}')
        except Exception as e:
            logger.error(f'Wikipedia error: {e}')

    logger.info(f'Research complete: {len(results["content"])} chars')
    return results if results['content'] else None


def research_topic_web_old(topic):
    """Research a topic using multiple free web sources:
    DuckDuckGo (no API key) + Wikipedia + Google snippets.
    Returns rich content for script writing.
    """
    import requests as req
    results = {'topic': topic, 'content': '', 'sources': [], 'images': []}

    # Source 1: DuckDuckGo Instant Answer API (completely free, no key)
    try:
        r = req.get(
            'https://api.duckduckgo.com/',
            params={'q': topic, 'format': 'json', 'no_html': 1, 'skip_disambig': 1},
            timeout=10
        )
        data = r.json()
        abstract = data.get('AbstractText', '')
        if abstract:
            results['content'] += 'Overview: ' + abstract + '\n\n'
        
        # Get related topics
        for rt in data.get('RelatedTopics', [])[:3]:
            if isinstance(rt, dict) and rt.get('Text'):
                results['content'] += '- ' + rt['Text'][:150] + '\n'
        
        # Get image if available
        image = data.get('Image', '')
        if image and image.startswith('http'):
            results['images'].append(image)
            
        logger.info(f'DuckDuckGo: {len(abstract)} chars for "{topic}"')
    except Exception as e:
        logger.error(f'DuckDuckGo error: {e}')

    # Source 2: Wikipedia (always reliable)
    try:
        search_r = req.get(
            'https://en.wikipedia.org/w/api.php',
            params={'action': 'query', 'list': 'search', 'srsearch': topic,
                    'format': 'json', 'srlimit': 2},
            timeout=10
        )
        wiki_results = search_r.json().get('query', {}).get('search', [])
        
        for wr in wiki_results[:2]:
            page_title = wr['title']
            content_r = req.get(
                'https://en.wikipedia.org/w/api.php',
                params={'action': 'query', 'titles': page_title,
                        'prop': 'extracts', 'exintro': True,
                        'explaintext': True, 'format': 'json'},
                timeout=10
            )
            pages = content_r.json().get('query', {}).get('pages', {})
            page = next(iter(pages.values()))
            extract = page.get('extract', '')[:1000]
            if extract:
                results['content'] += '\nWikipedia - ' + page_title + ':\n' + extract + '\n'
                results['sources'].append(
                    f'https://en.wikipedia.org/wiki/{page_title.replace(" ", "_")}')
    except Exception as e:
        logger.error(f'Wikipedia research error: {e}')

    # Source 3: News search via DuckDuckGo news
    try:
        r2 = req.get(
            'https://api.duckduckgo.com/',
            params={'q': topic + ' news facts', 'format': 'json', 
                    'no_html': 1, 't': 'XLAB'},
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=10
        )
        data2 = r2.json()
        for item in data2.get('Results', [])[:3]:
            if item.get('Text'):
                results['content'] += '\n' + item['Text'][:200] + '\n'
    except Exception as e:
        logger.error(f'News search error: {e}')

    logger.info(f'Web research: {len(results["content"])} chars, '
               f'{len(results["sources"])} sources')
    return results if results['content'] else None


def research_topic_wikipedia(topic):
    """Research a topic using Wikipedia API - completely free, always works.
    Kept for backward compatibility - now calls research_topic_web internally.
    """
    result = research_topic_web(topic)
    if result:
        return {
            'title': topic,
            'content': result['content'],
            'url': result['sources'][0] if result['sources'] else ''
        }
    import requests as req
    try:
        # Search Wikipedia
        search_r = req.get(
            'https://en.wikipedia.org/w/api.php',
            params={
                'action': 'query',
                'list': 'search',
                'srsearch': topic,
                'format': 'json',
                'srlimit': 3
            },
            timeout=10
        )
        results = search_r.json().get('query', {}).get('search', [])
        if not results:
            return None

        # Get content of top result
        page_title = results[0]['title']
        content_r = req.get(
            'https://en.wikipedia.org/w/api.php',
            params={
                'action': 'query',
                'titles': page_title,
                'prop': 'extracts',
                'exintro': True,
                'explaintext': True,
                'format': 'json'
            },
            timeout=10
        )
        pages = content_r.json().get('query', {}).get('pages', {})
        page = next(iter(pages.values()))
        extract = page.get('extract', '')[:2000]

        logger.info(f'Wikipedia found: {page_title} ({len(extract)} chars)')
        return {
            'title': page_title,
            'content': extract,
            'url': f'https://en.wikipedia.org/wiki/{page_title.replace(" ", "_")}'
        }
    except Exception as e:
        logger.error(f'Wikipedia error: {e}')
        return None


def generate_script_from_research(topic, research, api_key, style='conspiracy'):
    """Turn research into a viral script using AI."""
    wiki_content = research.get('content', '') if research else ''
    
    style_configs = {
        'conspiracy': {
            'hook_style': 'NOBODY WANTS TO TALK ABOUT THIS',
            'tone': 'outraged, shocking, educational',
            'cta': 'FOLLOW IF YOU WANT THE TRUTH'
        },
        'ai_news': {
            'hook_style': 'This just dropped and nobody knows about it yet',
            'tone': 'excited, informative, urgent',
            'cta': 'Follow for daily AI tools'
        },
        'history': {
            'hook_style': 'THEY NEVER TAUGHT YOU THIS IN SCHOOL',
            'tone': 'dramatic, revelatory, factual',
            'cta': 'Follow for hidden history'
        }
    }
    
    cfg = style_configs.get(style, style_configs['conspiracy'])
    
    prompt = f"""Create a viral 30-second Short script about: "{topic}"

Research context:
{wiki_content[:800]}

Style: {cfg['tone']}
Hook style: "{cfg['hook_style']}"
CTA: "{cfg['cta']}"

Reply ONLY with valid JSON:
{{"title": "HOOK IN CAPS UNDER 60 CHARS",
  "hook": "Opening line that shocks in 2 seconds",
  "script": "Punchy 30 second script with shocking facts",
  "key_facts": ["Fact 1", "Fact 2", "Fact 3"],
  "search_query": "YouTube search for documentary footage",
  "text_overlays": ["LINE 1 IN CAPS", "LINE 2", "LINE 3"],
  "hashtags": ["#shorts", "#viral", "#didyouknow"]
}}"""

    text = call_grok(prompt, api_key)
    if not text:
        text = call_gemini_free(prompt)
    if not text:
        return None
    
    try:
        text = text.replace('```json','').replace('```','').strip()
        start = text.find('{')
        end = text.rfind('}') + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
    except Exception as e:
        logger.error(f'Script parse error: {e}')
    return None


def analyze_viral_blueprint(channel_handle, api_key):
    """Generate viral conspiracy content based on proven topic categories."""
    import random
    config = NICHE_CONFIGS.get('conspiracy', {})
    categories = config.get('topic_categories', ['hidden history facts'])
    # Pick 3 random topic categories to keep content fresh
    selected = random.sample(categories, min(3, len(categories)))
    
    hooks_str = '\n'.join(['- ' + h for h in config.get('hooks', [])])
    
    prompt = f"""You are a viral conspiracy content creator. Research and generate shocking but FACTUAL content.

Use these proven viral hook styles:
{hooks_str}

Research these specific topic areas and find the most shocking TRUE facts:
- {selected[0]}
- {selected[1]}  
- {selected[2]}

Rules:
- ONLY real verified historical facts - no fake news or misinformation
- Each fact must be genuinely surprising and provable
- Write in the style: short punchy sentences, outraged tone, "they don't want you to know"
- Perfect for 30 second Shorts

Reply ONLY with valid JSON:
{{"items": [
  {{
    "title": "HOOK IN CAPS UNDER 60 CHARS",
    "hook": "Opening line - must shock in 2 seconds",
    "script": "30 second script - punchy facts, dramatic, educational",
    "key_facts": ["Shocking fact 1", "Shocking fact 2", "Shocking fact 3"],
    "search_query": "YouTube documentary search for this topic",
    "text_overlays": ["LINE 1 IN CAPS", "KEY FACT LINE 2", "SHOCKING LINE 3"],
    "hashtags": ["#conspiracy", "#history", "#didyouknow", "#facts", "#truth"]
  }}
]}}"""

    text = call_grok(prompt, api_key)
    if not text:
        text = call_gemini_free(prompt)
    if not text:
        return None
    try:
        text = text.replace('```json','').replace('```','').strip()
        start = text.find('{')
        end = text.rfind('}') + 1
        if start >= 0 and end > start:
            text = text[start:end]
        return json.loads(text)
    except Exception as e:
        logger.error(f'Blueprint parse error: {e}')
        return None


# ============================================================
# AFFILIATE DATABASE
# ============================================================
AFFILIATE_DB = {
    'midjourney': {'url': 'https://midjourney.com', 'commission': '20%', 'program': 'midjourney.com/account'},
    'elevenlabs': {'url': 'https://elevenlabs.io', 'commission': '22%', 'program': 'elevenlabs.io/affiliate'},
    'jasper': {'url': 'https://jasper.ai', 'commission': '30%', 'program': 'jasper.ai/affiliates'},
    'copy.ai': {'url': 'https://copy.ai', 'commission': '45%', 'program': 'copy.ai/affiliates'},
    'synthesia': {'url': 'https://synthesia.io', 'commission': '20%', 'program': 'synthesia.io/affiliates'},
    'heygen': {'url': 'https://heygen.com', 'commission': '25%', 'program': 'heygen.com/affiliates'},
    'runway': {'url': 'https://runwayml.com', 'commission': '20%', 'program': 'runwayml.com/affiliates'},
    'notion': {'url': 'https://notion.so', 'commission': '$10/ref', 'program': 'notion.so/affiliates'},
    'claude': {'url': 'https://anthropic.com', 'commission': 'N/A', 'program': ''},
    'chatgpt': {'url': 'https://openai.com', 'commission': 'N/A', 'program': ''},
    'perplexity': {'url': 'https://perplexity.ai', 'commission': '20%', 'program': 'perplexity.ai/pro'},
    'leonardo': {'url': 'https://leonardo.ai', 'commission': '20%', 'program': 'leonardo.ai/affiliates'},
}

def get_affiliate_info(tool_name):
    """Find affiliate info for a tool by name matching."""
    tool_lower = tool_name.lower()
    for key, info in AFFILIATE_DB.items():
        if key in tool_lower or tool_lower in key:
            return info
    return None


def add_crossfade_transition(clip1, clip2, output, duration=0.5):
    """Add smooth crossfade between two clips."""
    try:
        result = subprocess.run([
            'ffmpeg',
            '-i', clip1, '-i', clip2,
            '-filter_complex',
            f'[0:v][1:v]xfade=transition=fade:duration={duration}:offset=0[v];'
            f'[0:a][1:a]acrossfade=d={duration}[a]',
            '-map', '[v]', '-map', '[a]',
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            '-y', output, '-loglevel', 'quiet'
        ], capture_output=True, timeout=60)
        return os.path.exists(output) and os.path.getsize(output) > 50000
    except Exception as e:
        logger.error(f'Crossfade error: {e}')
        return False


def fetch_tool_screenshots(tool_name, work_dir, idx):
    """Fetch tool screenshots from Google Images as last resort."""
    import requests as req
    try:
        # Use DuckDuckGo image search (no API key needed)
        search = f'{tool_name} AI tool interface screenshot'
        r = req.get(
            'https://duckduckgo.com/',
            params={'q': search, 'iax': 'images', 'ia': 'images'},
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=10
        )
        import re
        # Extract image URLs from response
        urls = re.findall(r'"thumbnail":"(https?://[^"]+)"', r.text)
        valid_urls = [u for u in urls if any(ext in u.lower() 
                     for ext in ['.jpg', '.jpeg', '.png', '.webp'])][:4]
        
        if valid_urls:
            vid_path = f'{work_dir}/google_imgs_{idx}.mp4'
            if create_video_from_images(valid_urls, vid_path, duration_each=6):
                return vid_path, 'google_images'
    except Exception as e:
        logger.error(f'Google images error: {e}')
    return None, None


def fetch_website_screenshot(url, work_dir, idx):
    """Take screenshot of tool website using a screenshot API."""
    import requests as req
    try:
        # Use free screenshot API
        shot_url = f'https://image.thum.io/get/width/1080/crop/1920/{url}'
        r = req.get(shot_url, timeout=15)
        if r.status_code == 200 and len(r.content) > 10000:
            img_path = f'{work_dir}/website_{idx}.jpg'
            with open(img_path, 'wb') as f:
                f.write(r.content)
            # Animate it
            vid_path = f'{work_dir}/website_vid_{idx}.mp4'
            if create_video_from_images([img_path], vid_path, duration_each=10):
                return vid_path, 'website'
    except Exception as e:
        logger.error(f'Website screenshot error: {e}')
    return None, None


def fetch_wikipedia_images(topic, work_dir, idx):
    """Fetch real historical images from Wikipedia - free, factual, credible."""
    import requests as req
    try:
        # Search Wikipedia for images related to topic
        r = req.get(
            'https://en.wikipedia.org/w/api.php',
            params={
                'action': 'query',
                'generator': 'search',
                'gsrsearch': topic,
                'gsrlimit': 3,
                'prop': 'images',
                'imlimit': 5,
                'format': 'json'
            },
            timeout=10
        )
        pages = r.json().get('query', {}).get('pages', {})
        image_names = []
        for page in pages.values():
            for img in page.get('images', []):
                name = img.get('title', '')
                if any(ext in name.lower() for ext in ['.jpg', '.jpeg', '.png']):
                    if not any(x in name.lower() for x in ['icon', 'logo', 'flag', 'map']):
                        image_names.append(name)

        # Get actual image URLs
        image_urls = []
        for name in image_names[:4]:
            try:
                img_r = req.get(
                    'https://en.wikipedia.org/w/api.php',
                    params={
                        'action': 'query',
                        'titles': name,
                        'prop': 'imageinfo',
                        'iiprop': 'url',
                        'format': 'json'
                    },
                    timeout=10
                )
                img_pages = img_r.json().get('query', {}).get('pages', {})
                for p in img_pages.values():
                    url = p.get('imageinfo', [{}])[0].get('url', '')
                    if url:
                        image_urls.append(url)
            except: continue

        logger.info(f'Wikipedia images: {len(image_urls)} for "{topic}"')
        return image_urls
    except Exception as e:
        logger.error(f'Wikipedia images error: {e}')
        return []


def smart_fetch_visuals(item, work_dir, idx):
    """Smart visual fetching - real content only, completely free."""
    import re
    title = item.get('title', '')
    search_q = item.get('search_query', title)
    text = item.get('script', '') + item.get('x_source', '')

    # 1. Internet Archive - real historical footage (free, public domain)
    add_log_safe('   📚 Searching Internet Archive...')
    result, source = fetch_internet_archive_footage(search_q, work_dir, idx)
    if result:
        try:
            dur = get_duration(result)
            start = max(0, dur * 0.1)
            cut_path = f'{work_dir}/cut_archive_{idx}.mp4'
            cut_vertical(result, start, 30, cut_path, is_vertical(result))
            if os.path.exists(cut_path) and os.path.getsize(cut_path) > 100000:
                return cut_path, 'archive'
        except:
            return result, 'archive'

    # 2. Wikipedia images → Ken Burns slideshow (real historical photos)
    add_log_safe('   🖼️ Searching Wikipedia images...')
    wiki_imgs = fetch_wikipedia_images(search_q, work_dir, idx)
    if wiki_imgs:
        img_vid = f'{work_dir}/wiki_imgs_{idx}.mp4'
        if create_video_from_images(wiki_imgs, img_vid, duration_each=7):
            return img_vid, 'wikipedia'

    # 3. X post images (real screenshots from original posts)
    x_images = item.get('images', [])
    if x_images:
        img_vid = f'{work_dir}/ximg_{idx}.mp4'
        if create_video_from_images(x_images, img_vid):
            return img_vid, 'x_images'

    # 4. Website screenshot (official source)
    url_match = re.search(r'https?://[^\s\)"]+\.[a-z]{2,}', text)
    if url_match:
        result, source = fetch_website_screenshot(url_match.group(0), work_dir, idx)
        if result:
            return result, source

    # 5. GitHub screenshots (for tech content)
    gh_match = re.search(r'github\.com/[^\s\)"]+', text)
    if gh_match:
        gh_images = fetch_github_images('https://' + gh_match.group(0))
        if gh_images:
            gh_vid = f'{work_dir}/ghimg_{idx}.mp4'
            if create_video_from_images(gh_images, gh_vid):
                return gh_vid, 'github'

    # 6. YouTube as absolute last resort
    add_log_safe('   📹 Trying YouTube...')
    result, source = fetch_best_visuals(item, work_dir, idx)
    if result:
        return result, source

    return None, 'avatar_only'

def add_log_safe(msg):
    """Log without job_id - used in utility functions."""
    logger.info(msg)


def build_ai_news_short(job_id, item, work_dir, idx, grok_key, avatar_path=None):
    """Build a complete AI news Short with avatar intro + demo + avatar outro.
    Format: avatar hook → demo footage → avatar CTA
    Duration: ~30 seconds total
    """
    clips = []
    
    title = item.get('title', '')
    hook = item.get('hook', f'This AI just dropped and nobody is talking about it')
    script = item.get('script', '')
    
    # ── Part 1: Free avatar intro (MuseTalk lip sync) ─────────
    add_log(job_id, f'   🎭 Generating free avatar intro...')
    avatar_path = '/tmp/xlab_avatar.jpg'
    intro_done = False
    
    if os.path.exists(avatar_path):
        try:
            intro_text = f"{hook}. {script[:120]}"
            avatar_clip = f'{work_dir}/avatar_intro_{idx}.mp4'
            if generate_free_avatar_clip(intro_text, avatar_path, avatar_clip, work_dir):
                norm_path = f'{work_dir}/norm_intro_{idx}.mp4'
                if normalize_clip(avatar_clip, norm_path):
                    clips.append(norm_path)
                    add_log(job_id, f'   ✅ Avatar intro ready (lip synced)')
                    intro_done = True
        except Exception as e:
            add_log(job_id, f'   ⚠️ Avatar error: {str(e)[:40]}')
    
    if not intro_done:
        # Fallback: title card
        try:
            hook_card = f'{work_dir}/hook_{idx}.mp4'
            safe_hook = hook[:55].replace("'","").replace('"','').replace(':','')
            subprocess.run([
                'ffmpeg', '-f', 'lavfi',
                '-i', 'color=c=black:size=1080x1920:duration=4:rate=30',
                '-vf', (f"drawtext=text='{safe_hook}':fontsize=52:fontcolor=white:"
                       f"x=(w-text_w)/2:y=(h-text_h)/2:fontweight=bold:"
                       f"box=1:boxcolor=black@0.5:boxborderw=20"),
                '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
                '-pix_fmt', 'yuv420p', '-y', hook_card, '-loglevel', 'quiet'
            ], capture_output=True, timeout=30)
            if os.path.exists(hook_card) and os.path.getsize(hook_card) > 10000:
                clips.append(hook_card)
                add_log(job_id, f'   ✅ Hook card ready')
        except Exception as e:
            add_log(job_id, f'   ⚠️ Hook card error: {str(e)[:40]}')
    
    # ── Part 2: Demo footage middle (8-22s) ───────────────────
    add_log(job_id, f'   🎬 Finding best demo footage...')
    demo_path, source = smart_fetch_visuals(item, work_dir, idx)
    
    if demo_path:
        try:
            # Add Grok narration over demo
            narr_path = f'{work_dir}/narr_{idx}.mp3'
            demo_narrated = f'{work_dir}/demo_narrated_{idx}.mp4'
            if text_to_speech(script, narr_path):
                if overlay_narration(demo_path, narr_path, demo_narrated):
                    if os.path.exists(demo_narrated):
                        demo_path = demo_narrated

            # Add text overlays
            overlay_out = f'{work_dir}/demo_overlay_{idx}.mp4'
            key_points = item.get('key_facts', [
                (item.get('hook') or '')[:45],
                f'Tool: {title[:35]}',
                'Free to try'
            ])[:3]
            if add_text_overlays(demo_path, overlay_out, title, hook, key_points, ''):
                if os.path.exists(overlay_out):
                    demo_path = overlay_out

            clips.append(demo_path)
            add_log(job_id, f'   ✅ Demo ready - {source}')
        except Exception as e:
            add_log(job_id, f'   ⚠️ Demo processing error: {str(e)[:50]}')
            clips.append(demo_path)  # use raw clip even if processing fails
    
    # ── Part 3: Avatar CTA outro (22-30s) ─────────────────────
    # ── Part 3: Free avatar CTA ───────────────────────────────
    add_log(job_id, f'   🎭 Generating CTA...')
    cta_done = False
    affiliate = get_affiliate_info(title)
    cta_text = 'Follow for daily AI tools and hacks. New video every day.'
    if affiliate and affiliate.get('commission') not in ('N/A', None):
        cta_text = f"Follow for daily AI tools. Check the link in bio for {affiliate['commission']} commission deals."

    if os.path.exists(avatar_path):
        try:
            cta_clip = f'{work_dir}/avatar_cta_{idx}.mp4'
            if generate_free_avatar_clip(cta_text, avatar_path, cta_clip, work_dir):
                norm_cta = f'{work_dir}/norm_cta_{idx}.mp4'
                if normalize_clip(cta_clip, norm_cta):
                    clips.append(norm_cta)
                    add_log(job_id, f'   ✅ Avatar CTA ready')
                    cta_done = True
        except Exception as e:
            add_log(job_id, f'   ⚠️ CTA error: {str(e)[:40]}')
    
    if not cta_done:
        try:
            cta_card = f'{work_dir}/cta_{idx}.mp4'
            subprocess.run([
                'ffmpeg', '-f', 'lavfi',
                '-i', 'color=c=black:size=1080x1920:duration=3:rate=30',
                '-vf', ("drawtext=text='FOLLOW FOR DAILY AI TOOLS':fontsize=44:fontcolor=white:"
                       "x=(w-text_w)/2:y=h*0.45:fontweight=bold,"
                       "drawtext=text='New video every day':fontsize=30:fontcolor=#ff6b5b:"
                       "x=(w-text_w)/2:y=h*0.56"),
                '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
                '-pix_fmt', 'yuv420p', '-y', cta_card, '-loglevel', 'quiet'
            ], capture_output=True, timeout=30)
            if os.path.exists(cta_card) and os.path.getsize(cta_card) > 10000:
                clips.append(cta_card)
                add_log(job_id, f'   ✅ CTA card ready')
        except Exception as e:
            add_log(job_id, f'   ⚠️ CTA card error: {str(e)[:40]}')
    
    # If avatar_only mode - generate longer avatar clip with full narration
    if source == 'avatar_only' and avatar_path and grok_key:
        add_log(job_id, f'   🎭 No demo found - avatar talks full video...')
        full_avatar = f'{work_dir}/full_avatar_{idx}.mp4'
        if aurora_image_to_video(
            avatar_path, None, full_avatar, grok_key,
            prompt=f'Person talking enthusiastically about AI tools: {script[:100]}'
        ):
            norm_full = f'{work_dir}/norm_full_{idx}.mp4'
            if normalize_clip(full_avatar, norm_full):
                clips.append(norm_full)

    if not clips:
        return None
    
    # ── Assemble with crossfades ──────────────────────────────
    add_log(job_id, f'   🎬 Assembling {len(clips)} parts with transitions...')
    
    if len(clips) == 1:
        final_path = f'{work_dir}/final_{idx}.mp4'
        import shutil
        shutil.copy2(clips[0], final_path)
    else:
        # Add crossfades between each clip
        current = clips[0]
        for ci, next_clip in enumerate(clips[1:]):
            faded = f'{work_dir}/faded_{idx}_{ci}.mp4'
            if add_crossfade_transition(current, next_clip, faded, duration=0.4):
                current = faded
            else:
                # Fallback to hard cut
                merged = f'{work_dir}/merged_{idx}_{ci}.mp4'
                concatenate_clips([current, next_clip], merged)
                current = merged
        final_path = current

    if os.path.exists(final_path):
        # Add background music
        music_out = f'{work_dir}/music_{idx}.mp4'
        if add_trending_music(final_path, music_out, 'dramatic', 0.12):
            final_path = music_out
        
        size = os.path.getsize(final_path) / (1024*1024)
        add_log(job_id, f'   ✅ Complete Short ready ({size:.1f}MB)')
        return final_path
    
    return None


def build_conspiracy_short(job_id, item, work_dir, idx, grok_key, character_path=None):
    """Build viral conspiracy Short with mysterious character format:
    - Character appears (no face shown = mystery)
    - Bold text reveals shocking facts
    - Grok/gTTS narrates over footage
    - Character reappears for CTA
    """
    clips = []
    title = item.get('title', '')
    hook = item.get('hook', '')
    script = item.get('script', '')
    overlays = item.get('text_overlays', [])
    key_facts = item.get('key_facts', [])

    # ── Part 1: Character intro (3s) ──────────────────────────
    add_log(job_id, f'   🎭 Creating character intro...')
    try:
        char_intro = f'{work_dir}/char_intro_{idx}.mp4'
        if character_path and os.path.exists(character_path):
            # Animate character with zoom in effect
            subprocess.run([
                'ffmpeg', '-loop', '1', '-i', character_path,
                '-vf', ('scale=1080:1920:force_original_aspect_ratio=increase,'
                       'crop=1080:1920,'
                       'zoompan=z=\'min(zoom+0.002,1.2)\''
                       ':x=\'iw/2-(iw/zoom/2)\''
                       ':y=\'ih/2-(ih/zoom/2)\''
                       ':d=90:s=1080x1920:fps=30'),
                '-pix_fmt', 'yuv420p', '-y', char_intro, '-loglevel', 'quiet'
            ], capture_output=True, timeout=30)
        else:
            # Black intro card
            subprocess.run([
                'ffmpeg', '-f', 'lavfi',
                '-i', 'color=c=black:size=1080x1920:duration=3:rate=30',
                '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
                '-pix_fmt', 'yuv420p', '-y', char_intro, '-loglevel', 'quiet'
            ], capture_output=True, timeout=20)

        if os.path.exists(char_intro):
            clips.append(char_intro)
            add_log(job_id, f'   ✅ Character intro ready')
    except Exception as e:
        add_log(job_id, f'   ⚠️ Intro error: {str(e)[:40]}')

    # ── Part 2: Hook text reveal (3s) ─────────────────────────
    try:
        hook_card = f'{work_dir}/hook_card_{idx}.mp4'
        safe_hook = title[:55].replace("'","").replace('"','').replace(':','')
        subprocess.run([
            'ffmpeg', '-f', 'lavfi',
            '-i', 'color=c=black:size=1080x1920:duration=3:rate=30',
            '-vf', (f"drawtext=text='{safe_hook}':fontsize=52:fontcolor=white:"
                   f"x=(w-text_w)/2:y=(h-text_h)/2:fontweight=bold:"
                   f"box=1:boxcolor=black@0.3:boxborderw=15"),
            '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
            '-pix_fmt', 'yuv420p', '-y', hook_card, '-loglevel', 'quiet'
        ], capture_output=True, timeout=20)
        if os.path.exists(hook_card):
            clips.append(hook_card)
    except Exception as e:
        add_log(job_id, f'   ⚠️ Hook card error: {str(e)[:40]}')

    # ── Part 3: Documentary footage with narration (20s) ──────
    add_log(job_id, f'   🎬 Finding documentary footage...')
    demo_path, source = smart_fetch_visuals(item, work_dir, idx)

    if demo_path:
        try:
            # Add narration
            narr_path = f'{work_dir}/narr_{idx}.mp3'
            narrated = f'{work_dir}/narrated_{idx}.mp4'
            if text_to_speech(script, narr_path):
                if overlay_narration(demo_path, narr_path, narrated):
                    demo_path = narrated

            # Add shocking text overlays
            overlay_out = f'{work_dir}/overlay_{idx}.mp4'
            facts_to_show = (overlays or key_facts)[:3]
            if add_text_overlays(demo_path, overlay_out, title, hook, facts_to_show, ''):
                demo_path = overlay_out

            clips.append(demo_path)
            add_log(job_id, f'   ✅ Footage ready - {source}')
        except Exception as e:
            clips.append(demo_path)
            add_log(job_id, f'   ⚠️ Footage processing: {str(e)[:40]}')

    # ── Part 4: Character CTA (3s) ────────────────────────────
    try:
        cta_card = f'{work_dir}/cta_{idx}.mp4'
        if character_path and os.path.exists(character_path):
            subprocess.run([
                'ffmpeg', '-loop', '1', '-i', character_path,
                '-vf', (f'scale=1080:1920:force_original_aspect_ratio=increase,'
                       f'crop=1080:1920,'
                       f"drawtext=text='FOLLOW IF YOU WANT THE TRUTH':fontsize=40:"
                       f"fontcolor=white:x=(w-text_w)/2:y=h*0.85:fontweight=bold:"
                       f"box=1:boxcolor=black@0.6:boxborderw=12"),
                '-t', '3', '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
                '-pix_fmt', 'yuv420p', '-y', cta_card, '-loglevel', 'quiet'
            ], capture_output=True, timeout=30)
        else:
            subprocess.run([
                'ffmpeg', '-f', 'lavfi',
                '-i', 'color=c=black:size=1080x1920:duration=3:rate=30',
                '-vf', ("drawtext=text='FOLLOW IF YOU WANT THE TRUTH':fontsize=44:"
                       "fontcolor=white:x=(w-text_w)/2:y=(h-text_h)/2:fontweight=bold"),
                '-c:v', 'libx264', '-preset', 'fast', '-crf', '22',
                '-pix_fmt', 'yuv420p', '-y', cta_card, '-loglevel', 'quiet'
            ], capture_output=True, timeout=20)

        if os.path.exists(cta_card):
            clips.append(cta_card)
            add_log(job_id, f'   ✅ CTA ready')
    except Exception as e:
        add_log(job_id, f'   ⚠️ CTA error: {str(e)[:40]}')

    if not clips:
        return None

    # Assemble with crossfades
    add_log(job_id, f'   🎬 Assembling {len(clips)} parts...')
    if len(clips) == 1:
        final_path = f'{work_dir}/final_{idx}.mp4'
        import shutil
        shutil.copy2(clips[0], final_path)
    else:
        current = clips[0]
        for ci, nxt in enumerate(clips[1:]):
            faded = f'{work_dir}/faded_{idx}_{ci}.mp4'
            if not add_crossfade_transition(current, nxt, faded, 0.3):
                merged = f'{work_dir}/merged_{idx}_{ci}.mp4'
                concatenate_clips([current, nxt], merged)
                current = merged
            else:
                current = faded
        final_path = current

    if os.path.exists(final_path):
        # Add dramatic music
        music_out = f'{work_dir}/music_{idx}.mp4'
        if add_trending_music(final_path, music_out, 'dramatic', 0.15):
            final_path = music_out
        size = os.path.getsize(final_path) / (1024*1024)
        add_log(job_id, f'   ✅ Conspiracy Short ready ({size:.1f}MB)')
        return final_path

    return None


def process_conspiracy_studio(job_id, params):
    """Generate viral conspiracy/mystery Shorts using the proven blueprint."""
    work_dir = f'/tmp/conspiracy_{job_id}'
    os.makedirs(work_dir, exist_ok=True)

    try:
        update_job(job_id, {'status': 'processing', 'started_at': datetime.now().isoformat()})
        grok_key = os.environ.get('GROK_API_KEY', '').strip() or params.get('grok_key', '')
        max_videos = int(params.get('max_videos', 3))
        auto_upload = params.get('auto_upload') == 'Yes'
        yt_token = params.get('yt_access_token', '')

        if not grok_key:
            raise Exception('Grok API key required')

        custom_topic = params.get('custom_topic', '').strip()
        add_log(job_id, '🔍 Researching content...')
        update_job(job_id, {'progress': 5})

        if custom_topic:
            add_log(job_id, f'   📚 Researching: {custom_topic[:50]}')
            
            # Research with web
            research = research_topic_web(custom_topic)
            research_content = research.get('content', '') if research else ''
            
            item = None
            
            # Try Claude first
            anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '').strip()
            if anthropic_key:
                item = generate_script_with_claude(custom_topic, research_content, 'conspiracy')
            
            # Try Gemini
            if not item:
                gemini_key = (os.environ.get('GEMINI_API_KEY','') or os.environ.get('CLAUDE_API_KEY','')).strip()
                if gemini_key and gemini_key.startswith('AIza'):
                    item = generate_script_from_research(custom_topic, research, gemini_key, 'conspiracy')
            
            # Always generate something — even without AI
            if not item:
                add_log(job_id, f'   ⚠️ No AI key — using built-in template')
                item = {
                    'title': f'NOBODY TALKS ABOUT {custom_topic.upper()[:40]}',
                    'hook': f'What they never told you about {custom_topic}',
                    'script': f'Here is the shocking truth about {custom_topic} that mainstream media refuses to cover. The facts will blow your mind.',
                    'key_facts': [f'The truth about {custom_topic}', 'This was hidden from you', 'Share before its deleted'],
                    'search_query': custom_topic + ' documentary history',
                    'text_overlays': [custom_topic.upper()[:40], 'THE HIDDEN TRUTH', 'NOBODY TALKS ABOUT THIS'],
                    'hashtags': ['#conspiracy', '#truth', '#didyouknow', '#shorts', '#viral']
                }
            
            data = {'items': [item] * min(max_videos, 3)}
            add_log(job_id, f'   ✅ Script ready: {item.get("title","")[:50]}')
        else:
            # Auto-find viral topics
            data = analyze_viral_blueprint('@conspiracy_peterx', grok_key)
            if not data or not data.get('items'):
                raise Exception('Could not generate topics')

        items = data['items'][:max_videos]
        add_log(job_id, f'✅ Generated {len(items)} topics')
        for item in items:
            add_log(job_id, f'   • {item["title"][:50]}')

        produced = []

        for idx, item in enumerate(items):
            add_log(job_id, f'\n📱 Story {idx+1}: {item["title"][:50]}')
            update_job(job_id, {'progress': 10 + int((idx/len(items))*75)})

            # Find B-roll footage
            add_log(job_id, f'   🎬 Finding footage...')
            clip_path, source = fetch_best_visuals(item, work_dir, idx)

            if not clip_path:
                add_log(job_id, f'   ⚠️ No footage - skipping')
                continue

            # Add Grok narration
            add_log(job_id, f'   🎙️ Adding narration...')
            tts_path = f'{work_dir}/narr_{idx}.mp3'
            if text_to_speech(item.get('script', item['hook']), tts_path):
                narr_out = f'{work_dir}/narrated_{idx}.mp4'
                if overlay_narration(clip_path, tts_path, narr_out):
                    clip_path = narr_out

            # Add viral text overlays
            add_log(job_id, f'   📝 Adding bold text overlays...')
            overlay_out = f'{work_dir}/overlay_{idx}.mp4'
            key_facts = item.get('key_facts', [])[:3]
            if add_text_overlays(clip_path, overlay_out,
                                item['title'], item['hook'],
                                key_facts, 'FOLLOW FOR MORE 👀'):
                clip_path = overlay_out

            # Save final
            final_path = f'{work_dir}/conspiracy_{idx}_{job_id[:6]}.mp4'
            import shutil as _sh
            _sh.copy2(clip_path, final_path)
            save_to_library(final_path, item['title'], 'conspiracy', 'conspiracy', job_id)

            # Upload
            yt_id = ''
            if auto_upload and yt_token:
                add_log(job_id, f'   📤 Uploading...')
                desc = item['hook'] + '\n\n' + ' '.join(item.get('hashtags', ['#Shorts']))
                yt_id = upload_to_youtube(final_path, item['title'], desc,
                                         item.get('hashtags', ['#Shorts']), yt_token)
                if yt_id:
                    add_log(job_id, f'   ✅ https://youtube.com/shorts/{yt_id}')

            # Post to X
            x_key = os.environ.get('X_API_KEY', '')
            x_secret = os.environ.get('X_API_SECRET', '')
            x_token = os.environ.get('X_ACCESS_TOKEN', '')
            x_token_secret = os.environ.get('X_ACCESS_TOKEN_SECRET', '')
            if x_key and x_secret and x_token and x_token_secret:
                x_text = item['title'] + '\n\n' + ' '.join(item.get('hashtags', [])[:4])
                post_to_x(final_path, x_text, x_key, x_secret, x_token, x_token_secret)

            produced.append({'path': final_path, 'title': item['title'], 'yt_id': yt_id})
            add_log(job_id, f'   ✅ Done')

        if not produced:
            raise Exception('No videos produced')

        zip_name = f'{work_dir}/conspiracy_{job_id[:8]}.zip'
        with zipfile.ZipFile(zip_name, 'w') as zf:
            for v in produced:
                if os.path.exists(v['path']):
                    zf.write(v['path'], os.path.basename(v['path']))

        zip_size = os.path.getsize(zip_name) / (1024*1024)
        add_log(job_id, f'\n🎉 Done! {len(produced)} videos ready')
        update_job(job_id, {
            'status': 'done', 'progress': 100,
            'completed_at': datetime.now().isoformat(),
            'zip_path': zip_name, 'zip_size_mb': round(zip_size, 1),
            'total_clips': len(produced)
        })

    except Exception as e:
        add_log(job_id, f'❌ Fatal error: {e}')
        update_job(job_id, {'status': 'error', 'error': str(e)})


def grok_find_ai_news(api_key, categories=None):
    """Use Grok to find latest AI news, hacks, tools from X that aren't viral yet."""
    if not categories:
        categories = ['ai_tools', 'github_hacks', 'hustles', 'productivity']

    # First get real X posts as context
    x_posts = search_x_for_ai_news(categories)
    x_context = ""
    if x_posts:
        x_context = "\n\nReal trending X posts from last 48hrs:\n"
        for p in x_posts[:6]:
            img_note = f" [has {len(p.get('images',[]))} images]" if p.get('images') else ""
            x_context += f"- [{p['category']}] {p['text'][:150]} (likes:{p['likes']}){img_note}\n"
        logger.info(f'Using {len(x_posts)} real X posts as context')
    else:
        x_context = "\n\n(No X posts available - use your knowledge of recent AI news)"

    prompt = """Based on these real X posts and your knowledge of recent AI news, create 3 YouTube Short scripts.
Each should be about something NOT yet viral on YouTube/TikTok.
""" + x_context + """

Reply ONLY with this exact JSON, no other text:
{"items": [{"category": "ai_tool", "title": "Short catchy title under 60 chars", "hook": "Attention grabbing first line", "script": "Punchy 30 second narration script", "search_query": "Specific YouTube search to find demo of this tool", "images": [], "hashtags": ["#AI", "#Tech", "#AITools", "#Shorts"], "why_viral": "Why this will get views"}]}"""

    text = call_grok(prompt, api_key)
    logger.info(f'Grok AI news response: {text[:200] if text else "None"}')
    # Store x_posts for image lookup
    _x_posts_cache = x_posts
    
    if not text:
        return None
    
    # Try multiple JSON extraction methods
    try:
        # Clean response
        text = text.strip()
        # Remove markdown code blocks
        text = text.replace('```json', '').replace('```', '').strip()
        # Find JSON object
        start = text.find('{')
        end = text.rfind('}') + 1
        if start >= 0 and end > start:
            text = text[start:end]
        result = json.loads(text)
        if result.get('items'):
            # Enrich items with X post images
            for item in result['items']:
                if not item.get('images') and x_posts:
                    # Find matching X post images by category
                    for xp in x_posts:
                        if xp.get('category') == item.get('category') and xp.get('images'):
                            item['images'] = xp['images']
                            break
            return result
        logger.error(f'No items in Grok response: {result}')
        return None
    except Exception as e:
        logger.error(f'AI news parse error: {e} - raw: {text[:200]}')
        # Try to manually build a basic item from the response
        return {
            "items": [{
                "category": "ai_tool",
                "title": "Latest AI Tool You Need to Know",
                "hook": "This just dropped and nobody is talking about it yet",
                "script": text[:300] if text else "Check out this amazing new AI tool that just launched",
                "video_prompt": "Futuristic AI interface visualization, dark theme, glowing elements, cinematic 9:16",
                "hashtags": ["#AI", "#Tech", "#AITools", "#Shorts"],
                "why_viral": "New and trending"
            }]
        }


def process_ai_news_studio(job_id, params):
    """Auto-find AI news from X and create + post Shorts automatically."""
    work_dir = f'/tmp/ainews_{job_id}'
    os.makedirs(work_dir, exist_ok=True)

    try:
        update_job(job_id, {'status': 'processing', 'started_at': datetime.now().isoformat()})

        grok_key = (os.environ.get('GROK_API_KEY', '') or params.get('grok_key', '')).strip()
        max_videos = int(params.get('max_videos', 3))
        use_aurora = params.get('use_aurora') == 'Yes'
        auto_upload = params.get('auto_upload') == 'Yes'
        yt_token = params.get('yt_access_token', '')
        music_enabled = params.get('music_enabled', 'Yes') == 'Yes'
        categories = params.get('categories', [])

        logger.info(f'Grok key check: env={bool(os.environ.get("GROK_API_KEY"))}, param={bool(params.get("grok_key"))}')
        if not grok_key:
            raise Exception('Grok API key required - add GROK_API_KEY to Railway variables')

        add_log(job_id, '🔍 Scanning X for latest AI news, hacks and hustles...')
        update_job(job_id, {'progress': 5})

        news_data = grok_find_ai_news(grok_key, categories)
        if not news_data or not news_data.get('items'):
            add_log(job_id, f'   Grok response was empty or invalid')
            raise Exception('No AI news found - Grok returned no items. Check Railway logs for details.')

        items = news_data['items'][:max_videos]
        add_log(job_id, f'✅ Found {len(items)} stories to cover')
        for item in items:
            add_log(job_id, f'   • {item.get("title","")[:50]}')

        produced_videos = []

        for idx, item in enumerate(items):
            add_log(job_id, f'\n📱 Story {idx+1}/{len(items)}: {item["title"]}')
            update_job(job_id, {'progress': 10 + int((idx/len(items))*75)})

            clip_path = None

            # Build complete Short: avatar intro + demo + avatar CTA
            add_log(job_id, f'   🎬 Building complete Short...')
            avatar_path = '/tmp/xlab_avatar.jpg'
            if not os.path.exists(avatar_path):
                avatar_path = None
            
            clip_path = build_ai_news_short(
                job_id, item, work_dir, idx, grok_key, avatar_path
            )

            if not clip_path:
                add_log(job_id, f'   ❌ No footage found - skipping')
                continue

            # Add narration
            script_text = item.get('script', item.get('hook', item['title']))
            add_log(job_id, f'   🎙️ Adding Grok narration...')
            tts_path = f'{work_dir}/narr_{idx}.mp3'
            if text_to_speech(script_text, tts_path):
                narr_out = f'{work_dir}/narrated_{idx}.mp4'
                if overlay_narration(clip_path, tts_path, narr_out):
                    clip_path = narr_out

            # Add text overlays
            add_log(job_id, f'   📝 Adding text overlays...')
            overlay_out = f'{work_dir}/overlay_{idx}.mp4'
            key_points = [
                item.get('hook', '')[:50],
                f"Tool: {item['title'][:40]}",
                item.get('why_viral', '')[:50]
            ]
            if add_text_overlays(clip_path, overlay_out, item['title'],
                                item.get('hook', ''), key_points,
                                'Follow for daily AI tools 🤖'):
                clip_path = overlay_out
                add_log(job_id, f'   ✅ Text overlays added')

            # Add music
            if music_enabled:
                music_out = f'{work_dir}/music_{idx}.mp4'
                if add_trending_music(clip_path, music_out, 'dramatic', 0.15):
                    clip_path = music_out

            # Save final clip
            final_path = f'{work_dir}/ainews_{idx}_{job_id[:6]}.mp4'
            import shutil as _sh
            _sh.copy2(clip_path, final_path)

            # Save to permanent library
            save_to_library(final_path, item['title'],
                          item.get('category','ai_news'), 'ai_news', job_id)

            # Upload to YouTube
            yt_id = ''
            if auto_upload and yt_token:
                add_log(job_id, f'   📤 Posting to YouTube...')
                desc = item.get('hook','') + '\n\n' + ' '.join(item.get('hashtags',['#AI','#Shorts']))
                yt_id = upload_to_youtube(final_path, item['title'], desc,
                                         item.get('hashtags',['#AI','#Shorts']), yt_token)
                if yt_id:
                    add_log(job_id, f'   ✅ YouTube: https://youtube.com/shorts/{yt_id}')

            # Post to X
            x_key = os.environ.get('X_API_KEY','')
            x_secret = os.environ.get('X_API_SECRET','')
            x_token = os.environ.get('X_ACCESS_TOKEN','')
            x_token_secret = os.environ.get('X_ACCESS_TOKEN_SECRET','')
            if x_key and x_secret and x_token and x_token_secret:
                add_log(job_id, f'   🐦 Posting to X...')
                yt_link = f'\nhttps://youtube.com/shorts/{yt_id}' if yt_id else ''
                x_text = f'{item["title"]}\n\n{item.get("hook","")}\n{" ".join(item.get("hashtags",["#AI"])[:4])}{yt_link}\n\nMade with XLAB'
                x_id = post_to_x(final_path, x_text, x_key, x_secret, x_token, x_token_secret)
                if x_id:
                    add_log(job_id, f'   ✅ X: https://x.com/i/web/status/{x_id}')

            produced_videos.append({
                'path': final_path,
                'title': item['title'],
                'category': item.get('category','ai'),
                'why_viral': item.get('why_viral',''),
                'affiliate': item.get('affiliate_angle',''),
                'yt_id': yt_id
            })
            add_log(job_id, f'   ✅ Story {idx+1} complete')

        if not produced_videos:
            raise Exception('No videos produced')

        # ZIP all videos
        zip_name = f'{work_dir}/ainews_{job_id[:8]}.zip'
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
            for v in produced_videos:
                if os.path.exists(v['path']):
                    zf.write(v['path'], os.path.basename(v['path']))

        zip_size = os.path.getsize(zip_name) / (1024*1024)

        add_log(job_id, f'\n{"="*35}')
        add_log(job_id, f'🎉 AI News Studio done!')
        add_log(job_id, f'   {len(produced_videos)} videos ready ({zip_size:.1f}MB)')
        for v in produced_videos:
            yt_link = f' → youtube.com/shorts/{v["yt_id"]}' if v.get('yt_id') else ''
            add_log(job_id, f'   ✓ {v["title"][:45]}{yt_link}')

        update_job(job_id, {
            'status': 'done', 'progress': 100,
            'completed_at': datetime.now().isoformat(),
            'zip_path': zip_name,
            'zip_size_mb': round(zip_size, 1),
            'total_clips': len(produced_videos),
            'videos': produced_videos
        })

    except Exception as e:
        add_log(job_id, f'❌ Fatal error: {e}')
        update_job(job_id, {'status': 'error', 'error': str(e)})


def process_grok_original_video(job_id, params):
    """Create fully AI-generated Short using Grok Aurora video + TTS.
    3 x 10s clips assembled = 30s Short. No source video needed."""
    work_dir = f'/tmp/grok_{job_id}'
    os.makedirs(work_dir, exist_ok=True)
    try:
        update_job(job_id, {'status': 'processing', 'started_at': datetime.now().isoformat()})
        topic = params.get('topic', '')
        grok_key = (os.environ.get('GROK_API_KEY', '') or params.get('grok_key', '')).strip()
        num_clips = int(params.get('num_clips', 3))
        clip_duration = int(params.get('clip_duration', 10))
        music_enabled = params.get('music_enabled') == 'Yes'
        music_style = params.get('music_style', 'energetic')
        auto_upload = params.get('auto_upload') == 'Yes'
        yt_token = params.get('yt_access_token', '')

        logger.info(f'Grok key check: env={bool(os.environ.get("GROK_API_KEY"))}, param={bool(params.get("grok_key"))}')
        if not grok_key:
            raise Exception('Grok API key required - add GROK_API_KEY to Railway variables')

        add_log(job_id, f'⚡ Writing script for: "{topic}"...')
        script = generate_ai_script(topic, num_clips, clip_duration, grok_key)
        if not script:
            raise Exception('Script generation failed')

        add_log(job_id, f'📝 "{script["title"]}"')
        add_log(job_id, f'   {len(script["points"])} scenes to generate')

        assembled_clips = []

        for idx, point in enumerate(script['points']):
            add_log(job_id, f'\n🎬 Scene {idx+1}: {point["title"]}')
            update_job(job_id, {'progress': 10 + int((idx / len(script['points'])) * 65)})

            clip_path = f'{work_dir}/clip_{idx}.mp4'
            video_prompt = (f'Cinematic 9:16 vertical video: {point["search_query"]}. '
                          f'Dynamic, engaging, social media optimized, professional cinematography. '
                          f'High contrast, dramatic lighting, smooth motion.')

            add_log(job_id, f'   🎥 Generating {clip_duration}s AI video...')
            if not grok_generate_video(video_prompt, clip_path, grok_key, clip_duration, '9:16'):
                add_log(job_id, f'   ⚠️ Generation failed - skipping')
                continue

            # Add narration
            if point.get('narration'):
                add_log(job_id, f'   🎙️ Adding narration...')
                tts_path = f'{work_dir}/narr_{idx}.mp3'
                if text_to_speech(point['narration'], tts_path):
                    narr_out = f'{work_dir}/narr_clip_{idx}.mp4'
                    if overlay_narration(clip_path, tts_path, narr_out):
                        clip_path = narr_out

            assembled_clips.append(clip_path)
            size = os.path.getsize(clip_path) / (1024*1024)
            add_log(job_id, f'   ✅ Scene {idx+1} ready ({size:.1f}MB)')

        if not assembled_clips:
            raise Exception('No clips generated - check Grok API key and credits')

        add_log(job_id, f'\n🎬 Assembling {len(assembled_clips)} scenes...')
        update_job(job_id, {'progress': 80})
        final_path = f'{work_dir}/xlab_ai_{job_id[:8]}.mp4'
        if not concatenate_clips(assembled_clips, final_path):
            raise Exception('Assembly failed')

        if music_enabled:
            add_log(job_id, f'🎵 Adding {music_style} music...')
            music_out = final_path.replace('.mp4', '_music.mp4')
            if add_trending_music(final_path, music_out, music_style, 0.2):
                os.replace(music_out, final_path)

        yt_id = ''
        if auto_upload and yt_token:
            add_log(job_id, '📤 Uploading to YouTube...')
            update_job(job_id, {'progress': 92})
            desc = script.get('hook','') + '\n\n' + ' '.join(script.get('hashtags',['#Shorts']))
            yt_id = upload_to_youtube(final_path, script['title'], desc,
                                      script.get('hashtags', ['#Shorts']), yt_token)
            if yt_id:
                add_log(job_id, f'   ✅ https://youtube.com/shorts/{yt_id}')

        size = os.path.getsize(final_path) / (1024*1024)

        # Save to permanent library
        save_to_library(final_path, script['title'], 'grok_original', 'grok_original', job_id)

        zip_name = f'{work_dir}/grok_video_{job_id[:8]}.zip'
        with zipfile.ZipFile(zip_name, 'w') as zf:
            zf.write(final_path, os.path.basename(final_path))
        zip_size = os.path.getsize(zip_name) / (1024*1024)

        add_log(job_id, f'\n🎉 AI video ready! ({size:.1f}MB) - {len(assembled_clips)*clip_duration}s total')
        update_job(job_id, {
            'status': 'done', 'progress': 100,
            'completed_at': datetime.now().isoformat(),
            'zip_path': zip_name, 'zip_size_mb': round(zip_size, 1),
            'total_clips': len(assembled_clips), 'yt_id': yt_id
        })

    except Exception as e:
        add_log(job_id, f'❌ Fatal error: {e}')
        update_job(job_id, {'status': 'error', 'error': str(e)})


def text_to_speech(text, output_path, style='normal'):
    """Convert text to speech — Groq Orpheus first (best quality), gTTS fallback."""
    groq_key = os.environ.get('GROQ_API_KEY', '').strip()
    
    # Add dramatic style tags for conspiracy content
    if style == 'conspiracy' and groq_key:
        styled = f'[dramatic] {text}'
    else:
        styled = text

    # Try Groq Orpheus TTS first — much better quality
    if groq_key:
        wav_path = output_path.replace('.mp3', '.wav')
        if groq_tts(styled, wav_path, groq_key, voice='zeus'):
            # Convert wav to mp3 for compatibility
            result = subprocess.run([
                'ffmpeg', '-i', wav_path, '-acodec', 'libmp3lame',
                '-q:a', '2', '-y', output_path, '-loglevel', 'quiet'
            ], capture_output=True, timeout=30)
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1000:
                logger.info('Groq Orpheus TTS success')
                return True

    # Fallback to gTTS (always works, no key needed)
    try:
        from gtts import gTTS
        tts = gTTS(text=text, lang='en', slow=False)
        tts.save(output_path)
        return os.path.exists(output_path)
    except Exception as e:
        logger.error(f'TTS error: {e}')
        return False


def overlay_narration(video_path, narration_path, output_path, narration_volume=0.8):
    """Mix narration audio over video, reducing original audio."""
    try:
        cmd = [
            'ffmpeg', '-i', video_path, '-i', narration_path,
            '-filter_complex',
            f'[0:a]volume=0.2[orig];[1:a]volume={narration_volume}[narr];[orig][narr]amix=inputs=2:duration=first[aout]',
            '-map', '0:v', '-map', '[aout]',
            '-c:v', 'copy', '-c:a', 'aac', '-b:a', '192k',
            '-shortest', '-y', output_path, '-loglevel', 'quiet'
        ]
        subprocess.run(cmd, check=True, timeout=120)
        return os.path.exists(output_path)
    except Exception as e:
        logger.error(f'Narration overlay error: {e}')
        return False


def add_text_overlay(video_path, text, output_path, position='top'):
    """Add text title card overlay to video."""
    try:
        y_pos = '50' if position == 'top' else 'h-th-50'
        safe_text = text.replace("'", "\'").replace(':', '\:')
        vf = f"drawtext=text='{safe_text}':fontsize=36:fontcolor=white:x=(w-tw)/2:y={y_pos}:box=1:boxcolor=black@0.6:boxborderw=10:font=Arial:fontweight=bold"
        cmd = [
            'ffmpeg', '-i', video_path, '-vf', vf,
            '-c:a', 'copy', '-y', output_path, '-loglevel', 'quiet'
        ]
        subprocess.run(cmd, check=True, timeout=60)
        return os.path.exists(output_path)
    except Exception as e:
        logger.error(f'Text overlay error: {e}')
        return False


def concatenate_clips(clip_paths, output_path):
    """Concatenate multiple clips - normalizes all to same format first."""
    try:
        if not clip_paths:
            return False

        work_dir = os.path.dirname(output_path)

        # Normalize all clips to same specs for consistent quality
        norm_paths = []
        for i, cp in enumerate(clip_paths):
            norm_path = os.path.join(work_dir, f'norm_{i}.mp4')
            if normalize_clip(cp, norm_path):
                norm_paths.append(norm_path)
            else:
                norm_paths.append(cp)

        if len(norm_paths) == 1:
            import shutil
            shutil.copy2(norm_paths[0], output_path)
            return True

        # Create concat file
        concat_file = output_path.replace('.mp4', '_concat.txt')
        with open(concat_file, 'w') as f:
            for cp in norm_paths:
                f.write(f"file '{cp}'\n")

        # Use copy since all clips are now same format
        cmd = [
            'ffmpeg', '-f', 'concat', '-safe', '0',
            '-i', concat_file,
            '-c', 'copy', '-y', output_path, '-loglevel', 'quiet'
        ]
        subprocess.run(cmd, check=True, timeout=300)
        if os.path.exists(concat_file):
            os.remove(concat_file)
        return os.path.exists(output_path) and os.path.getsize(output_path) > 50000
    except Exception as e:
        logger.error(f'Concat error: {e}')
        return False


def process_ai_content_job(job_id, params):
    """Level 3 - Full AI video assembly from a topic prompt."""
    work_dir = f'/tmp/aicontent_{job_id}'
    clips_dir = f'{work_dir}/clips'
    out_dir = f'{work_dir}/output'
    tts_dir = f'{work_dir}/tts'

    try:
        os.makedirs(clips_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)
        os.makedirs(tts_dir, exist_ok=True)
        update_job(job_id, {{'status': 'processing', 'started_at': datetime.now().isoformat()}})

        topic = params.get('topic', '')
        num_points = int(params.get('num_points', 5))
        clip_duration = int(params.get('clip_duration', 45))
        claude_key = params.get('claude_api_key', '') or CLAUDE_API_KEY
        auto_upload = params.get('auto_upload') == 'Yes'
        yt_token = params.get('yt_access_token', '')
        music_enabled = params.get('music_enabled') == 'Yes'
        music_style = params.get('music_style', 'energetic')
        watermark_text = params.get('watermark_text', '') if params.get('watermark_enabled') == 'Yes' else ''

        add_log(job_id, f'🤖 Generating script for: "{topic}"...')
        update_job(job_id, {{'progress': 5}})

        # Step 1 - Generate script
        script = generate_ai_script(topic, num_points, clip_duration, claude_key)
        if not script:
            raise Exception('Failed to generate script')

        add_log(job_id, f'📝 Script ready: "{script["title"]}"')
        add_log(job_id, f'   Hook: {script["hook"][:80]}...')
        add_log(job_id, f'   {len(script["points"])} points to find footage for')
        update_job(job_id, {{'progress': 10, 'script': script}})

        # Step 2 - Find and download clips for each point
        assembled_clips = []
        cookies_file = None
        yt_cookies = os.environ.get('YT_COOKIES', '')
        if yt_cookies:
            cookies_file = f'{work_dir}/cookies.txt'
            with open(cookies_file, 'w') as cf:
                cf.write(yt_cookies)

        for idx, point in enumerate(script['points']):
            add_log(job_id, f'\n🔍 Point {idx+1}/{len(script["points"])}: {point["title"]}')
            add_log(job_id, f'   Searching: "{point["search_query"]}"')
            update_job(job_id, {{'progress': 10 + int((idx / len(script['points'])) * 50)}})

            # Search and download
            clip_path = None

            # Try cobalt first
            search_result = search_best_video(point['search_query'], cookies_file)
            if search_result:
                add_log(job_id, f'   📹 Found: {search_result["title"][:50]}')
                clip_path = download_via_cobalt(search_result['url'], clips_dir, f'point_{idx}', job_id)

            # Fallback to yt-dlp
            if not clip_path:
                proxy = os.environ.get('PROXY_URL', '')
                cmd = ['yt-dlp', '--format', 'best', '--merge-output-format', 'mp4',
                       '--output', f'{clips_dir}/point_{idx}.mp4',
                       '--no-playlist', '--no-warnings', '--no-check-certificates',
                       '--extractor-args', 'youtube:player_client=web',
                       f'ytsearch1:{point["search_query"]}']
                if proxy: cmd += ['--proxy', proxy]
                if cookies_file: cmd += ['--cookies', cookies_file]
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                found = glob.glob(f'{clips_dir}/point_{idx}*.mp4')
                if found:
                    clip_path = found[0]

            if not clip_path or not os.path.exists(clip_path):
                add_log(job_id, f'   ⚠️ No footage found - skipping point')
                continue

            # Cut to required duration
            cut_path = f'{clips_dir}/cut_{idx}.mp4'
            vertical = is_vertical(clip_path)
            try:
                duration = get_duration(clip_path)
                start = max(0, duration * 0.2)  # Start 20% in to skip intros
                cut_vertical(clip_path, start, clip_duration, cut_path, vertical, watermark_text)
            except Exception as e:
                add_log(job_id, f'   ❌ Cut failed: {e}')
                continue

            # Add narration
            narration_text = point.get('narration', '')
            if narration_text and claude_key:
                add_log(job_id, f'   🎙️ Adding narration...')
                tts_path = f'{tts_dir}/narr_{idx}.mp3'
                if text_to_speech(narration_text, tts_path):
                    narr_out = f'{clips_dir}/narr_{idx}.mp4'
                    if overlay_narration(cut_path, tts_path, narr_out):
                        cut_path = narr_out

            # Add point title overlay
            title_out = f'{clips_dir}/titled_{idx}.mp4'
            if add_text_overlay(cut_path, point['title'], title_out):
                cut_path = title_out

            assembled_clips.append(cut_path)
            add_log(job_id, f'   ✅ Point {idx+1} ready')

        if not assembled_clips:
            raise Exception('No clips assembled - all points failed')

        add_log(job_id, f'\n🎬 Assembling {len(assembled_clips)} clips...')
        update_job(job_id, {{'progress': 70}})

        # Step 3 - Concatenate all clips
        final_path = f'{out_dir}/{job_id}_ai_video.mp4'
        if not concatenate_clips(assembled_clips, final_path):
            raise Exception('Failed to concatenate clips')

        # Step 4 - Add music if enabled
        if music_enabled:
            add_log(job_id, f'🎵 Adding {music_style} music...')
            music_out = final_path.replace('.mp4', '_music.mp4')
            if add_trending_music(final_path, music_out, music_style, 0.25):
                os.replace(music_out, final_path)

        # Step 5 - Auto upload to YouTube
        yt_id = ''
        if auto_upload and yt_token:
            add_log(job_id, f'📤 Uploading to YouTube...')
            update_job(job_id, {{'progress': 85}})
            try:
                desc = f'{script.get("hook", "")}\n\n{{" ".join(script.get("hashtags", ["#Shorts"]))}}'
                yt_id = upload_to_youtube(
                    final_path, script['title'], desc,
                    script.get('hashtags', ['#Shorts']), yt_token
                )
                add_log(job_id, f'   ✅ Live: https://youtube.com/shorts/{yt_id}')
            except Exception as e:
                add_log(job_id, f'   ❌ Upload failed: {e}')

        # ZIP for download
        zip_name = f'{work_dir}/ai_content_{job_id[:8]}.zip'
        with zipfile.ZipFile(zip_name, 'w') as zf:
            zf.write(final_path, os.path.basename(final_path))

        zip_size = os.path.getsize(zip_name) / (1024*1024)
        add_log(job_id, f'\n🎉 AI Video ready! ({zip_size:.1f}MB)')
        if yt_id:
            add_log(job_id, f'▶️ Watch: https://youtube.com/shorts/{yt_id}')

        update_job(job_id, {{
            'status': 'done', 'progress': 100,
            'completed_at': datetime.now().isoformat(),
            'zip_path': zip_name,
            'zip_size_mb': round(zip_size, 1),
            'yt_id': yt_id,
            'script': script,
            'total_clips': len(assembled_clips)
        }})

    except Exception as e:
        add_log(job_id, f'❌ Fatal error: {e}')
        update_job(job_id, {{'status': 'error', 'error': str(e)}})
    finally:
        pass  # Keep files for download


def search_best_video(query, cookies_file=None):
    """Search YouTube for best matching video."""
    try:
        proxy = os.environ.get('PROXY_URL', '')
        cmd = ['yt-dlp', '--dump-json', '--no-playlist', '--no-warnings',
               '--flat-playlist', '--match-filter', 'duration >= 30 & duration <= 7200',
               '--no-check-certificates']
        if proxy: cmd += ['--proxy', proxy]
        if cookies_file: cmd += ['--cookies', cookies_file]
        cmd.append(f'ytsearch1:{query}')
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        for line in r.stdout.strip().split('\n'):
            if line.strip():
                try:
                    info = json.loads(line)
                    return {{
                        'id': info.get('id', ''),
                        'title': info.get('title', '')[:60],
                        'url': info.get('webpage_url') or f'https://youtube.com/watch?v={info.get("id","")}',
                    }}
                except: continue
    except Exception as e:
        logger.error(f'Search error: {e}')
    return None


def generate_ai_metadata(video_title, clip_index, topic, api_key):
    import requests as req
    if not api_key.strip():
        return {'title': f'{video_title[:40]} #Shorts', 'description': '',
                'hashtags': ['#Shorts', '#Football'], 'score': 7, 'score_reason': ''}
    try:
        prompt = f"""YouTube Shorts expert. Generate metadata for this clip.
Source: "{video_title}" | Clip #{clip_index} | Topic: {topic}
Respond ONLY with JSON, no markdown:
{{"title":"catchy title under 60 chars with emoji","description":"2-3 sentences under 200 chars","hashtags":["#Shorts","5 more relevant"],"viral_score":8,"viral_reason":"one sentence"}}"""
        r = req.post('https://api.anthropic.com/v1/messages',
            headers={'x-api-key': api_key, 'anthropic-version': '2023-06-01', 'content-type': 'application/json'},
            json={'model': 'claude-sonnet-4-20250514', 'max_tokens': 400,
                  'messages': [{'role': 'user', 'content': prompt}]}, timeout=15)
        text = r.json()['content'][0]['text'].replace('```json','').replace('```','').strip()
        data = json.loads(text)
        return {'title': data.get('title', video_title)[:60], 'description': data.get('description',''),
                'hashtags': data.get('hashtags', ['#Shorts']), 'score': data.get('viral_score', 7),
                'score_reason': data.get('viral_reason', '')}
    except:
        return {'title': f'{video_title[:40]} #Shorts', 'description': '',
                'hashtags': ['#Shorts'], 'score': 7, 'score_reason': ''}

def upload_to_youtube(video_path, title, description, hashtags, access_token):
    """Upload using a frontend-obtained OAuth access token."""
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    import google.oauth2.credentials as oauth2_creds

    if not access_token:
        raise Exception('No YouTube access token provided.')

    creds   = oauth2_creds.Credentials(token=access_token)
    youtube = build('youtube', 'v3', credentials=creds)

    youtube = build('youtube', 'v3', credentials=creds)
    body = {
        'snippet': {
            'title': title[:100],
            'description': f"{description}\n\n{' '.join(hashtags)}"[:5000],
            'tags': [h.replace('#','') for h in hashtags],
            'categoryId': '17'
        },
        'status': {'privacyStatus': 'public', 'selfDeclaredMadeForKids': False}
    }
    media = MediaFileUpload(video_path, mimetype='video/mp4', resumable=True)
    req2  = youtube.videos().insert(part='snippet,status', body=body, media_body=media)
    response = None
    while response is None:
        _, response = req2.next_chunk()
    return response.get('id', '')

# ============================================================
# Background job processor
# ============================================================

def download_via_rapidapi(video_url, output_dir, video_id):
    """Download via YouTube Media Downloader RapidAPI - most reliable."""
    import requests as req
    import re

    api_key = os.environ.get('RAPIDAPI_KEY', '')
    if not api_key:
        return None

    # Extract video ID
    vid_id = video_id
    m = re.search(r'(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})', video_url)
    if m:
        vid_id = m.group(1)

    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': 'youtube-media-downloader.p.rapidapi.com',
        'Content-Type': 'application/json'
    }

    try:
        # Step 1: Get video details and download URLs
        r = req.get(
            'https://youtube-media-downloader.p.rapidapi.com/v2/video/details',
            params={'videoId': vid_id, 'videos': 'true', 'audios': 'false'},
            headers=headers,
            timeout=15
        )
        if r.status_code != 200:
            logger.error(f'RapidAPI details error: {r.status_code} {r.text[:100]}')
            return None

        data = r.json()
        logger.info(f'RapidAPI response keys: {list(data.keys())} errorId: {data.get("errorId")}')
        if data.get('errorId') != 'Success':
            logger.error(f'RapidAPI error: {data.get("errorId")} - {str(data)[:200]}')
            return None

        # Get best video stream
        videos = data.get('videos', {}).get('items', [])
        logger.info(f'RapidAPI videos found: {len(videos)}')
        if not videos:
            # Try alternative response structure
            videos = data.get('streamingData', {}).get('formats', [])

        # Pick best quality MP4
        best_url = None
        best_quality = 0
        for v in videos:
            url = v.get('url') or v.get('file')
            quality = v.get('height', 0) or v.get('quality', 0)
            ext = v.get('extension', '') or v.get('mimeType', '')
            if url and 'mp4' in str(ext).lower() and quality > best_quality:
                best_url = url
                best_quality = quality

        # Fallback - take first video url
        if not best_url and videos:
            best_url = videos[0].get('url') or videos[0].get('file')

        if not best_url:
            logger.error(f'RapidAPI: no video URL in response')
            return None

        # Step 2: Download the video
        logger.info(f'RapidAPI: downloading {best_quality}p from {best_url[:60]}')
        output_path = os.path.join(output_dir, f'{video_id}.mp4')
        temp_path = output_path + '.tmp'

        # Use wget for faster download - much faster than requests streaming
        wget_result = subprocess.run([
            'wget', '-q', '-O', temp_path,
            '--timeout=60', '--tries=3',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            best_url
        ], capture_output=True, timeout=300)

        if not os.path.exists(temp_path) or os.path.getsize(temp_path) < 100000:
            # Fallback to requests if wget fails
            r2 = req.get(best_url, stream=True, timeout=300,
                        headers={'User-Agent': 'Mozilla/5.0'},
                        allow_redirects=True)
            with open(temp_path, 'wb') as f:
                for chunk in r2.iter_content(chunk_size=1024*1024):  # 1MB chunks
                    if chunk:
                        f.write(chunk)

        if not os.path.exists(temp_path) or os.path.getsize(temp_path) < 100000:
            return None

        # Check what ffmpeg thinks the file is
        probe = subprocess.run([
            'ffmpeg', '-i', temp_path
        ], capture_output=True, text=True, timeout=30)
        probe_output = probe.stderr[:500]
        logger.info(f'RapidAPI file probe: {probe_output[:200]}')

        # Detect if it's a valid video
        is_valid = any(x in probe_output for x in ['Video:', 'Audio:', 'Duration:'])
        size_mb = os.path.getsize(temp_path) / (1024*1024)
        logger.info(f'File valid: {is_valid}, size: {size_mb:.1f}MB')

        remuxed = False

        if is_valid:
            # Method 1: Simple copy remux
            result = subprocess.run([
                'ffmpeg', '-i', temp_path,
                '-c', 'copy', '-movflags', '+faststart',
                '-y', output_path, '-loglevel', 'quiet'
            ], capture_output=True, timeout=120)

            if os.path.exists(output_path) and os.path.getsize(output_path) > 100000:
                remuxed = True
            else:
                # Method 2: Re-encode
                result2 = subprocess.run([
                    'ffmpeg', '-i', temp_path,
                    '-c:v', 'libx264', '-preset', 'fast', '-crf', '23',
                    '-c:a', 'aac', '-b:a', '128k',
                    '-y', output_path, '-loglevel', 'quiet'
                ], capture_output=True, timeout=300)
                if os.path.exists(output_path) and os.path.getsize(output_path) > 100000:
                    remuxed = True
        else:
            logger.error(f'RapidAPI returned invalid file: {probe_output[:100]}')
            # Try using file directly - maybe ffmpeg is wrong
            import shutil
            shutil.copy2(temp_path, output_path)
            if os.path.exists(output_path) and os.path.getsize(output_path) > 100000:
                remuxed = True

        # Clean up temp
        if os.path.exists(temp_path):
            os.remove(temp_path)

        if remuxed:
            return output_path

    except Exception as e:
        logger.error(f'RapidAPI download error: {e}')
    return None


def download_via_piped(video_url, output_dir, video_id):
    """Download via Piped API - free, no account needed."""
    import requests as req
    piped_instances = [
        'https://pipedapi.kavin.rocks',
        'https://piped-api.garudalinux.org',
        'https://api.piped.projectsegfault.net',
        'https://pipedapi.tokhmi.xyz',
    ]
    # Extract video ID
    vid_id = video_id
    if 'youtube.com' in video_url or 'youtu.be' in video_url:
        import re
        m = re.search(r'(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})', video_url)
        if m: vid_id = m.group(1)

    for instance in piped_instances:
        try:
            r = req.get(f'{instance}/streams/{vid_id}', timeout=15)
            if r.status_code != 200: continue
            data = r.json()
            # Get best video stream
            streams = data.get('videoStreams', [])
            # Prefer 1080p, fallback to 720p, then best available
            best = None
            for quality in ['1080', '720', '480', '360']:
                for s in streams:
                    if quality in str(s.get('quality','')) and s.get('mimeType','').startswith('video'):
                        best = s
                        break
                if best: break
            if not best and streams:
                best = streams[0]
            if not best: continue

            stream_url = best.get('url')
            if not stream_url: continue

            # Download the stream
            output_path = os.path.join(output_dir, f'{video_id}.mp4')
            r2 = req.get(stream_url, stream=True, timeout=300,
                        headers={'User-Agent': 'Mozilla/5.0'})
            with open(output_path, 'wb') as f:
                for chunk in r2.iter_content(chunk_size=65536):
                    f.write(chunk)
            if os.path.exists(output_path) and os.path.getsize(output_path) > 100000:
                return output_path
        except Exception as e:
            logger.error(f'Piped {instance} error: {e}')
            continue
    return None


def download_via_invidious(video_url, output_dir, video_id):
    """Download via Invidious - free, no account needed."""
    import requests as req
    invidious_instances = [
        'https://invidious.snopyta.org',
        'https://y.com.sb',
        'https://invidious.kavin.rocks',
        'https://vid.puffyan.us',
        'https://invidious.tiekoetter.com',
    ]
    vid_id = video_id
    if 'youtube.com' in video_url or 'youtu.be' in video_url:
        import re
        m = re.search(r'(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})', video_url)
        if m: vid_id = m.group(1)

    for instance in invidious_instances:
        try:
            r = req.get(f'{instance}/api/v1/videos/{vid_id}', timeout=15)
            if r.status_code != 200: continue
            data = r.json()
            formats = data.get('formatStreams', []) + data.get('adaptiveFormats', [])
            # Find best mp4
            best = None
            for quality in ['1080', '720', '480', '360']:
                for f in formats:
                    if quality in str(f.get('qualityLabel','')) and f.get('type','').startswith('video/mp4'):
                        best = f
                        break
                if best: break
            if not best:
                for f in formats:
                    if f.get('type','').startswith('video/mp4'):
                        best = f
                        break
            if not best: continue

            stream_url = best.get('url')
            if not stream_url: continue

            output_path = os.path.join(output_dir, f'{video_id}.mp4')
            r2 = req.get(stream_url, stream=True, timeout=300,
                        headers={'User-Agent': 'Mozilla/5.0'})
            with open(output_path, 'wb') as f:
                for chunk in r2.iter_content(chunk_size=65536):
                    f.write(chunk)
            if os.path.exists(output_path) and os.path.getsize(output_path) > 100000:
                return output_path
        except Exception as e:
            logger.error(f'Invidious {instance} error: {e}')
            continue
    return None


def download_via_cobalt(url, output_dir, video_id, job_id):
    """Try to download video using cobalt.tools API."""
    import requests as req

    # Try multiple cobalt instances
    cobalt_instances = [
        'https://api.cobalt.tools',
        'https://cobalt.api.timelessnesses.me',
        'https://cobalt.canine.tools',
    ]

    for instance in cobalt_instances:
        try:
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (compatible; XLAB/1.0)',
            }
            payload = {
                'url': url,
                'videoQuality': '720',
                'filenameStyle': 'basic',
                'downloadMode': 'auto',
            }
            logger.info(f'Trying cobalt instance: {instance}')
            r = req.post(f'{instance}/', json=payload, headers=headers, timeout=30)
            logger.info(f'Cobalt {instance} response: {r.status_code} {r.text[:500]}')
            
            if r.status_code != 200:
                continue
                
            data = r.json()
            logger.info(f'Cobalt data: {data}')

            if data.get('status') in ['stream', 'redirect', 'tunnel']:
                download_url = data.get('url')
                if download_url:
                    output_path = os.path.join(output_dir, f'{video_id}.mp4')
                    r2 = req.get(download_url, stream=True, timeout=300,
                                headers={'User-Agent': 'Mozilla/5.0'})
                    with open(output_path, 'wb') as f:
                        for chunk in r2.iter_content(chunk_size=8192):
                            f.write(chunk)
                    if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
                        return output_path
            elif data.get('status') == 'picker':
                # Multiple streams - pick first video
                items = data.get('picker', [])
                for item in items:
                    if item.get('type') == 'video':
                        download_url = item.get('url')
                        if download_url:
                            output_path = os.path.join(output_dir, f'{video_id}.mp4')
                            r2 = req.get(download_url, stream=True, timeout=300)
                            with open(output_path, 'wb') as f:
                                for chunk in r2.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            if os.path.exists(output_path) and os.path.getsize(output_path) > 10000:
                                return output_path
                            break
            else:
                logger.info(f'Cobalt bad status: {data.get("status")} error: {data.get("error")}')
                continue

        except Exception as e:
            logger.error(f'Cobalt instance {instance} failed: {e}')
            continue

    return None

def process_job(job_id, params):
    work_dir = f'/tmp/job_{job_id}'
    raw_dir  = f'{work_dir}/raw'
    out_dir  = f'{work_dir}/output'

    try:
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)
        update_job(job_id, {'status': 'processing', 'started_at': datetime.now().isoformat()})

        selected_videos  = params.get('selected_videos', [])
        clip_length      = int(params.get('clip_length', 45))
        clips_per_video  = int(params.get('clips_per_video', 3))
        captions         = params.get('captions', 'No') == 'Yes'
        caption_lang     = params.get('caption_lang', 'en')
        wm_text          = params.get('watermark_text', '') if params.get('watermark_enabled') == 'Yes' else ''
        music_enabled    = params.get('music_enabled') == 'Yes'
        music_style      = params.get('music_style', 'energetic')
        music_volume     = float(params.get('music_volume', 0.3))
        wm_pos           = params.get('watermark_position', 'bottom_right')
        ai_enabled       = params.get('ai_metadata') == 'Yes'
        claude_key       = params.get('claude_api_key', '') or CLAUDE_API_KEY
        topic            = params.get('topic', 'sports')
        auto_upload = params.get('auto_upload') == 'Yes'
        yt_token    = params.get('yt_access_token', '')

        # Write cookies file if available
        cookies_file = None
        yt_cookies = os.environ.get('YT_COOKIES', '')
        if yt_cookies:
            cookies_file = f'{work_dir}/cookies.txt'
            with open(cookies_file, 'w') as cf:
                cf.write(yt_cookies)

        # List available formats for first video (debug)
        if selected_videos and cookies_file:
            debug_cmd = ['yt-dlp', '--list-formats', '--no-warnings',
                        '--cookies', cookies_file]
            proxy = os.environ.get('PROXY_URL', '')
            if proxy:
                debug_cmd += ['--proxy', proxy]
            debug_cmd.append(selected_videos[0]['url'])
            debug_result = subprocess.run(debug_cmd, capture_output=True, text=True, timeout=60)
            add_log(job_id, f'Available formats: {debug_result.stdout[:1000]}')
            add_log(job_id, f'Format errors: {debug_result.stderr[:500]}')

        # Skip browser upload wait - proxy handles all downloads directly
        update_job(job_id, {'uploads_ready': True})

        # Check for browser-uploaded videos
        upload_dir = f'/tmp/uploads/{job_id}'
        browser_uploads = {}
        if os.path.exists(upload_dir):
            for fname in os.listdir(upload_dir):
                if fname.endswith('.mp4'):
                    vid_id = fname.replace('.mp4', '')
                    browser_uploads[vid_id] = os.path.join(upload_dir, fname)
        add_log(job_id, f'📱 Browser uploads found: {len(browser_uploads)}')

        proxy = os.environ.get('PROXY_URL', '')
        add_log(job_id, f'📥 Processing {len(selected_videos)} video(s)...')
        if proxy:
            add_log(job_id, f'   🔒 Using residential proxy for downloads')
        else:
            add_log(job_id, f'   ⚠️ No proxy set - add PROXY_URL env variable')

        for v in selected_videos:
            add_log(job_id, f'   ⬇️  {v["title"][:50]}...')
            downloaded_path = None

            # Check browser upload first
            if v['id'] in browser_uploads:
                import shutil as sh
                dest = os.path.join(raw_dir, f'{v["id"]}.mp4')
                sh.copy2(browser_uploads[v['id']], dest)
                add_log(job_id, f'   ✅ Using browser upload')
                downloaded_path = dest

            # Method 0: RapidAPI YouTube Media Downloader (most reliable, no IP issues)
            if not downloaded_path:
                rapidapi_key = os.environ.get('RAPIDAPI_KEY', '')
                if rapidapi_key:
                    add_log(job_id, f'   🚀 Downloading via RapidAPI...')
                    downloaded_path = download_via_rapidapi(v['url'], raw_dir, v['id'])
                    if downloaded_path:
                        size = os.path.getsize(downloaded_path) / (1024*1024)
                        add_log(job_id, f'   ✅ Downloaded via RapidAPI ({size:.1f}MB)')

            # Method 1: yt-dlp with residential proxy
            if not downloaded_path and proxy:
                add_log(job_id, f'   🔄 Downloading via proxy...')
                out_tmpl = f'{raw_dir}/{v["id"]}.%(ext)s'

                # Quality cascade - tries best quality, audio optional
                fmt = (
                    'bestvideo[height<=1080]+bestaudio/bestvideo[height<=1080]'
                    '/bestvideo[height<=720]+bestaudio/bestvideo[height<=720]'
                    '/bestvideo[height<=480]+bestaudio/bestvideo[height<=480]'
                    '/bestvideo+bestaudio/bestvideo'
                )
                add_log(job_id, f'   📋 Requesting best quality (1080p → 720p → 480p fallback)...')
                cmd = [
                    'yt-dlp',
                    '--format', fmt,
                    '--merge-output-format', 'mp4',
                    '--output', out_tmpl,
                    '--no-playlist', '--no-warnings',
                    '--proxy', proxy,
                    '--socket-timeout', '30',
                    '--retries', '3',
                    '--extractor-args', 'youtube:player_client=android,ios,web',
                    '--add-header', 'User-Agent:com.google.ios.youtube/19.29.1 CFNetwork/1474 Darwin/23.0.0',
                ]
                if cookies_file:
                    cmd += ['--cookies', cookies_file]
                cmd.append(v['url'])
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                found = glob.glob(f'{raw_dir}/{v["id"]}*.mp4')
                if found:
                    downloaded_path = found[0]
                    size = os.path.getsize(downloaded_path) / (1024*1024)
                    add_log(job_id, f'   ✅ Downloaded via proxy ({size:.1f}MB)')
                else:
                    err = result.stderr[-200:] if result.stderr else 'no output'
                    add_log(job_id, f'   ⚠️ Proxy download failed: {err[-100:]}')

            # Method 2: cobalt.tools
            if not downloaded_path:
                add_log(job_id, f'   🔄 Trying cobalt.tools...')
                downloaded_path = download_via_cobalt(v['url'], raw_dir, v['id'], job_id)
                if downloaded_path:
                    size = os.path.getsize(downloaded_path) / (1024*1024)
                    add_log(job_id, f'   ✅ Downloaded via cobalt ({size:.1f}MB)')

            # Method 3: Piped API (free, no account)
            if not downloaded_path:
                add_log(job_id, f'   🔄 Trying Piped API...')
                downloaded_path = download_via_piped(v['url'], raw_dir, v['id'])
                if downloaded_path:
                    size = os.path.getsize(downloaded_path) / (1024*1024)
                    add_log(job_id, f'   ✅ Downloaded via Piped ({size:.1f}MB)')

            # Method 4: Invidious (free, no account)
            if not downloaded_path:
                add_log(job_id, f'   🔄 Trying Invidious...')
                downloaded_path = download_via_invidious(v['url'], raw_dir, v['id'])
                if downloaded_path:
                    size = os.path.getsize(downloaded_path) / (1024*1024)
                    add_log(job_id, f'   ✅ Downloaded via Invidious ({size:.1f}MB)')

            # Method 5: yt-dlp direct last resort
            if not downloaded_path:
                add_log(job_id, f'   🔄 Trying direct download...')
                out_tmpl = f'{raw_dir}/{v["id"]}.%(ext)s'
                cmd = [
                    'yt-dlp', '--format', 'best',
                    '--merge-output-format', 'mp4',
                    '--output', out_tmpl,
                    '--no-playlist', '--no-warnings',
                    '--extractor-args', 'youtube:player_client=web',
                ]
                if cookies_file:
                    cmd += ['--cookies', cookies_file]
                cmd.append(v['url'])
                subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                found = glob.glob(f'{raw_dir}/{v["id"]}*.mp4')
                if found:
                    downloaded_path = found[0]
                    size = os.path.getsize(downloaded_path) / (1024*1024)
                    add_log(job_id, f'   ✅ Downloaded direct ({size:.1f}MB)')
                else:
                    add_log(job_id, f'   ❌ All 5 download methods failed - skipping')

        downloaded = glob.glob(f'{raw_dir}/*.mp4')
        add_log(job_id, f'✅ Downloaded {len(downloaded)} file(s)')
        if not downloaded:
            raise Exception('No videos downloaded - check PROXY_URL environment variable')

        all_clips     = []
        clip_metadata = []
        total         = len(downloaded)

        for vi, video_path in enumerate(downloaded):
            vname        = os.path.splitext(os.path.basename(video_path))[0][:35]
            source_title = selected_videos[vi]['title'] if vi < len(selected_videos) else vname
            add_log(job_id, f'\n🎬 Processing {vi+1}/{total}: {vname}')
            update_job(job_id, {'progress': int((vi / total) * 60) + 10})

            try:
                duration = get_duration(video_path)
                vertical = is_vertical(video_path)
                add_log(job_id, f'   ⏱ {duration/60:.1f} min | {"📱 Vertical" if vertical else "🖥 Landscape"}')

                if duration <= clip_length + 5:
                    clips_to_cut = [(0, duration)]
                else:
                    audio_path = f'{work_dir}/audio_{vi}.wav'
                    add_log(job_id, '   🔊 Detecting highlights...')
                    extract_audio(video_path, audio_path)
                    peaks = detect_peaks(audio_path)
                    valid = [p for p in peaks if p > 10 and p < duration - clip_length]
                    sel   = valid[:clips_per_video]
                    if not sel:
                        step = duration / (clips_per_video + 1)
                        sel  = [step * (i+1) for i in range(clips_per_video)]
                    clips_to_cut = [(p, clip_length) for p in sel]
                    if os.path.exists(audio_path): os.remove(audio_path)

                for ci, (peak, clen) in enumerate(clips_to_cut):
                    clip_name = f'short_{vi+1}_{ci+1}_{vname[:20]}.mp4'
                    clip_path = os.path.join(out_dir, clip_name)
                    temp_path = clip_path.replace('.mp4', '_temp.mp4')

                    add_log(job_id, f'   ✂️  Clip {ci+1} @ {peak:.0f}s')
                    cut_vertical(video_path, peak, clen, temp_path, vertical, wm_text, wm_pos)

                    if captions:
                        add_log(job_id, '      💬 Adding captions...')
                        burn_captions(temp_path, clip_path, caption_lang)
                        os.remove(temp_path)
                    else:
                        os.rename(temp_path, clip_path)

                    meta = {'title': source_title, 'description': '', 'hashtags': ['#Shorts'], 'score': 7, 'score_reason': ''}
                    if ai_enabled:
                        add_log(job_id, '      🤖 AI metadata...')
                        meta = generate_ai_metadata(source_title, ci+1, topic, claude_key)

                    size = os.path.getsize(clip_path) / (1024 * 1024)
                    add_log(job_id, f'      ✅ {clip_name} ({size:.1f}MB) - {meta["score"]}/10')

                    all_clips.append(clip_path)
                    clip_metadata.append({
                        'name': clip_name, 'path': clip_path,
                        'title': meta['title'], 'description': meta['description'],
                        'hashtags': meta['hashtags'], 'score': meta['score'],
                        'score_reason': meta.get('score_reason', ''),
                        'size_mb': round(size, 1), 'uploaded': False, 'yt_id': ''
                    })

            except Exception as e:
                add_log(job_id, f'   ❌ Error: {e}')
                continue

        # Auto YouTube upload
        if auto_upload and yt_token:
            add_log(job_id, f'\n📤 Uploading {len(all_clips)} Shorts to YouTube...')
            update_job(job_id, {'progress': 80})
            for cm in clip_metadata:
                add_log(job_id, f'   ⬆️  {cm["title"][:50]}...')
                try:
                    yt_id = upload_to_youtube(cm['path'], cm['title'], cm['description'],
                        cm['hashtags'], yt_token)
                    cm['uploaded'] = True
                    cm['yt_id']    = yt_id
                    add_log(job_id, f'      ✅ https://youtube.com/shorts/{yt_id}')
                except Exception as e:
                    add_log(job_id, f'      ❌ Upload failed: {e}')

        # ZIP
        add_log(job_id, '\n📦 Packaging...')
        update_job(job_id, {'progress': 90})
        zip_name = f'{work_dir}/shorts_{job_id[:8]}.zip'
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zf:
            for c in all_clips:
                zf.write(c, os.path.basename(c))

        zip_size = os.path.getsize(zip_name) / (1024 * 1024)
        add_log(job_id, f'\n🎉 DONE! {len(all_clips)} Shorts ready ({zip_size:.1f}MB)')

        update_job(job_id, {
            'status':       'done',
            'progress':     100,
            'completed_at': datetime.now().isoformat(),
            'zip_path':     zip_name,
            'zip_size_mb':  round(zip_size, 1),
            'clips':        clip_metadata,
            'total_clips':  len(all_clips)
        })

    except Exception as e:
        add_log(job_id, f'❌ Fatal error: {e}')
        update_job(job_id, {'status': 'error', 'error': str(e)})

# ============================================================
# Flask routes
# ============================================================
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '')

@app.route('/')
def index():
    return render_template('index.html', google_client_id=GOOGLE_CLIENT_ID)

@app.route('/api/upload', methods=['POST'])
def upload_video():
    """Receive video uploaded from browser and save to temp storage."""
    try:
        job_id = request.form.get('job_id')
        video_id = request.form.get('video_id')
        
        if not job_id or not video_id:
            return jsonify({'error': 'Missing job_id or video_id'}), 400
        
        file = request.files.get('video')
        if not file:
            return jsonify({'error': 'No video file'}), 400
        
        # Save to temp location
        upload_dir = f'/tmp/uploads/{job_id}'
        os.makedirs(upload_dir, exist_ok=True)
        save_path = f'{upload_dir}/{video_id}.mp4'
        file.save(save_path)
        
        size_mb = os.path.getsize(save_path) / (1024*1024)
        logger.info(f'Uploaded {video_id} for job {job_id}: {size_mb:.1f}MB')
        
        return jsonify({'success': True, 'path': save_path, 'size_mb': round(size_mb,1)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def search_via_invidious(query, max_results=5):
    """Search YouTube via proxy-routed yt-dlp with multiple fallback methods."""
    import requests as req
    proxy = os.environ.get('PROXY_URL', '')
    proxies = {'http': proxy, 'https': proxy} if proxy else {}

    # Method 1: YouTube search page scrape via proxy
    if proxy:
        try:
            r = req.get('https://www.youtube.com/results',
                params={'search_query': query, 'sp': 'EgIQAQ%3D%3D'},
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml',
                },
                proxies=proxies, timeout=20)
            if r.status_code == 200:
                import re
                # Extract video IDs and titles from YouTube search page
                ids = re.findall(r'"videoId":"([a-zA-Z0-9_-]{11})"', r.text)
                titles = re.findall(r'"title":{"runs":\[{"text":"([^"]+)"', r.text)
                channels = re.findall(r'"ownerText":{"runs":\[{"text":"([^"]+)"', r.text)
                durations = re.findall(r'"lengthText":{"accessibility":.*?"simpleText":"([^"]+)"', r.text)
                
                seen = set()
                videos = []
                for i, vid_id in enumerate(ids):
                    if vid_id in seen: continue
                    seen.add(vid_id)
                    dur_str = durations[i] if i < len(durations) else '0:00'
                    # Parse duration string like "10:24" to seconds
                    try:
                        parts = dur_str.split(':')
                        dur_secs = int(parts[-1]) + int(parts[-2])*60 + (int(parts[-3])*3600 if len(parts)>2 else 0)
                    except:
                        dur_secs = 0
                    videos.append({
                        'id': vid_id,
                        'title': (titles[i] if i < len(titles) else query)[:60],
                        'duration': dur_secs,
                        'channel': channels[i] if i < len(channels) else 'Unknown',
                        'thumbnail': f'https://img.youtube.com/vi/{vid_id}/hqdefault.jpg',
                        'url': f'https://youtube.com/watch?v={vid_id}',
                        'platform': 'youtube',
                        'view_count': 0
                    })
                    if len(videos) >= max_results:
                        break
                if videos:
                    logger.info(f'YouTube proxy search found {len(videos)} results')
                    return videos
        except Exception as e:
            logger.error(f'YouTube proxy search error: {e}')

    # Method 2: Invidious instances via proxy
    instances = [
        'https://invidious.snopyta.org',
        'https://y.com.sb',
        'https://invidious.kavin.rocks',
        'https://vid.puffyan.us',
        'https://inv.tux.pizza',
        'https://invidious.nerdvpn.de',
    ]
    for instance in instances:
        try:
            r = req.get(f'{instance}/api/v1/search',
                params={'q': query, 'type': 'video', 'page': 1},
                proxies=proxies, timeout=15)
            if r.status_code != 200: continue
            results = r.json()
            if not isinstance(results, list): continue
            videos = []
            for v in results[:max_results]:
                if v.get('type') != 'video': continue
                vid_id = v.get('videoId','')
                if not vid_id: continue
                videos.append({
                    'id': vid_id,
                    'title': v.get('title','Unknown')[:60],
                    'duration': v.get('lengthSeconds', 0),
                    'channel': v.get('author','Unknown'),
                    'thumbnail': f'https://img.youtube.com/vi/{vid_id}/hqdefault.jpg',
                    'url': f'https://youtube.com/watch?v={vid_id}',
                    'platform': 'youtube',
                    'view_count': v.get('viewCount', 0) or 0
                })
            if videos:
                logger.info(f'Invidious search found {len(videos)} results from {instance}')
                return videos
        except Exception as e:
            logger.error(f'Invidious {instance} error: {e}')
            continue
    return []


@app.route('/api/formats', methods=['POST'])
def get_formats():
    data = request.json
    url = data.get('url', '')
    proxy = os.environ.get('PROXY_URL', '')
    try:
        cmd = [
            'yt-dlp', '--list-formats', '--no-warnings',
            '--extractor-args', 'youtube:player_client=android,ios,web',
        ]
        if proxy:
            cmd += ['--proxy', proxy]
        cmd.append(url)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        formats = []
        seen_res = set()

        for line in result.stdout.split('\n'):
            line = line.strip()
            if not line or line.startswith('ID') or line.startswith('-') or line.startswith('['):
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            
            fmt_id = parts[0]
            ext = parts[1] if len(parts) > 1 else ''
            
            # Find resolution in the line
            res = ''
            for p in parts:
                if 'x' in p and p.replace('x','').replace('0123456789','').strip() == '':
                    res = p
                    break
                if p.endswith('p') and p[:-1].isdigit():
                    res = p
                    break
            
            # Build label
            if res and ext in ('mp4', 'webm', 'm4a', 'avc1', 'av01'):
                key = f"{res}_{ext}"
                if key not in seen_res:
                    seen_res.add(key)
                    size_info = ''
                    for p in parts:
                        if 'MiB' in p or 'KiB' in p or 'GiB' in p:
                            size_info = f' ~{p}'
                            break
                    formats.append({
                        'id': fmt_id,
                        'label': f'{res} {ext.upper()}{size_info}',
                        'ext': ext
                    })

        # Sort by resolution descending
        def res_sort(f):
            label = f['label']
            for q in ['2160','1440','1080','720','480','360','240','144']:
                if q in label:
                    return int(q)
            return 0
        formats.sort(key=res_sort, reverse=True)

        # Always add auto option first
        options = [{'id': 'auto', 'label': 'Best available (auto)', 'ext': 'mp4'}] + formats[:8]
        logger.info(f'Formats for {url}: {[f["label"] for f in options]}')
        return jsonify({'formats': options})
    except Exception as e:
        logger.error(f'Format fetch error: {e}')
        return jsonify({'formats': [{'id': 'auto', 'label': 'Best available (auto)', 'ext': 'mp4'}]})


# ============================================================
# MULTI-CHANNEL MANAGER
# ============================================================

CHANNELS = {}  # channel_id -> channel config
GENERATED_VIDEOS = {}  # video_id -> metadata + path

VIDEOS_DIR = '/tmp/xlab_library'
os.makedirs(VIDEOS_DIR, exist_ok=True)


def save_to_library(video_path, title, category, source, job_id):
    """Save generated video to permanent library for multi-platform publishing."""
    vid_id = str(uuid.uuid4())[:8]
    filename = f'{vid_id}_{title[:30].replace(" ","_").replace("/","")}.mp4'
    lib_path = os.path.join(VIDEOS_DIR, filename)
    try:
        import shutil
        shutil.copy2(video_path, lib_path)
        GENERATED_VIDEOS[vid_id] = {
            'id': vid_id,
            'title': title,
            'category': category,
            'source': source,  # ai_news, grok_original, studio, clips
            'path': lib_path,
            'filename': filename,
            'created_at': datetime.now().isoformat(),
            'job_id': job_id,
            'size_mb': round(os.path.getsize(lib_path)/(1024*1024), 1),
            'published': []  # track which platforms published to
        }
        return vid_id
    except Exception as e:
        logger.error(f'Library save error: {e}')
        return None


@app.route('/api/library', methods=['GET'])
def get_library():
    videos = sorted(GENERATED_VIDEOS.values(),
                   key=lambda x: x['created_at'], reverse=True)
    return jsonify(videos)


@app.route('/api/library/<vid_id>/download', methods=['GET'])
def download_library_video(vid_id):
    if vid_id not in GENERATED_VIDEOS:
        return jsonify({'error': 'Not found'}), 404
    v = GENERATED_VIDEOS[vid_id]
    if not os.path.exists(v['path']):
        return jsonify({'error': 'File not found'}), 404
    return send_file(v['path'], as_attachment=True, download_name=v['filename'])


@app.route('/api/library/<vid_id>/mark-published', methods=['POST'])
def mark_published(vid_id):
    data = request.json
    if vid_id in GENERATED_VIDEOS:
        platform = data.get('platform', 'unknown')
        if platform not in GENERATED_VIDEOS[vid_id]['published']:
            GENERATED_VIDEOS[vid_id]['published'].append(platform)
    return jsonify({'success': True})


@app.route('/api/library/<vid_id>', methods=['DELETE'])
def delete_library_video(vid_id):
    if vid_id in GENERATED_VIDEOS:
        v = GENERATED_VIDEOS[vid_id]
        if os.path.exists(v['path']):
            os.remove(v['path'])
        del GENERATED_VIDEOS[vid_id]
    return jsonify({'success': True})

@app.route('/api/channels', methods=['GET'])
def get_channels():
    return jsonify(list(CHANNELS.values()))

@app.route('/api/channels', methods=['POST'])
def create_channel():
    data = request.json
    cid = str(uuid.uuid4())[:8]
    channel = {
        'id': cid,
        'name': data.get('name', 'My Channel'),
        'niche': data.get('niche', 'ai tools'),
        'categories': data.get('categories', ['ai_tools']),
        'mode': data.get('mode', 'ai_news'),  # ai_news, clips, studio
        'schedule_hour': int(data.get('schedule_hour', 9)),
        'max_videos': int(data.get('max_videos', 3)),
        'yt_token': data.get('yt_token', ''),
        'music_style': data.get('music_style', 'dramatic'),
        'voice': data.get('voice', 'ara'),
        'active': True,
        'last_run': '',
        'total_posted': 0,
        'created_at': datetime.now().isoformat()
    }
    CHANNELS[cid] = channel
    # Start scheduler for this channel
    start_channel_scheduler(cid)
    return jsonify(channel)

@app.route('/api/channels/<cid>', methods=['DELETE'])
def delete_channel(cid):
    if cid in CHANNELS:
        CHANNELS[cid]['active'] = False
        del CHANNELS[cid]
    return jsonify({'success': True})

@app.route('/api/channels/<cid>/toggle', methods=['POST'])
def toggle_channel(cid):
    if cid in CHANNELS:
        CHANNELS[cid]['active'] = not CHANNELS[cid].get('active', True)
        return jsonify(CHANNELS[cid])
    return jsonify({'error': 'Not found'}), 404

@app.route('/api/channels/<cid>/run', methods=['POST'])
def run_channel_now(cid):
    if cid not in CHANNELS:
        return jsonify({'error': 'Not found'}), 404
    channel = CHANNELS[cid]
    job_id = trigger_channel_job(cid, channel)
    return jsonify({'job_id': job_id})

def trigger_channel_job(cid, channel):
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        'id': job_id, 'status': 'queued',
        'created_at': datetime.now().isoformat(),
        'progress': 0, 'logs': [],
        'channel_id': cid,
        'channel_name': channel.get('name', '')
    }
    params = {
        'niche': channel.get('niche', ''),
        'categories': channel.get('categories', []),
        'max_videos': channel.get('max_videos', 3),
        'use_aurora': 'Yes',
        'music_enabled': 'Yes',
        'music_style': channel.get('music_style', 'dramatic'),
        'voice': channel.get('voice', 'ara'),
        'auto_upload': 'Yes' if channel.get('yt_token') else 'No',
        'yt_access_token': channel.get('yt_token', ''),
    }
    mode = channel.get('mode', 'ai_news')
    if mode == 'ai_news':
        t = threading.Thread(target=process_ai_news_studio, args=(job_id, params), daemon=True)
    elif mode == 'studio':
        params['topic'] = channel.get('niche', '')
        t = threading.Thread(target=process_ai_content_job, args=(job_id, params), daemon=True)
    else:
        t = threading.Thread(target=process_job, args=(job_id, params), daemon=True)
    t.start()
    CHANNELS[cid]['last_run'] = datetime.now().strftime('%Y-%m-%d')
    CHANNELS[cid]['total_posted'] = CHANNELS[cid].get('total_posted', 0) + params['max_videos']
    return job_id

def start_channel_scheduler(cid):
    def run():
        while CHANNELS.get(cid, {}).get('active'):
            try:
                channel = CHANNELS.get(cid)
                if not channel:
                    break
                now = datetime.now()
                if (now.hour == channel.get('schedule_hour', 9) and
                    now.minute == 0 and
                    channel.get('last_run','') != now.strftime('%Y-%m-%d')):
                    logger.info(f'Running channel: {channel["name"]}')
                    trigger_channel_job(cid, channel)
            except Exception as e:
                logger.error(f'Channel scheduler error: {e}')
            time.sleep(60)
    t = threading.Thread(target=run, daemon=True)
    t.start()


@app.route('/api/universal', methods=['POST'])
def create_universal():
    params = request.json
    job_id = str(uuid.uuid4())
    niche = params.get('niche', 'ai_tools')
    config = NICHE_CONFIGS.get(niche, NICHE_CONFIGS['ai_tools'])
    update_job(job_id, {
        'id': job_id, 'status': 'queued',
        'created_at': datetime.now().isoformat(),
        'progress': 0, 'logs': [],
        'type': 'universal',
        'topic': config['name']
    })
    thread = threading.Thread(target=process_universal_studio, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({'job_id': job_id})


@app.route('/api/niche-configs', methods=['GET'])
def get_niche_configs():
    return jsonify([{
        'key': k,
        'name': v['name'],
        'emoji': v['emoji'],
    } for k, v in NICHE_CONFIGS.items()])


@app.route('/api/conspiracy', methods=['POST'])
def create_conspiracy():
    params = request.json
    job_id = str(uuid.uuid4())
    update_job(job_id, {
        'id': job_id, 'status': 'queued',
        'created_at': datetime.now().isoformat(),
        'progress': 0, 'logs': [],
        'type': 'conspiracy',
        'topic': 'Conspiracy Studio'
    })
    thread = threading.Thread(target=process_conspiracy_studio, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({'job_id': job_id})


@app.route('/api/ai-news', methods=['POST'])
def create_ai_news():
    params = request.json
    job_id = str(uuid.uuid4())
    update_job(job_id, {
        'id': job_id, 'status': 'queued',
        'created_at': datetime.now().isoformat(),
        'progress': 0, 'logs': [],
        'type': 'ai_news',
        'topic': 'AI News Studio'
    })
    thread = threading.Thread(target=process_ai_news_studio, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({'job_id': job_id})


@app.route('/api/grok-video', methods=['POST'])
def create_grok_video():
    params = request.json
    job_id = str(uuid.uuid4())
    update_job(job_id, {
        'id': job_id, 'status': 'queued',
        'created_at': datetime.now().isoformat(),
        'progress': 0, 'logs': [],
        'type': 'grok_video',
        'topic': params.get('topic', '')
    })
    thread = threading.Thread(target=process_grok_original_video, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({'job_id': job_id})


# ============================================================
# AUTH ROUTES
# ============================================================

@app.route('/login')
def login_page():
    return render_template('login.html')

@app.route('/api/auth/register', methods=['POST'])
def register():
    data = request.json
    email = data.get('email','').lower().strip()
    password = data.get('password','')
    name = data.get('name','')
    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400
    if email in USERS:
        return jsonify({'error': 'Email already registered'}), 400
    USERS[email] = {
        'email': email,
        'name': name,
        'password': hash_password(password),
        'plan': 'free',
        'created_at': datetime.now().isoformat(),
        'channels': 0,
        'videos_generated': 0
    }
    token = generate_token()
    SESSIONS[token] = USERS[email]
    return jsonify({'token': token, 'user': USERS[email], 'redirect': '/'})

@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.json
    email = data.get('email','').lower().strip()
    password = data.get('password','')
    # Admin bypass
    if password == ADMIN_PASSWORD:
        token = ADMIN_TOKEN
        return jsonify({'token': token, 'user': {'email': 'admin', 'plan': 'admin', 'is_admin': True}, 'redirect': '/'})
    user = USERS.get(email)
    if not user or user['password'] != hash_password(password):
        return jsonify({'error': 'Invalid email or password'}), 401
    token = generate_token()
    SESSIONS[token] = user
    return jsonify({'token': token, 'user': user, 'redirect': '/'})

@app.route('/api/auth/me', methods=['GET'])
def get_me():
    user = get_current_user(request)
    if not user:
        return jsonify({'error': 'Not authenticated'}), 401
    return jsonify(user)

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    token = request.headers.get('X-Auth-Token') or request.cookies.get('auth_token')
    if token and token in SESSIONS:
        del SESSIONS[token]
    return jsonify({'success': True})

@app.route('/api/auth/plans', methods=['GET'])
def get_plans():
    return jsonify({
        'plans': [
            {
                'id': 'free',
                'name': 'Free',
                'price': 0,
                'features': ['3 videos/month', '1 channel', 'YouTube clips only'],
                'limits': {'videos': 3, 'channels': 1}
            },
            {
                'id': 'pro',
                'name': 'Pro',
                'price': 29,
                'features': ['100 videos/month', '3 channels', 'AI News Studio', 'Grok Originals', 'Priority processing'],
                'limits': {'videos': 100, 'channels': 3},
                'stripe_price_id': os.environ.get('STRIPE_PRO_PRICE_ID', '')
            },
            {
                'id': 'agency',
                'name': 'Agency',
                'price': 99,
                'features': ['Unlimited videos', 'Unlimited channels', 'All features', 'White label', 'API access'],
                'limits': {'videos': 999999, 'channels': 999},
                'stripe_price_id': os.environ.get('STRIPE_AGENCY_PRICE_ID', '')
            }
        ]
    })


@app.route('/api/stripe/checkout', methods=['POST'])
def stripe_checkout():
    import stripe
    stripe.api_key = os.environ.get('STRIPE_SECRET_KEY', '')
    if not stripe.api_key:
        return jsonify({'error': 'Stripe not configured'}), 500
    data = request.json
    plan = data.get('plan', 'pro')
    user = get_current_user(request)
    price_id = os.environ.get(f'STRIPE_{"PRO" if plan=="pro" else "AGENCY"}_PRICE_ID', '')
    if not price_id:
        return jsonify({'error': 'Price not configured'}), 500
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{'price': price_id, 'quantity': 1}],
            mode='subscription',
            success_url=f'{request.host_url}?upgraded=true',
            cancel_url=f'{request.host_url}pricing',
            customer_email=user.get('email') if user else None,
            metadata={'plan': plan, 'email': user.get('email','') if user else ''}
        )
        return jsonify({'url': session.url})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stripe/webhook', methods=['POST'])
def stripe_webhook():
    import stripe
    stripe.api_key = os.environ.get('STRIPE_SECRET_KEY', '')
    webhook_secret = os.environ.get('STRIPE_WEBHOOK_SECRET', '')
    payload = request.get_data()
    sig = request.headers.get('Stripe-Signature', '')
    try:
        event = stripe.Webhook.construct_event(payload, sig, webhook_secret)
        if event['type'] == 'checkout.session.completed':
            session = event['data']['object']
            email = session.get('customer_email') or session.get('metadata', {}).get('email', '')
            plan = session.get('metadata', {}).get('plan', 'pro')
            if email and email in USERS:
                USERS[email]['plan'] = plan
                logger.info(f'Upgraded {email} to {plan}')
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400


@app.route('/api/test-download', methods=['GET'])
def test_download():
    """Test RapidAPI download with a known working video."""
    import requests as req
    api_key = os.environ.get('RAPIDAPI_KEY', '')
    if not api_key:
        return jsonify({'error': 'No RAPIDAPI_KEY'})
    
    # Test with a simple short video
    vid_id = 'dQw4w9WgXcQ'  # Rick Astley - always works
    
    try:
        r = req.get(
            'https://youtube-media-downloader.p.rapidapi.com/v2/video/details',
            params={'videoId': vid_id},
            headers={
                'x-rapidapi-key': api_key,
                'x-rapidapi-host': 'youtube-media-downloader.p.rapidapi.com'
            },
            timeout=15
        )
        data = r.json()
        
        # Extract video URLs
        videos = data.get('videos', {}).get('items', [])
        result = {
            'status': r.status_code,
            'errorId': data.get('errorId'),
            'video_count': len(videos),
            'first_video': videos[0] if videos else None,
            'keys': list(data.keys())
        }
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/grok-models', methods=['GET'])
def list_grok_models():
    import requests as req
    api_key = os.environ.get('GROK_API_KEY', '').strip()
    try:
        r = req.get(
            'https://api.x.ai/v1/models',
            headers={'Authorization': f'Bearer {api_key}'},
            timeout=10
        )
        return jsonify(r.json())
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/test-research', methods=['GET'])
def test_research():
    topic = request.args.get('topic', 'Colonial America farming laws')
    gemini_key = os.environ.get('GEMINI_API_KEY', '') or os.environ.get('CLAUDE_API_KEY', '')
    brave_key = os.environ.get('BRAVE_API_KEY', '')
    
    results = {
        'gemini_key': bool(gemini_key),
        'brave_key': bool(brave_key),
        'topic': topic
    }
    
    # Test DuckDuckGo (no key needed)
    import requests as req
    try:
        r = req.get('https://api.duckduckgo.com/',
            params={'q': topic, 'format': 'json', 'no_html': 1},
            timeout=10)
        data = r.json()
        results['ddg_abstract'] = data.get('AbstractText', '')[:100]
        results['ddg_ok'] = bool(data.get('AbstractText'))
    except Exception as e:
        results['ddg_error'] = str(e)
    
    # Test Gemini grounded
    if gemini_key:
        text = search_gemini_grounded(topic, gemini_key)
        results['gemini_result'] = text[:100] if text else None
        results['gemini_ok'] = bool(text)
    
    # Test Brave
    if brave_key:
        snippets = search_brave(topic, brave_key)
        results['brave_count'] = len(snippets)
    
    return jsonify(results)


@app.route('/api/research-topic', methods=['POST'])
def research_and_generate():
    """Research any topic and generate a viral script.
    Flow: Claude search → Gemini script → fallback
    """
    data = request.json or {}
    topic = data.get('topic', '')
    style = data.get('style', 'conspiracy')
    if not topic:
        return jsonify({'error': 'Topic required'})

    anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '').strip()
    gemini_key = (os.environ.get('GEMINI_API_KEY', '') or 
                  os.environ.get('CLAUDE_API_KEY', '')).strip()
    # Only use gemini key if it starts with AIza (not anthropic key)
    if gemini_key and not gemini_key.startswith('AIza'):
        gemini_key = ''

    debug = {
        'topic': topic,
        'anthropic_key': bool(anthropic_key),
        'gemini_key': bool(gemini_key),
    }

    # ── Step 1: Research using Claude web search ──────────────
    research_content = ''

    if anthropic_key:
        logger.info(f'Researching with Claude: {topic}')
        claude_result = research_with_claude_search(topic, anthropic_key)
        if claude_result:
            research_content = claude_result
            debug['research_source'] = 'claude_web'
            debug['research_chars'] = len(research_content)
            logger.info(f'Claude research: {len(research_content)} chars')

    # ── Step 2: Fallback research - DuckDuckGo + Wikipedia ────
    if not research_content:
        logger.info(f'Claude unavailable - using free web research')
        web_result = research_topic_web(topic)
        if web_result and web_result.get('content'):
            research_content = web_result['content']
            debug['research_source'] = 'ddg_wikipedia'
            debug['research_chars'] = len(research_content)

    # Still nothing - use topic alone
    if not research_content:
        research_content = f'Topic: {topic}'
        debug['research_source'] = 'topic_only'

    research = {'content': research_content, 'sources': [], 'topic': topic}

    # ── Step 3: Generate script using Gemini (free) ───────────
    result = None

    if gemini_key:
        logger.info(f'Generating script with Gemini')
        result = generate_script_from_research(topic, research, gemini_key, style)
        if result:
            debug['script_ai'] = 'gemini'

    # Fallback to Groq free (groq.com)
    if not result:
        groq_free_key = os.environ.get('GROQ_API_KEY', '').strip()
        if groq_free_key:
            result = generate_script_from_research(topic, research, groq_free_key, style)
            if result:
                debug['script_ai'] = 'groq_free'

    # Last resort - basic script without AI
    if not result:
        result = {
            'title': topic[:55].upper(),
            'hook': f'Nobody talks about {topic}',
            'script': f'Here is what you need to know about {topic}. This information has been hidden from you.',
            'key_facts': [f'Fact about {topic}', 'This changes everything', 'Share this truth'],
            'search_query': topic + ' documentary',
            'text_overlays': [topic.upper()[:45], 'THE TRUTH', 'NOBODY TALKS ABOUT THIS'],
            'hashtags': ['#conspiracy', '#history', '#didyouknow', '#shorts']
        }
        debug['script_ai'] = 'fallback'

    logger.info(f'Research complete: {debug}')
    result['debug'] = debug
    return jsonify({'success': True, 'item': result})


@app.route('/api/save-character', methods=['POST'])
def save_character():
    """Save mystery character image for conspiracy content."""
    import requests as req
    data = request.json or {}
    image_data = data.get('image_data', '')
    if not image_data:
        return jsonify({'error': 'No image data'})
    try:
        import base64
        if ',' in image_data:
            image_data = image_data.split(',')[1]
        img_bytes = base64.b64decode(image_data)
        char_path = '/tmp/xlab_character.jpg'
        with open(char_path, 'wb') as f:
            f.write(img_bytes)
        return jsonify({'success': True, 'path': char_path,
                       'size': os.path.getsize(char_path)})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/generate-avatar', methods=['POST'])
def generate_avatar():
    """Generate a channel avatar image using Grok Aurora."""
    import requests as req
    data = request.json or {}
    prompt = data.get('prompt', 
        'Professional AI news anchor, realistic, dark background, '
        'futuristic aesthetic, looking directly at camera, high quality portrait')
    api_key = os.environ.get('GROK_API_KEY', '').strip()
    if not api_key:
        return jsonify({'error': 'No GROK_API_KEY'})
    url = aurora_generate_image(prompt, api_key)
    if url:
        # Save locally
        avatar_path = '/tmp/xlab_avatar.jpg'
        try:
            r = req.get(url, timeout=15)
            with open(avatar_path, 'wb') as f:
                f.write(r.content)
        except: pass
        return jsonify({'url': url, 'saved': os.path.exists(avatar_path)})
    return jsonify({'error': 'Generation failed'})


@app.route('/api/test-aurora', methods=['GET'])
def test_aurora():
    import requests as req
    api_key = os.environ.get('GROK_API_KEY', '').strip()
    if not api_key:
        return jsonify({'error': 'No GROK_API_KEY'})
    try:
        r = req.post(
            'https://api.x.ai/v1/videos/generations',
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
            json={'model': 'grok-imagine-video', 'prompt': 'A glowing blue AI interface', 'duration': 6},
            timeout=30
        )
        return jsonify({'status': r.status_code, 'response': r.json()})
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/api/test-grok', methods=['GET'])
def test_grok():
    import requests as req
    api_key = os.environ.get('GROK_API_KEY', '')
    if not api_key:
        return jsonify({'error': 'No GROK_API_KEY set'})
    
    results = {}
    for model in ['grok-3', 'grok-2-1212', 'grok-beta', 'grok-4']:
        try:
            r = req.post(
                'https://api.x.ai/v1/chat/completions',
                headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                json={'model': model, 'messages': [{'role': 'user', 'content': 'Say OK'}], 'max_tokens': 10},
                timeout=15
            )
            data = r.json()
            if 'choices' in data:
                results[model] = '✅ ' + data['choices'][0]['message']['content']
            else:
                results[model] = '❌ ' + str(data)[:100]
        except Exception as e:
            results[model] = f'❌ {e}'
    
    return jsonify(results)


@app.route('/api/keys/status', methods=['GET'])
def keys_status():
    return jsonify({
        'grok': bool(os.environ.get('GROK_API_KEY')),
        'rapidapi': bool(os.environ.get('RAPIDAPI_KEY')),
        'proxy': bool(os.environ.get('PROXY_URL')),
        'x': bool(os.environ.get('X_API_KEY')),
        'stripe': bool(os.environ.get('STRIPE_SECRET_KEY')),
    })


@app.route('/api/trending', methods=['POST'])
def get_trending():
    data = request.json
    niche = data.get('niche', 'football')
    grok_key = os.environ.get('GROK_API_KEY', '')
    if not grok_key:
        return jsonify({'topics': [], 'error': 'No Grok API key'})
    try:
        topics = grok_trending_topics(niche, grok_key)
        if topics:
            return jsonify(topics)
        return jsonify({'topics': []})
    except Exception as e:
        return jsonify({'topics': [], 'error': str(e)})


@app.route('/api/search', methods=['POST'])
def search():
    data = request.json
    try:
        videos = fetch_video_info(
            mode=data.get('mode', 'search'),
            search_query=data.get('search_query', ''),
            youtube_url=data.get('youtube_url', ''),
            other_urls=data.get('other_urls', []),
            date_filter=data.get('date_filter', 'Any Time'),
            max_videos=int(data.get('max_videos', 5)),
            trending_topic=data.get('trending_topic', 'football')
        )
        return jsonify({'videos': videos})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/jobs', methods=['POST'])
def create_job():
    params  = request.json
    job_id  = str(uuid.uuid4())
    pending_uploads = params.pop('pending_uploads', False)
    update_job(job_id, {
        'id': job_id, 'status': 'queued',
        'created_at': datetime.now().isoformat(),
        'progress': 0, 'logs': [],
        'uploads_ready': not pending_uploads
    })
    thread = threading.Thread(target=process_job, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({'job_id': job_id})

@app.route('/api/jobs/<job_id>/start', methods=['POST'])
def start_job(job_id):
    """Signal that browser uploads are done and processing can begin."""
    job = get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    update_job(job_id, {'uploads_ready': True})
    return jsonify({'success': True})

@app.route('/api/jobs/<job_id>', methods=['GET'])
def job_status(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    safe = {k: v for k, v in job.items() if k != 'zip_path'}
    return jsonify(safe)

@app.route('/api/jobs/<job_id>/logs', methods=['GET'])
def job_logs(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({'logs': [], 'status': 'unknown', 'progress': 0})
    return jsonify({'logs': job.get('logs', []), 'status': job.get('status',''), 'progress': job.get('progress', 0)})

@app.route('/api/jobs/<job_id>/download', methods=['GET'])
def download_zip(job_id):
    job = get_job(job_id)
    if not job or job.get('status') != 'done':
        return jsonify({'error': 'Not ready'}), 404
    zip_path = job.get('zip_path')
    if not zip_path or not os.path.exists(zip_path):
        return jsonify({'error': 'File not found'}), 404
    return send_file(zip_path, as_attachment=True,
                     download_name=f'xlab_shorts_{job_id[:8]}.zip')

@app.route('/api/cobalt', methods=['POST'])
def cobalt_proxy():
    """Proxy cobalt.tools request to avoid CORS issues in browser."""
    import requests as req
    data = request.json
    url = data.get('url')
    
    cobalt_instances = [
        'https://api.cobalt.tools',
        'https://cobalt.api.timelessnesses.me',
        'https://cobalt.canine.tools',
    ]
    
    for instance in cobalt_instances:
        try:
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            payload = {
                'url': url,
                'videoQuality': '720',
                'filenameStyle': 'basic',
                'downloadMode': 'auto',
            }
            r = req.post(f'{instance}/', json=payload, headers=headers, timeout=30)
            if r.status_code == 200:
                result = r.json()
                if result.get('status') in ['stream', 'redirect', 'tunnel', 'picker']:
                    return jsonify(result)
        except Exception as e:
            logger.error(f'Cobalt proxy error {instance}: {e}')
            continue
    
    return jsonify({'error': 'All cobalt instances failed'}), 500

@app.route('/api/ai-content', methods=['POST'])
def create_ai_content():
    params = request.json
    job_id = str(uuid.uuid4())
    update_job(job_id, {
        'id': job_id, 'status': 'queued',
        'created_at': datetime.now().isoformat(),
        'progress': 0, 'logs': [],
        'type': 'ai_content',
        'topic': params.get('topic', '')
    })
    thread = threading.Thread(target=process_ai_content_job, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({'job_id': job_id})

@app.route('/api/schedules', methods=['GET'])
def get_schedules():
    return jsonify(list(SCHEDULES.values()))

@app.route('/api/schedules', methods=['POST'])
def create_schedule():
    data = request.json
    sid = str(uuid.uuid4())
    schedule = {
        'id': sid,
        'name': data.get('name', 'My Schedule'),
        'query': data.get('query', ''),
        'mode': data.get('mode', 'search'),
        'hour': int(data.get('hour', 9)),
        'minute': int(data.get('minute', 0)),
        'timezone': data.get('timezone', 'Europe/Paris'),
        'max_videos': int(data.get('max_videos', 3)),
        'clip_length': int(data.get('clip_length', 45)),
        'clips_per_video': int(data.get('clips_per_video', 3)),
        'captions': data.get('captions', 'No'),
        'watermark_enabled': data.get('watermark_enabled', 'No'),
        'watermark_text': data.get('watermark_text', ''),
        'ai_metadata': data.get('ai_metadata', 'No'),
        'auto_upload': data.get('auto_upload', 'No'),
        'yt_access_token': data.get('yt_access_token', ''),
        'active': True,
        'created_at': datetime.now().isoformat(),
        'last_run': '',
        'job_history': []
    }
    SCHEDULES[sid] = schedule
    return jsonify(schedule)

@app.route('/api/schedules/<sid>', methods=['DELETE'])
def delete_schedule(sid):
    if sid in SCHEDULES:
        del SCHEDULES[sid]
        return jsonify({'success': True})
    return jsonify({'error': 'Not found'}), 404

@app.route('/api/schedules/<sid>/toggle', methods=['POST'])
def toggle_schedule(sid):
    if sid in SCHEDULES:
        SCHEDULES[sid]['active'] = not SCHEDULES[sid].get('active', True)
        return jsonify(SCHEDULES[sid])
    return jsonify({'error': 'Not found'}), 404

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'jobs': len(JOBS)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
