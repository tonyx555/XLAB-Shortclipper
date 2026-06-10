"""
XLAB ShortClipper — Railway Backend
Flask app with in-memory job tracking, background processing,
direct ZIP download. No Firebase or GCS needed.
"""

import os, json, shutil, glob, zipfile, subprocess, sys
import threading, uuid, logging, warnings
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file, render_template

import numpy as np

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ============================================================
# Config — set these as Railway environment variables
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
                    # Use AI content pipeline if mode is ai_content
                    if schedule.get('mode') == 'ai_content':
                        ai_params = {**params, 'topic': schedule.get('query', '')}
                        t2 = threading.Thread(target=process_ai_content_job, args=(job_id, ai_params), daemon=True)
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
    subprocess.run(['ffmpeg','-i',video_path,'-vn','-ar','16000','-ac','1',
        '-y', audio_path, '-loglevel','quiet'], check=True)

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

    if watermark_text.strip():
        pos_map = {
            'top_left':    'x=20:y=20',
            'top_right':   'x=w-tw-20:y=20',
            'bottom_left': 'x=20:y=h-th-20',
            'bottom_right':'x=w-tw-20:y=h-th-20'
        }
        pos  = pos_map.get(watermark_position, 'x=w-tw-20:y=h-th-20')
        safe = watermark_text.replace("'", "\\'").replace(":", "\\:")
        wm   = f"drawtext=text='{safe}':fontsize=32:fontcolor=white:alpha=0.7:{pos}:box=1:boxcolor=black@0.3:boxborderw=6"
        vf   = f'{vf_base},{wm}'
    else:
        vf = vf_base

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
# LEVEL 3 — AI Content Creation
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
        r = req.post('https://api.anthropic.com/v1/messages',
            headers={{'x-api-key': api_key, 'anthropic-version': '2023-06-01', 'content-type': 'application/json'}},
            json={{'model': 'claude-sonnet-4-20250514', 'max_tokens': 2000,
                  'messages': [{{'role': 'user', 'content': prompt}}]}},
            timeout=30)
        text = r.json()['content'][0]['text'].replace('```json','').replace('```','').strip()
        return json.loads(text)
    except Exception as e:
        logger.error(f'Script generation error: {e}')
        return None


def text_to_speech(text, output_path):
    """Convert text to speech using gTTS (free, no API key needed)."""
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
    """Concatenate multiple clips into one video."""
    try:
        if len(clip_paths) == 1:
            import shutil
            shutil.copy2(clip_paths[0], output_path)
            return True

        # Create concat file
        concat_file = output_path.replace('.mp4', '_concat.txt')
        with open(concat_file, 'w') as f:
            for cp in clip_paths:
                f.write(f"file '{cp}'\n")

        cmd = [
            'ffmpeg', '-f', 'concat', '-safe', '0',
            '-i', concat_file,
            '-c', 'copy', '-y', output_path, '-loglevel', 'quiet'
        ]
        subprocess.run(cmd, check=True, timeout=300)
        if os.path.exists(concat_file):
            os.remove(concat_file)
        return os.path.exists(output_path)
    except Exception as e:
        logger.error(f'Concat error: {e}')
        return False


def process_ai_content_job(job_id, params):
    """Level 3 — Full AI video assembly from a topic prompt."""
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

        # Step 1 — Generate script
        script = generate_ai_script(topic, num_points, clip_duration, claude_key)
        if not script:
            raise Exception('Failed to generate script')

        add_log(job_id, f'📝 Script ready: "{script["title"]}"')
        add_log(job_id, f'   Hook: {script["hook"][:80]}...')
        add_log(job_id, f'   {len(script["points"])} points to find footage for')
        update_job(job_id, {{'progress': 10, 'script': script}})

        # Step 2 — Find and download clips for each point
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
                add_log(job_id, f'   ⚠️ No footage found — skipping point')
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
            raise Exception('No clips assembled — all points failed')

        add_log(job_id, f'\n🎬 Assembling {len(assembled_clips)} clips...')
        update_job(job_id, {{'progress': 70}})

        # Step 3 — Concatenate all clips
        final_path = f'{out_dir}/{job_id}_ai_video.mp4'
        if not concatenate_clips(assembled_clips, final_path):
            raise Exception('Failed to concatenate clips')

        # Step 4 — Add music if enabled
        if music_enabled:
            add_log(job_id, f'🎵 Adding {music_style} music...')
            music_out = final_path.replace('.mp4', '_music.mp4')
            if add_trending_music(final_path, music_out, music_style, 0.25):
                os.replace(music_out, final_path)

        # Step 5 — Auto upload to YouTube
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

        # Wait for browser uploads if needed
        max_wait = 300
        waited = 0
        while not get_job(job_id).get('uploads_ready', True) and waited < max_wait:
            time.sleep(2)
            waited += 2
            if waited % 10 == 0:
                add_log(job_id, f'⏳ Waiting for browser uploads... ({waited}s)')

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
            add_log(job_id, f'   ⚠️ No proxy set — add PROXY_URL env variable')

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

            # Method 1: yt-dlp with residential proxy (most reliable)
            if not downloaded_path and proxy:
                add_log(job_id, f'   🔄 Downloading via proxy...')
                out_tmpl = f'{raw_dir}/{v["id"]}.%(ext)s'
                cmd = [
                    'yt-dlp',
                    '--format', 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/bestvideo[height<=1080]+bestaudio/best',
                    '--merge-output-format', 'mp4',
                    '--output', out_tmpl,
                    '--no-playlist', '--no-warnings',
                    '--proxy', proxy,
                    '--socket-timeout', '30',
                    '--retries', '3',
                    '--fragment-retries', '3',
                    '--extractor-args', 'youtube:player_client=web,web_creator',
                    '--add-header', 'User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
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
                    add_log(job_id, f'   ❌ All 5 download methods failed — skipping')

        downloaded = glob.glob(f'{raw_dir}/*.mp4')
        add_log(job_id, f'✅ Downloaded {len(downloaded)} file(s)')
        if not downloaded:
            raise Exception('No videos downloaded — check PROXY_URL environment variable')

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
                    add_log(job_id, f'      ✅ {clip_name} ({size:.1f}MB) — {meta["score"]}/10')

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
