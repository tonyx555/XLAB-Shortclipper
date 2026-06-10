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
                   '--flat-playlist','--match-filter','duration >= 60 & duration <= 7200',
                   '--no-check-certificates'] + (['--proxy', proxy] if proxy else [])
            if days:
                cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
                cmd += ['--dateafter', cutoff]
            cmd.append(f'ytsearch{max_videos}:{q}')
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
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
        wm_pos           = params.get('watermark_position', 'bottom_right')
        ai_enabled       = params.get('ai_metadata') == 'Yes'
        claude_key       = params.get('claude_api_key', '') or CLAUDE_API_KEY
        topic            = params.get('topic', 'sports')
        auto_upload = params.get('auto_upload') == 'Yes'
        yt_token    = params.get('yt_access_token', '')

        add_log(job_id, f'📥 Downloading {len(selected_videos)} video(s)...')

        for v in selected_videos:
            add_log(job_id, f'   ⬇️  {v["title"][:50]}...')
            proxy = os.environ.get('PROXY_URL', '')
            cmd = ['yt-dlp',
                   '--format', 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                   '--merge-output-format', 'mp4',
                   '--output', f'{raw_dir}/%(id)s_%(title).40s.%(ext)s',
                   '--no-playlist', '--no-warnings',
                   '--no-check-certificates',
                   '--extractor-retries', '3',
                   ] + (['--proxy', proxy] if proxy else [])
            if v.get('platform') == 'instagram':
                cmd += ['--add-header', 'User-Agent:Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)']
            elif v.get('platform') == 'tiktok':
                cmd += ['--add-header', 'User-Agent:TikTok 26.2.0 rv:262018 (iPhone; iOS 14.4.2; en_US) Cronet']
            cmd.append(v['url'])
            subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        downloaded = glob.glob(f'{raw_dir}/*.mp4')
        add_log(job_id, f'✅ Downloaded {len(downloaded)} file(s)')
        if not downloaded:
            raise Exception('No videos downloaded')

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
    update_job(job_id, {
        'id': job_id, 'status': 'queued',
        'created_at': datetime.now().isoformat(),
        'progress': 0, 'logs': []
    })
    thread = threading.Thread(target=process_job, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({'job_id': job_id})

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

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'jobs': len(JOBS)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
