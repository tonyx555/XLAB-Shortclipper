FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN yt-dlp -U

RUN python -c "import whisper; whisper.load_model('base')"

COPY app.py .
COPY templates/ templates/

ENV PORT=8080
CMD ["python", "app.py"]
