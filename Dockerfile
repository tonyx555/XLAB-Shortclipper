FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
   ffmpeg wget curl \
   && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --upgrade yt-dlp

COPY . .
RUN mkdir -p /tmp/uploads /tmp/jobs

EXPOSE 8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "3600", "app:app"]
