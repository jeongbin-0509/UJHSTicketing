import os

bind = os.getenv("BIND", "0.0.0.0:5000")
workers = int(os.getenv("WEB_CONCURRENCY", "4"))
worker_class = "gthread"
threads = int(os.getenv("GUNICORN_THREADS", "16"))
backlog = int(os.getenv("GUNICORN_BACKLOG", "4096"))
timeout = int(os.getenv("GUNICORN_TIMEOUT", "120"))
graceful_timeout = 30
keepalive = 5
accesslog = None
errorlog = "-"
