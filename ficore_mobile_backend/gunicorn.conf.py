# Gunicorn configuration for FiCore Backend
# Optimized for SSE streams and long-running connections

import os

# Server socket
bind = f"0.0.0.0:{os.environ.get('PORT', '5000')}"
backlog = 2048

# Worker processes
workers = int(os.environ.get('WEB_CONCURRENCY', '2'))
worker_class = 'sync'  # Use sync workers (most stable)
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50

# Timeouts - CRITICAL for SSE streams
timeout = 300  # 5 minutes for SSE connections
keepalive = 30  # Keep connections alive
graceful_timeout = 120  # Graceful shutdown time

# Memory management
preload_app = True
max_requests = 1000  # Restart workers after 1000 requests to prevent memory leaks

# Logging
accesslog = '-'
errorlog = '-'
loglevel = 'info'
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = 'ficore-backend'

# Security
limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190

# SSL (if needed)
# keyfile = '/path/to/keyfile'
# certfile = '/path/to/certfile'

def when_ready(server):
    server.log.info("FiCore Backend server is ready. Listening on %s", server.address)

def worker_int(worker):
    worker.log.info("Worker received INT or QUIT signal")

def pre_fork(server, worker):
    server.log.info("Worker spawned (pid: %s)", worker.pid)

def post_fork(server, worker):
    server.log.info("Worker spawned (pid: %s)", worker.pid)