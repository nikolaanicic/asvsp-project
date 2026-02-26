#!/bin/bash
set -e

echo "========================================="
echo "Hue Startup Script"
echo "========================================="

# Create all necessary directories with proper permissions
mkdir -p /tmp/hue
mkdir -p /usr/share/hue/desktop
mkdir -p /usr/share/hue/logs
chmod 777 /tmp/hue
chmod 777 /usr/share/hue/desktop
chmod 777 /usr/share/hue/logs

# Set environment variables
export TMPDIR=/tmp/hue
export PYTHONUNBUFFERED=1
export HOME=/tmp/hue

# Wait for namenode (optional, continue if not available)
echo "Checking namenode connection..."
if nc -z namenode 9870 2>/dev/null; then
    echo "Namenode is available!"
else
    echo "WARNING: Namenode not available, but continuing..."
fi

cd /usr/share/hue

# Initialize database if needed
if [ ! -f /usr/share/hue/desktop/desktop.db ]; then
    echo "Initializing Hue database..."
    ./build/env/bin/hue migrate
fi

# Create a custom gunicorn config to use temp directory
cat > /tmp/hue/gunicorn.conf.py << EOF
import tempfile
tempfile.tempdir = '/tmp/hue'
bind = '0.0.0.0:8888'
workers = 1
worker_class = 'sync'
EOF

echo "Starting Hue server on port 8888..."
exec ./build/env/bin/hue runserver 0.0.0.0:8888