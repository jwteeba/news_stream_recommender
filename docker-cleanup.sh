#!/bin/bash

echo "🧹 Starting Docker cleanup..."

echo "⏹️  Stopping all containers..."
docker stop $(docker ps -aq) 2>/dev/null || true

echo "🗑️  Removing all containers..."
docker rm $(docker ps -aq) 2>/dev/null || true

echo "🖼️  Removing all images..."
docker rmi $(docker images -q) 2>/dev/null || true

echo "🧽 Running system prune..."
docker system prune -af

echo "✅ Docker cleanup complete!"