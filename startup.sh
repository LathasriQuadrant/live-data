#!/bin/bash
# Azure Web App startup command
# Set this as the startup command in Azure Portal:
#   gunicorn -w 1 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:8000 --timeout 600

cd /home/site/wwwroot
gunicorn -w 1 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:8000 --timeout 600