#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
uvicorn src.app:app --uds=/tmp/uvicorn.sock