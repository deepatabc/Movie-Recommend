#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
uvicorn app:app --uds=/tmp/uvicorn.sock