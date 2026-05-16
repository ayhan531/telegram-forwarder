web: gunicorn -w 1 -k uvicorn.workers.UvicornWorker app:app --bind 0.0.0.0:${PORT:-10000} --timeout 120 --keep-alive 5
