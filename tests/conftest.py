import os

# Set before any test module imports the app: create_app() reads settings at import time.
SECRET = "test-secret-that-is-at-least-32-characters"
os.environ["SERVICE_TOKEN_SECRET"] = SECRET
os.environ["STORAGE_MODE"] = "local"
os.environ["LOG_JSON"] = "false"
