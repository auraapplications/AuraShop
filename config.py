import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin123')
    DATABASE = os.path.join(os.path.dirname(__file__), 'shop.db')
    UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max upload
    ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
    SESSION_TYPE = 'filesystem'
    SESSION_PERMANENT = False
    DISCLOUD_API_URL = 'https://api.discloud.app/v2'
