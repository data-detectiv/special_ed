import os
import streamlit as st

# Backend URL configuration
# You can change this to point to your local backend or deployed backend
BACKEND_URL = os.getenv('BACKEND_URL', 'http://localhost:8000')

# For local development, use: http://localhost:8000
# For deployed backend, use: https://special-ed.onrender.com

def get_backend_url():
    """Get the backend URL from environment or use default"""
    return BACKEND_URL 