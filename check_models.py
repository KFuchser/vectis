"""
A standalone diagnostic script to verify access to Google Generative AI models.
Lists all available models that support the 'generateContent' method.

Usage:
    GOOGLE_API_KEY=your_key python check_models.py
  or with a .env file:
    python check_models.py
"""
import os
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("GOOGLE_API_KEY")

if not api_key:
    print("❌ ERROR: GOOGLE_API_KEY not found in environment or .env file.")
    print("   Set it with: export GOOGLE_API_KEY=your_key")
    exit(1)

genai.configure(api_key=api_key)

print(f"Checking models for key ending in ...{api_key[-4:]}")

try:
    print("--- AVAILABLE MODELS ---")
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(f"✅ {m.name}")
except Exception as e:
    print(f"❌ Error: {e}")
