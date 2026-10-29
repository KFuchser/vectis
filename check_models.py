"""
A standalone diagnostic script to verify access to Google Generative AI models.
Lists all available models that support the 'generateContent' method, and flags whether
the configured GEMINI_MODEL (see gemini_config.py) is among them.

Usage:
    GOOGLE_API_KEY=your_key python check_models.py
  or with a .env file:
    python check_models.py
"""
import os
import sys
import google.genai as genai
from dotenv import load_dotenv

load_dotenv()

from gemini_config import GEMINI_MODEL

api_key = os.getenv("GOOGLE_API_KEY")

if not api_key:
    print("❌ ERROR: GOOGLE_API_KEY not found in environment or .env file.")
    print("   Set it with: export GOOGLE_API_KEY=your_key")
    sys.exit(1)

client = genai.Client(api_key=api_key)

print(f"Checking models for key ending in ...{api_key[-4:]}")

try:
    print("--- AVAILABLE MODELS ---")
    names = []
    for m in client.models.list():
        if "generateContent" in (m.supported_actions or []):
            names.append(m.name)
            print(f"✅ {m.name}")
    if f"models/{GEMINI_MODEL}" in names:
        print(f"\n✅ Configured GEMINI_MODEL '{GEMINI_MODEL}' is available.")
    else:
        print(f"\n❌ Configured GEMINI_MODEL '{GEMINI_MODEL}' is NOT in the list above.")
        sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
