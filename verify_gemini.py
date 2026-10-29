"""
A simple diagnostic script to verify that the Google Gemini API is accessible and responsive.
It sends a test prompt to the configured model (GEMINI_MODEL, see gemini_config.py) using the
google-genai SDK and prints the result, helping to confirm that the model id, API key,
billing and network connection are all valid.
"""
import os
import sys
from dotenv import load_dotenv
import google.genai as genai

from gemini_config import GEMINI_MODEL

load_dotenv()

# 1. Configure
api_key = os.environ.get("GOOGLE_API_KEY")

if not api_key:
    print("❌ ERROR: GOOGLE_API_KEY not found in environment.")
    sys.exit(1)

print(f"🔑 Key found: {api_key[:5]}... checking status with model '{GEMINI_MODEL}'...")

try:
    client = genai.Client(api_key=api_key)

    # 2. The Test Payload
    test_prompt = "Classify this construction project: 'New ground-up Starbucks coffee shop'. Return ONLY one word: COMMERCIAL or RESIDENTIAL."

    print("📡 Pinging Google Gemini API...")
    response = client.models.generate_content(model=GEMINI_MODEL, contents=test_prompt)

    # 3. Validation
    if response.text:
        print(f"✅ SUCCESS! API is Active.")
        print(f"   Input: 'New ground-up Starbucks...'")
        print(f"   Output: {response.text.strip()}")
        print("🚀 You are GO for AI Reactivation.")
    else:
        print("⚠️ WARNING: Empty response received.")
        sys.exit(1)

except Exception as e:
    print(f"❌ CRITICAL FAILURE: API Refused Connection.")
    print(f"   Error: {e}")
    print("⛔ STOP: Do not unpause AI. Check GEMINI_MODEL (404 = retired), the API key (401/403) and billing (402).")
    sys.exit(1)
