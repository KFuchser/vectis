"""
A simple diagnostic script to verify that the Google Gemini API is accessible and responsive.
It sends a test prompt to the 'gemini-1.5-flash' model and prints the result,
helping to confirm that the API key and network connection are valid.
"""
import os
import google.generativeai as genai

# 1. Configure
api_key = os.environ.get("GOOGLE_API_KEY")

if not api_key:
    print("❌ ERROR: GOOGLE_API_KEY not found in environment.")
    exit()

print(f"🔑 Key found: {api_key[:5]}... checking status...")

try:
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash')

    # 2. The Test Payload
    test_prompt = "Classify this construction project: 'New ground-up Starbucks coffee shop'. Return ONLY one word: COMMERCIAL or RESIDENTIAL."

    print("📡 Pinging Google Gemini API...")
    response = model.generate_content(test_prompt)

    # 3. Validation
    if response.text:
        print(f"✅ SUCCESS! API is Active.")
        print(f"   Input: 'New ground-up Starbucks...'")
        print(f"   Output: {response.text.strip()}")
        print("🚀 You are GO for AI Reactivation.")
    else:
        print("⚠️ WARNING: Empty response received.")

except Exception as e:
    print(f"❌ CRITICAL FAILURE: API Refused Connection.")
    print(f"   Error: {e}")
    print("⛔ STOP: Do not unpause AI. The hold is likely still active.")