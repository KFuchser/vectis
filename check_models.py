# check_models.py
import os
import google.generativeai as genai


# 1. Manually paste your key here to rule out any "secrets.toml" issues
# (Delete this line after the test)
MY_KEY = "AIzaSyCftQ5LW6xs1KxaYLJHcp7mQaJqy4pADdM" # <--- PASTE YOUR KEY INSIDE THESE QUOTES

genai.configure(api_key=MY_KEY)

print(f"Checking models for key ending in ...{MY_KEY[-4:]}")

try:
    print("--- AVAILABLE MODELS ---")
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(f"✅ {m.name}")
except Exception as e:
    print(f"❌ Error: {e}")