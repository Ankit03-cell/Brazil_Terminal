import urllib.request
import json

try:
    url = "http://127.0.0.1:8000/api/fra/spreads/history?tenor=12&f_only=false"
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode('utf-8'))
    print(f"Success! Got {len(data)} generic series.")
    if data:
        print(f"Generic 1 has {len(data[0]['x'])} points.")
except Exception as e:
    print("Error:", e)
