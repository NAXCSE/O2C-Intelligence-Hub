import json
import os

base_path = "."  # change this to your dataset folder path

for root, dirs, files in os.walk(base_path):
    for file in files:
        if file.endswith(".jsonl"):
            filepath = os.path.join(root, file)
            print(f"\n📁 {filepath}")
            
            # Get keys from first record
            with open(filepath, "r") as f:
                first_line = f.readline()
                if first_line:
                    record = json.loads(first_line)
                    print(f"   Keys: {list(record.keys())}")
                    print(f"   Sample: {json.dumps(record, indent=2)[:300]}")