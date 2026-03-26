from dotenv import load_dotenv
import os
from groq import Groq

load_dotenv()

key = os.getenv("GROQ_API_KEY")
print(f"Using key: {key[:8]}...{key[-4:]}")
print(f"Full key length: {len(key)}")

client = Groq(api_key=key)

response = client.chat.completions.create(
    model="llama-3.3-70b-versatile",
    messages=[{"role": "user", "content": "Say hello"}],
    max_tokens=10
)

print(f"Response: {response.choices[0].message.content}")