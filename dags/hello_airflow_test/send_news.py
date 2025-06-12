from google.cloud import pubsub_v1
import json
import time
import random

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("talabat-labs-3927", "news-headlines")

sample_news = [
    {"title": "Tech Stocks Rise", "source": "CNN", "published_at": "2025-06-12T09:00:00Z"},
    {"title": "Flood Hits Midwest", "source": "BBC", "published_at": "2025-06-12T10:00:00Z"},
    {"title": "AI Reshaping Jobs", "source": "Reuters", "published_at": "2025-06-12T11:00:00Z"}
]

while True:
    news = random.choice(sample_news)
    publisher.publish(topic_path, json.dumps(news).encode("utf-8"))
    print(f"Published: {news['title']}")
    time.sleep(10)
