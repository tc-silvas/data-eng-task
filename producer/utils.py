import random
from datetime import datetime, timedelta

# Predefine constants to avoid re-creating lists repeatedly
DEVICES = ["iPhone", "Samsung Galaxy", "PC"]
OS = ["Linux", "Windows", "MacOS"]
COUNTRIES = ["GB", "PT", "BR", "US", "FR", "IT", "ES", "DE"]

def generate_random_timestamp():
    """
    Generate a random UNIX timestamp within the last 30 days.
    This picks a random float between [now - 30 days, now].
    """
    now_ts = datetime.now().timestamp()
    thirty_days_ago_ts = (datetime.now() - timedelta(days=30)).timestamp()
    random_ts = random.uniform(thirty_days_ago_ts, now_ts)
    return int(random_ts)

def generate_postmatch_info():
    """
    Generate random post-match information, including device and platform.
    """
    device = random.choice(DEVICES)
    platform = "iOS" if device == "iPhone" else random.choice(OS) if device == "PC" else "Android"

    return {
        "coin_balance_after_match": random.randint(0, 10000),
        "level_after_match": random.randint(1, 100),
        "device": device,
        "platform": platform
    }

def generate_init_event():
    """
    Generate a random 'init' event.
    """

    device = random.choice(DEVICES)
    platform = "iOS" if device == "iPhone" else random.choice(OS) if device == "PC" else "Android"

    return {
        "event_type": "init",
        "time": generate_random_timestamp(),
        "user_id": random.randint(1, 1000),
        "country": random.choice(COUNTRIES),
        "platform": platform
    }

def generate_match_event():
    """
    Generate a random 'match' event, including user A/B post-match info.
    """
    user_a = f"user_{random.randint(1, 1000)}"
    user_b = f"user_{random.randint(1, 1000)}"
    return {
        "event_type": "match",
        "time": generate_random_timestamp(),
        "user_a": user_a,
        "user_b": user_b,
        "user_a_postmatch_info": generate_postmatch_info(),
        "user_b_postmatch_info": generate_postmatch_info(),
        "winner": random.choice([user_a, user_b]),
        "game_tier": random.randint(1, 5),
        "duration": random.randint(30, 600)
    }

def generate_inapp_purchase_event():
    """
    Generate a random 'in-app-purchase' event.
    """
    return {
        "event_type": "in_app_purchase",
        "time": generate_random_timestamp(),
        "purchase_value": round(random.uniform(0.99, 99.99), 2),
        "user_id": random.randint(1, 1000),
        "product_id": f"product_{random.randint(1000, 9999)}"
    }