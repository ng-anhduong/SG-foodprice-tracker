# pipeline/commodity_matching.py
#
# Algorithm 2: Commodity / Cut-based matching for Meat & Seafood and Fruits & Vegetables.
#
# Unlike Algorithm 1 (brand-based), this algorithm:
#   - Groups products by CUT TYPE (e.g. "chicken breast", "salmon fillet", "broccoli")
#   - Computes UNIT PRICE (price per 100g) for each product
#   - Compares unit prices across stores for the same cut
#   - Flags fresh vs frozen products separately
#
# Usage:
#   python3 pipeline/commodity_matching.py                  # runs both categories
#   python3 pipeline/commodity_matching.py "Meat & Seafood"
#   python3 pipeline/commodity_matching.py "Fruits & Vegetables"

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(".env")

CATEGORIES = ["Meat & Seafood", "Fruits & Vegetables"]
STORES = ["fairprice", "shengsiong", "redmart", "coldstorage"]
FETCH_PAGE_SIZE = 1000

# ── CUT TAXONOMY ──────────────────────────────────────────────────────────────
# Maps a canonical cut name to a list of keywords to match in product names.
# Keywords are checked as substrings of the normalized (lowercased) product name.
# Order within each list does not matter.
# Chinese character keywords included for Sheng Siong's bilingual naming.

MEAT_CUTS: dict[str, list[str]] = {
    # Chicken
    "chicken - whole": ["whole chicken", "chicken griller", "spring chicken", "half chicken", "chicken half"],
    "chicken - breast": ["chicken breast", "breast boneless", "breast fillet", "breast skinless", "boneless breast", "boneless skinless breast"],
    "chicken - leg": ["chicken leg", "boneless leg", "leg boneless", "leg quarter", "whole leg", "chicken boneless leg"],
    "chicken - thigh": ["chicken thigh", "thigh boneless", "thigh skinless", "thigh cube"],
    "chicken - drumstick": ["drumstick"],
    "chicken - wing": ["mid joint wing", "wing stick", "drumette", "winglet", "chicken wing", "3 joint wing"],
    "chicken - fillet": ["chicken fillet", "chicken tenderloin"],
    "chicken - mince": ["minced chicken", "chicken mince"],
    "chicken - feet": ["chicken feet"],
    "chicken - bones": ["chicken bone", "chicken bones"],

    # Pork
    "pork - belly": ["pork belly", "三层肉", "三层肉片", "belly slices", "belly chunk", "belly cubes"],
    "pork - collar/shoulder": ["pork collar", "pork shoulder", "twee bak", "五花肉", "collar steak", "collar slices", "collar cubes", "collar minced"],
    "pork - spare rib": ["spare rib", "big spare rib", "prime rib", "小排", "子弹排"],
    "pork - loin": ["pork loin", "pork chop", "loin chop", "loin boneless", "loin bone"],
    "pork - fillet": ["pork fillet", "pork tenderloin", "腰肉"],
    "pork - mince": ["minced pork", "pork mince", "肉碎"],
    "pork - soft bone": ["soft bone", "软骨"],
    "pork - soup bone": ["soup bone", "big bone", "汤骨"],
    "pork - trotter": ["pork trotter", "trotter"],

    # Beef
    "beef - ribeye": ["beef ribeye", "ribeye steak", "rib eye"],
    "beef - striploin": ["beef striploin", "striploin steak", "strip loin"],
    "beef - mince": ["minced beef", "beef mince", "mince beef"],
    "beef - brisket": ["beef brisket", "brisket flat"],
    "beef - flank": ["beef flank", "flank steak", "flank stir fry"],
    "beef - chuck": ["beef chuck", "chuck tender", "chuck roll"],
    "beef - tenderloin": ["beef tenderloin"],
    "beef - oxtail": ["beef oxtail", "oxtail"],
    "beef - short rib": ["beef short rib", "short ribs", "galbi"],

    # Lamb
    "lamb - rack": ["lamb rack", "lamb cutlet", "frenched lamb"],
    "lamb - diced": ["lamb diced", "lamb cube"],

    # Seafood
    "seafood - salmon": ["salmon fillet", "salmon portion", "salmon belly", "salmon steak", "salmon chunk", "三文鱼", "salmon head"],
    "seafood - prawn": ["tiger prawn", "vannamei prawn", "vannamei shrimp", "shrimp meat", "prawn meat", "peeled prawn", "glass prawn", "banana prawn"],
    "seafood - squid": ["squid ring", "squid tube", "squid flower", "sotong", "鱿鱼", "苏东", "cuttlefish"],
    "seafood - scallop": ["hokkaido scallop", "sea scallop", "bay scallop", "boiled scallop", "half shell scallop"],
    "seafood - clam": ["clam meat", "flower clam", "white clam", "venus clam", "shortnecked clam"],
    "seafood - mussel": ["mussel meat", "whole shell mussel", "half shell mussel"],
    "seafood - pomfret": ["golden pomfret", "black pomfret", "chinese pomfret", "white pomfret", "白鲳", "黑鲳", "金鲳"],
    "seafood - threadfin": ["threadfin", "午鱼", "balai threadfin"],
    "seafood - batang/mackerel": ["batang", "spanish mackerel", "巴东", "saba fish", "saba mackerel"],
    "seafood - snapper": ["red snapper", "white snapper", "ang go li", "红鸡"],
    "seafood - seabass/barramundi": ["seabass", "barramundi", "asian seabass"],
    "seafood - stingray": ["stingray", "方鱼"],
    "seafood - dory/sutchi": ["dory", "sutchi fillet", "sutchi cube", "多利鱼"],
    "seafood - oyster": ["oyster meat", "生蚝"],
}

PRODUCE_CUTS: dict[str, list[str]] = {
    # Fruits
    "fruit - apple": ["apple", "fuji apple", "gala apple", "pink lady", "granny smith"],
    "fruit - banana": ["banana", "pisang mas"],
    "fruit - grape": ["grape", "seedless grape", "shine muscat", "muscat"],
    "fruit - strawberry": ["strawberry"],
    "fruit - blueberry": ["blueberry"],
    "fruit - watermelon": ["watermelon"],
    "fruit - mango": ["mango"],
    "fruit - orange": ["orange", "cara cara", "mandarin orange", "mandarin"],
    "fruit - kiwi": ["kiwi"],
    "fruit - pear": ["pear", "packham pear", "fragrant pear", "bing tang pear"],
    "fruit - avocado": ["avocado"],
    "fruit - pineapple": ["pineapple"],
    "fruit - papaya": ["papaya"],
    "fruit - coconut": ["coconut"],
    "fruit - dragonfruit": ["dragonfruit", "dragon fruit"],
    "fruit - guava": ["guava", "seedless guava"],
    "fruit - longan": ["longan"],
    "fruit - durian": ["durian"],

    # Vegetables
    "veg - broccoli": ["broccoli", "broccolini"],
    "veg - cauliflower": ["cauliflower"],
    "veg - carrot": ["carrot"],
    "veg - cabbage": ["cabbage", "beijing cabbage", "wong bok", "wongbok", "round cabbage", "purple cabbage"],
    "veg - spinach": ["spinach", "puay leng", "round spinach", "red spinach", "sharp spinach"],
    "veg - xiao bai cai": ["xiao bai cai", "bok choy", "siew pak choy", "nai bai", "baby bok choy"],
    "veg - kailan": ["kailan", "kai lan", "baby kailan", "baby kai lan"],
    "veg - chye sim": ["chye sim", "cai xin", "cai sim", "choy sum"],
    "veg - long bean": ["long bean"],
    "veg - lady finger": ["lady finger", "ladies finger", "okra"],
    "veg - cucumber": ["cucumber", "japanese cucumber", "old cucumber", "local cucumber"],
    "veg - tomato": ["tomato", "cherry tomato"],
    "veg - onion": ["yellow onion", "red onion", "white onion", "small onion", "shallot"],
    "veg - garlic": ["garlic"],
    "veg - ginger": ["old ginger", "young ginger", "ginger"],
    "veg - potato": ["potato", "sweet potato", "purple sweet potato", "baby potato"],
    "veg - mushroom": ["shiitake mushroom", "enoki mushroom", "shimeji mushroom", "oyster mushroom", "king oyster", "button mushroom", "mushroom"],
    "veg - corn": ["sweet corn", "corn", "baby corn"],
    "veg - capsicum": ["capsicum", "bell pepper"],
    "veg - pumpkin": ["pumpkin", "butternut"],
    "veg - kang kong": ["kang kong"],
    "veg - bittergourd": ["bitter gourd", "bittergourd"],
    "veg - spring onion": ["spring onion", "scallion"],
    "veg - celery": ["celery"],
    "veg - asparagus": ["asparagus"],
    "veg - zucchini": ["zucchini"],
    "veg - chilli": ["chilli padi", "red chilli", "green chilli"],
}

ALL_CUTS = {**MEAT_CUTS, **PRODUCE_CUTS}

FROZEN_KEYWORDS = ["frozen", "iqf", "冻", "冷冻"]


# ── HELPERS ───────────────────────────────────────────────────────────────────

def get_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(url, key)


def fetch_all_rows(supabase, table: str, columns: str, category: str) -> list[dict]:
    rows = []
    offset = 0
    while True:
        resp = (
            supabase.table(table)
            .select(columns)
            .eq("unified_category", category)
            .range(offset, offset + FETCH_PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < FETCH_PAGE_SIZE:
            break
        offset += FETCH_PAGE_SIZE
    return rows


def normalize(text: str) -> str:
    text = text.lower()
    text = text.replace("×", " x ").replace("&", " and ")
    text = re.sub(r"[\(\)\[\],/+\-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_frozen(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in FROZEN_KEYWORDS)


def extract_cut(name: str) -> Optional[str]:
    """Return the first matching canonical cut name, or None."""
    normalized = normalize(name)
    for cut, keywords in ALL_CUTS.items():
        for kw in keywords:
            # Match as substring, with word-boundary awareness for short keywords
            if kw in normalized:
                return cut
    return None


def extract_weight_g(text: str) -> Optional[float]:
    """Extract weight in grams from product name or unit field."""
    normalized = normalize(text)

    # e.g. "6 x 200g" -> total 1200g
    multi = re.search(r"(\d+)\s*x\s*(\d+(?:\.\d+)?)\s*(g|kg)", normalized)
    if multi:
        count = int(multi.group(1))
        value = float(multi.group(2))
        unit = multi.group(3)
        grams = value * 1000 if unit == "kg" else value
        return round(grams * count, 2)

    # e.g. "500g" or "1.5kg"
    single = re.search(r"(\d+(?:\.\d+)?)\s*(g|kg)\b", normalized)
    if single:
        value = float(single.group(1))
        unit = single.group(2)
        return value * 1000 if unit == "kg" else value

    return None


def latest_date_per_store(rows: list[dict]) -> dict[str, str]:
    """For each store, find the most recent scraped_at date."""
    latest: dict[str, str] = {}
    for row in rows:
        store = row.get("store", "")
        scraped = (row.get("scraped_at") or "")[:10]  # YYYY-MM-DD
        if store not in latest or scraped > latest[store]:
            latest[store] = scraped
    return latest


# ── CORE MATCHING ─────────────────────────────────────────────────────────────

def build_commodity_comparisons(rows: list[dict]) -> list[dict]:
    """
    Groups products by (cut, fresh/frozen) and computes unit price per 100g.
    Returns one comparison row per cut per day with per-store price breakdown.
    """
    latest_per_store = latest_date_per_store(rows)

    # Only use the latest available date per store
    filtered = []
    for row in rows:
        store = row.get("store", "")
        scraped = (row.get("scraped_at") or "")[:10]
        if scraped == latest_per_store.get(store):
            filtered.append(row)

    # Group products by cut + frozen flag
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)

    for row in filtered:
        name = row.get("name") or ""
        unit = row.get("unit") or ""
        price = row.get("price_sgd")

        if price is None:
            continue

        cut = extract_cut(name)
        if cut is None:
            continue

        # Try weight from unit field first, then name
        weight_g = extract_weight_g(unit) or extract_weight_g(name)
        if weight_g is None or weight_g <= 0:
            continue

        frozen_flag = "frozen" if is_frozen(name) else "fresh/chilled"
        unit_price_per_100g = round((price / weight_g) * 100, 4)

        groups[(cut, frozen_flag)].append({
            "product_id": row.get("id"),
            "name": name,
            "store": row.get("store"),
            "price_sgd": price,
            "weight_g": weight_g,
            "unit_price_per_100g": unit_price_per_100g,
            "product_url": row.get("product_url"),
            "scraped_date": latest_per_store.get(row.get("store", ""), ""),
        })

    # Build comparison rows — only keep groups where 2+ stores are present
    comparisons = []
    refreshed_at = datetime.now().isoformat()

    for (cut, frozen_flag), products in groups.items():
        stores_present = {p["store"] for p in products}
        if len(stores_present) < 2:
            continue

        # Per store: pick the product with the lowest unit price
        by_store: dict[str, dict] = {}
        for p in products:
            store = p["store"]
            if store not in by_store or p["unit_price_per_100g"] < by_store[store]["unit_price_per_100g"]:
                by_store[store] = p

        store_list = sorted(by_store.values(), key=lambda x: x["unit_price_per_100g"])
        cheapest = store_list[0]
        priciest = store_list[-1]

        store_prices = {
            s["store"]: {
                "product_id": s["product_id"],
                "product_name": s["name"],
                "price_sgd": s["price_sgd"],
                "weight_g": s["weight_g"],
                "unit_price_per_100g": s["unit_price_per_100g"],
                "product_url": s["product_url"],
                "is_cheapest": s["store"] == cheapest["store"],
            }
            for s in store_list
        }

        category = "Meat & Seafood" if cut in MEAT_CUTS else "Fruits & Vegetables"

        comparisons.append({
            "cut": cut,
            "frozen_flag": frozen_flag,
            "unified_category": category,
            "stores_seen": len(stores_present),
            "cheapest_store": cheapest["store"],
            "cheapest_unit_price_per_100g": cheapest["unit_price_per_100g"],
            "cheapest_product_name": cheapest["name"],
            "priciest_store": priciest["store"],
            "priciest_unit_price_per_100g": priciest["unit_price_per_100g"],
            "price_spread_per_100g": round(
                priciest["unit_price_per_100g"] - cheapest["unit_price_per_100g"], 4
            ),
            "store_prices": store_prices,
            "scraped_date": cheapest["scraped_date"],
            "refreshed_at": refreshed_at,
        })

    comparisons.sort(key=lambda x: -x["price_spread_per_100g"])
    return comparisons


# ── OUTPUT ────────────────────────────────────────────────────────────────────

def save_local(comparisons: list[dict], category: str):
    slug = category.lower().replace(" ", "-").replace("&", "and").replace("/", "-")
    out_dir = Path("data") / "commodity_matching"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{slug}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(comparisons, f, indent=2, ensure_ascii=False)
    print(f"  Saved {len(comparisons)} comparison groups -> {out_path}")


def sync_to_supabase(supabase, comparisons: list[dict]):
    if not comparisons:
        return
    # Upsert in batches of 200
    for i in range(0, len(comparisons), 200):
        batch = comparisons[i:i + 200]
        supabase.table("commodity_price_comparisons").upsert(
            batch,
            on_conflict="cut,frozen_flag,scraped_date",
            ignore_duplicates=False,
        ).execute()
    print(f"  Synced {len(comparisons)} rows to Supabase")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run(category: Optional[str] = None, sync: bool = True):
    supabase = get_client()
    categories = [category] if category else CATEGORIES

    for cat in categories:
        print(f"\n{'=' * 60}")
        print(f"Commodity matching: {cat}")
        print("=" * 60)

        rows = fetch_all_rows(
            supabase,
            "products",
            "id,name,brand,price_sgd,unit,store,product_url,scraped_at,unified_category",
            cat,
        )
        print(f"  Fetched {len(rows)} products from Supabase")

        comparisons = build_commodity_comparisons(rows)
        print(f"  Found {len(comparisons)} cross-store cut comparisons")

        save_local(comparisons, cat)

        if sync:
            sync_to_supabase(supabase, comparisons)

    print("\nDone.")


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(arg)
