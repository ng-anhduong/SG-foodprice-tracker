# pipeline/matching/vegetable_produce_matching.py
#
# Algorithm 1 adapted specifically for Fruits & Vegetables.
#
# Core challenge: produce has no cross-store brands, no consistent sizing,
# and short product names. Standard brand+size+title scoring fails.
#
# Key idea: strip store-private labels (Pasar, Agro Fresh, Yuan Zhen Yuan, etc.)
# from product names before comparing, then match on:
#   1. Produce type (broccoli, apple, carrot) — must match
#   2. Origin (Australian, Malaysian, Thai) — should match
#   3. Variety/qualifier (Fuji, organic, baby, mini) — bonus
#
# Usage:
#   python3 pipeline/matching/vegetable_produce_matching.py

import hashlib
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from rapidfuzz import fuzz
from supabase import create_client

load_dotenv(".env")

CATEGORY = "Fruits & Vegetables"
STORE_ORDER = ["fairprice", "coldstorage", "redmart", "shengsiong"]
DEFAULT_OUTPUT_BASE = Path("data") / "matching"
FETCH_PAGE_SIZE = 1000

# ── STORE-PRIVATE LABELS TO STRIP ─────────────────────────────────────────────
# These are labels exclusive to one store and carry no cross-store meaning.
# Strip them before comparing product names.

PRIVATE_LABELS = {
    # FairPrice labels
    "pasar", "agro fresh", "agrofresh", "yuan zhen yuan", "yuanzhenyuan",
    "simply finest", "simplyfinest", "go fresh", "yili farm", "yilafarm",
    "chef", "hydrogreen", "vegeponics", "blush", "thygrace",
    "sustainir", "sustenir", "freshstory", "kok fah",
    # RedMart labels
    "churo",
    # general
    "sg local", "sg", "local",
}

# ── ORIGIN KEYWORDS ───────────────────────────────────────────────────────────
# Normalised origin strings — map aliases to canonical form

ORIGIN_MAP = {
    "australia": "australia", "australian": "australia", "aus": "australia",
    "malaysia": "malaysia", "malaysian": "malaysia",
    "thailand": "thailand", "thai": "thailand",
    "japan": "japan", "japanese": "japan",
    "korea": "korea", "korean": "korea",
    "china": "china", "chinese": "china",
    "new zealand": "new zealand", "nz": "new zealand",
    "india": "india", "indian": "india",
    "vietnam": "vietnam", "vietnamese": "vietnam",
    "usa": "usa", "us": "usa", "america": "usa", "american": "usa",
    "italy": "italy", "italian": "italy",
    "france": "france", "french": "france",
    "south africa": "south africa",
    "indonesia": "indonesia",
    "philippines": "philippines",
}

# ── PRODUCE QUALIFIERS ────────────────────────────────────────────────────────
# Qualifiers that distinguish different versions of the same produce.
# If two products have conflicting qualifiers they are NOT the same product.

QUALIFIER_GROUPS = [
    {"organic", "bio"},
    {"baby", "mini", "petit"},
    {"young", "old"},               # young ginger vs old ginger
    {"seedless"},
    {"cherry"},                     # cherry tomato vs regular tomato
    {"sweet"},                      # sweet corn vs corn
    {"purple"},                     # purple sweet potato vs sweet potato
    # apple varieties
    {"fuji"},
    {"gala", "royal gala"},
    {"pink lady"},
    {"granny smith", "green"},
    {"envy"},
    # grape varieties
    {"shine muscat", "muscat"},
    {"black"},
    {"red"},
    # banana varieties
    {"pisang mas"},
    # kiwi varieties
    {"gold", "golden"},
    {"ruby red"},
    # pear varieties
    {"packham"},
    {"fragrant"},
]

# ── PRODUCE TYPE TAXONOMY ─────────────────────────────────────────────────────
# Maps canonical produce name → keywords to detect it.
# Used to confirm two products are the same type before scoring.

PRODUCE_TYPES: dict[str, list[str]] = {
    # fruits
    "apple": ["apple"],
    "banana": ["banana", "pisang"],
    "grape": ["grape", "muscat"],
    "strawberry": ["strawberry"],
    "blueberry": ["blueberry"],
    "watermelon": ["watermelon"],
    "mango": ["mango"],
    "orange": ["orange", "mandarin", "tangerine"],
    "kiwi": ["kiwi"],
    "pear": ["pear"],
    "avocado": ["avocado"],
    "pineapple": ["pineapple"],
    "papaya": ["papaya"],
    "coconut": ["coconut"],
    "dragonfruit": ["dragonfruit", "dragon fruit"],
    "guava": ["guava"],
    "longan": ["longan"],
    "lemon": ["lemon"],
    "lime": ["lime"],
    "peach": ["peach", "nectarine"],
    "plum": ["plum"],
    "fig": ["fig"],
    "date": ["date", "dates"],
    # vegetables
    "broccoli": ["broccoli", "broccolini"],
    "cauliflower": ["cauliflower"],
    "carrot": ["carrot"],
    "cabbage": ["cabbage", "wong bok", "wongbok"],
    "spinach": ["spinach", "puay leng"],
    "xiao bai cai": ["xiao bai cai", "bok choy", "siew pak choy", "nai bai"],
    "kailan": ["kailan", "kai lan"],
    "chye sim": ["chye sim", "cai xin", "choy sum", "cai sim"],
    "long bean": ["long bean"],
    "lady finger": ["lady finger", "ladies finger", "okra"],
    "cucumber": ["cucumber"],
    "tomato": ["tomato"],
    "onion": ["onion"],
    "garlic": ["garlic"],
    "ginger": ["ginger"],
    "potato": ["potato"],
    "sweet potato": ["sweet potato"],
    "mushroom": ["mushroom", "shiitake", "enoki", "shimeji", "oyster mushroom", "king oyster"],
    "corn": ["corn", "sweet corn", "baby corn"],
    "capsicum": ["capsicum", "bell pepper"],
    "pumpkin": ["pumpkin", "butternut"],
    "kang kong": ["kang kong"],
    "bittergourd": ["bitter gourd", "bittergourd"],
    "spring onion": ["spring onion", "scallion"],
    "celery": ["celery"],
    "asparagus": ["asparagus"],
    "zucchini": ["zucchini"],
    "brinjal": ["brinjal", "eggplant"],
    "leek": ["leek"],
    "pea": ["pea", "snow pea", "pea sprout", "edamame"],
    "bean sprout": ["bean sprout"],
    "radish": ["radish", "daikon"],
    "beetroot": ["beetroot", "beet"],
    "lettuce": ["lettuce", "romaine", "butterhead", "iceberg"],
    "kale": ["kale"],
    "yam": ["yam", "nagaimo"],
    "lemongrass": ["lemon grass", "lemongrass"],
    "chilli": ["chilli", "chili", "chilli padi"],
}


# ── CORE DATA STRUCTURES ──────────────────────────────────────────────────────

@dataclass
class ParsedProduce:
    raw: dict[str, Any]
    original_name: str
    stripped_name: str          # name with private labels removed
    produce_type: Optional[str] # canonical produce type
    origin: Optional[str]       # canonical origin
    qualifiers: list[str]       # e.g. ["organic", "fuji"]
    core_tokens: list[str]      # tokens of stripped name, no noise
    latest_date: str


class UnionFind:
    def __init__(self):
        self.parent: dict[int, int] = {}

    def find(self, item: int) -> int:
        if item not in self.parent:
            self.parent[item] = item
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, a: int, b: int):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


# ── HELPERS ───────────────────────────────────────────────────────────────────

def get_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(url, key)


def slugify(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return value or "unknown"


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.lower()
    text = text.replace("×", " x ").replace("&", " and ")
    text = re.sub(r"[\(\)\[\],/+\-]", " ", text)
    text = re.sub(r"[^a-z0-9\.\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def strip_private_labels(name: str) -> str:
    """Remove store-private brand labels from produce name."""
    normalized = normalize_text(name)
    # Try longest labels first to avoid partial matches
    for label in sorted(PRIVATE_LABELS, key=len, reverse=True):
        normalized = re.sub(rf"\b{re.escape(label)}\b", "", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def extract_origin(text: str) -> Optional[str]:
    normalized = normalize_text(text)
    # Try multi-word origins first
    for alias, canonical in sorted(ORIGIN_MAP.items(), key=lambda x: -len(x[0])):
        if re.search(rf"\b{re.escape(alias)}\b", normalized):
            return canonical
    return None


def extract_qualifiers(text: str) -> list[str]:
    normalized = normalize_text(text)
    found = set()
    for group in QUALIFIER_GROUPS:
        for phrase in sorted(group, key=len, reverse=True):
            if re.search(rf"\b{re.escape(phrase)}\b", normalized):
                found.add(sorted(group)[0])
                break
    return sorted(found)


def extract_produce_type(text: str) -> Optional[str]:
    normalized = normalize_text(text)
    for produce_type, keywords in PRODUCE_TYPES.items():
        for kw in keywords:
            if re.search(rf"\b{re.escape(kw)}\b", normalized):
                return produce_type
    return None


def build_core_tokens(stripped_name: str, origin: Optional[str]) -> list[str]:
    noise = {
        "fresh", "premium", "prepacked", "pack", "local", "imported",
        "pesticide", "free", "seedless", "organic", "bio", "natural",
        "reared", "sg",
    }
    # Also remove origin words — they're tracked separately
    origin_words = set(normalize_text(origin).split()) if origin else set()

    tokens = []
    for tok in normalize_text(stripped_name).split():
        if tok in noise:
            continue
        if tok in origin_words:
            continue
        if re.fullmatch(r"\d+(\.\d+)?", tok):
            continue
        if tok in {"g", "kg", "ml", "l", "x", "pcs", "pc"}:
            continue
        tokens.append(tok)
    return tokens


def parse_produce(row: dict[str, Any], latest_date: str) -> ParsedProduce:
    name = row.get("name") or ""
    stripped = strip_private_labels(name)
    origin = extract_origin(name)
    qualifiers = extract_qualifiers(name)
    produce_type = extract_produce_type(name)
    core_tokens = build_core_tokens(stripped, origin)

    return ParsedProduce(
        raw=row,
        original_name=name,
        stripped_name=stripped,
        produce_type=produce_type,
        origin=origin,
        qualifiers=qualifiers,
        core_tokens=core_tokens,
        latest_date=latest_date,
    )


# ── SCORING ───────────────────────────────────────────────────────────────────

def produce_type_score(a: ParsedProduce, b: ParsedProduce) -> float:
    """Produce type must match — this is the most critical dimension."""
    if a.produce_type is None or b.produce_type is None:
        # Fall back to token overlap if type unknown
        shared = set(a.core_tokens) & set(b.core_tokens)
        return 0.5 if shared else 0.0
    return 1.0 if a.produce_type == b.produce_type else 0.0


def origin_score(a: ParsedProduce, b: ParsedProduce) -> float:
    if a.origin is None or b.origin is None:
        return 0.6  # unknown — don't penalise
    return 1.0 if a.origin == b.origin else 0.0


def qualifier_score(a: ParsedProduce, b: ParsedProduce) -> float:
    if not a.qualifiers and not b.qualifiers:
        return 0.8
    if not a.qualifiers or not b.qualifiers:
        return 0.5
    if set(a.qualifiers) == set(b.qualifiers):
        return 1.0
    if set(a.qualifiers) & set(b.qualifiers):
        return 0.5
    return 0.0  # conflicting qualifiers (e.g. organic vs non-organic)


def title_score(a: ParsedProduce, b: ParsedProduce) -> float:
    """Compare stripped names after removing private labels."""
    left = " ".join(a.core_tokens)
    right = " ".join(b.core_tokens)
    if not left or not right:
        return 0.0
    shared = set(a.core_tokens) & set(b.core_tokens)
    union = set(a.core_tokens) | set(b.core_tokens)
    jaccard = len(shared) / len(union) if union else 0.0
    return (
        0.40 * (fuzz.token_sort_ratio(left, right) / 100)
        + 0.35 * (fuzz.ratio(left, right) / 100)
        + 0.25 * jaccard
    )


def score_pair(a: ParsedProduce, b: ParsedProduce) -> dict[str, Any]:
    pt_score = produce_type_score(a, b)
    or_score = origin_score(a, b)
    ql_score = qualifier_score(a, b)
    ti_score = title_score(a, b)

    # Produce type is the anchor — if it doesn't match, nothing else matters
    if pt_score == 0.0:
        return {
            "produce_type_score": 0.0,
            "origin_score": or_score,
            "qualifier_score": ql_score,
            "title_score": round(ti_score, 4),
            "match_score": 0.0,
            "match_status": "no_match",
            "explanation": "produce_type_mismatch",
        }

    score = (
        0.40 * pt_score
        + 0.25 * or_score
        + 0.20 * ql_score
        + 0.15 * ti_score
    )

    penalties = []
    if or_score == 0.0:
        penalties.append(("origin_conflict", 0.20))   # Australian vs Chinese broccoli = different
    if ql_score == 0.0:
        penalties.append(("qualifier_conflict", 0.25)) # organic vs non-organic, fuji vs gala

    total_penalty = sum(p for _, p in penalties)
    final = max(0.0, min(1.0, score - total_penalty))

    if final >= 0.88 and ti_score >= 0.60:
        status = "strong_match"
    elif final >= 0.72:
        status = "review"
    else:
        status = "no_match"

    explanation_bits = [
        f"produce_type={pt_score:.2f}",
        f"origin={or_score:.2f}",
        f"qualifier={ql_score:.2f}",
        f"title={ti_score:.2f}",
    ]
    if penalties:
        explanation_bits.append("penalties=" + ",".join(f"{n}:{v:.2f}" for n, v in penalties))

    return {
        "produce_type_score": round(pt_score, 4),
        "origin_score": round(or_score, 4),
        "qualifier_score": round(ql_score, 4),
        "title_score": round(ti_score, 4),
        "match_score": round(final, 4),
        "match_status": status,
        "explanation": "; ".join(explanation_bits),
    }


def likely_candidate(a: ParsedProduce, b: ParsedProduce) -> bool:
    if a.raw["store"] == b.raw["store"]:
        return False
    # Must be same produce type (if known)
    if a.produce_type and b.produce_type and a.produce_type != b.produce_type:
        return False
    # Conflicting qualifiers → not the same product
    if a.qualifiers and b.qualifiers and set(a.qualifiers).isdisjoint(set(b.qualifiers)):
        # Allow if neither has a variety qualifier — just organic/baby type
        type_qualifiers = {"organic", "bio", "baby", "mini", "sweet", "seedless", "cherry", "young", "old", "purple"}
        a_variety = [q for q in a.qualifiers if q not in type_qualifiers]
        b_variety = [q for q in b.qualifiers if q not in type_qualifiers]
        if a_variety and b_variety and set(a_variety).isdisjoint(set(b_variety)):
            return False
    return True


# ── SUPABASE I/O ──────────────────────────────────────────────────────────────

def get_latest_date_for_store(supabase, store: str) -> Optional[str]:
    rows = (
        supabase.table("products")
        .select("scraped_at")
        .eq("store", store)
        .eq("unified_category", CATEGORY)
        .order("scraped_at", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if not rows:
        return None
    return rows[0]["scraped_at"][:10]


def fetch_products_for_store_date(supabase, store: str, date_str: str) -> list[dict]:
    start = f"{date_str}T00:00:00"
    end = f"{(date.fromisoformat(date_str) + timedelta(days=1)).isoformat()}T00:00:00"
    all_rows = []
    offset = 0
    while True:
        resp = (
            supabase.table("products")
            .select("id,name,brand,price_sgd,unit,unified_category,store,product_url,scraped_at")
            .eq("store", store)
            .eq("unified_category", CATEGORY)
            .gte("scraped_at", start)
            .lt("scraped_at", end)
            .order("id")
            .range(offset, offset + FETCH_PAGE_SIZE - 1)
            .execute()
        )
        rows = resp.data or []
        all_rows.extend(rows)
        if len(rows) < FETCH_PAGE_SIZE:
            break
        offset += FETCH_PAGE_SIZE
    return all_rows


# ── GROUP BUILDING ─────────────────────────────────────────────────────────────

def generate_pairwise_matches(products: list[ParsedProduce]) -> list[dict]:
    pairs = []
    items = sorted(products, key=lambda i: (i.raw["store"], i.raw["id"]))
    for i, left in enumerate(items):
        for right in items[i + 1:]:
            if not likely_candidate(left, right):
                continue
            scores = score_pair(left, right)
            if scores["match_status"] == "no_match":
                continue
            pairs.append({
                "product_id_a": left.raw["id"],
                "product_id_b": right.raw["id"],
                "store_a": left.raw["store"],
                "store_b": right.raw["store"],
                "name_a": left.original_name,
                "name_b": right.original_name,
                "stripped_a": left.stripped_name,
                "stripped_b": right.stripped_name,
                "produce_type_a": left.produce_type,
                "produce_type_b": right.produce_type,
                "origin_a": left.origin,
                "origin_b": right.origin,
                "qualifier_a": left.qualifiers,
                "qualifier_b": right.qualifiers,
                # store expected fields for product_match_candidates table
                "brand_a": None,
                "brand_b": None,
                "size_a": None,
                "size_b": None,
                "variant_a": left.qualifiers,
                "variant_b": right.qualifiers,
                "brand_score": scores["produce_type_score"],  # repurpose field
                "size_score": scores["origin_score"],
                "title_score": scores["title_score"],
                "variant_score": scores["qualifier_score"],
                "packaging_score": 0.7,
                **{k: v for k, v in scores.items() if k not in ("produce_type_score", "origin_score", "qualifier_score", "title_score")},
            })
    pairs.sort(key=lambda r: (-r["match_score"], r["store_a"], r["name_a"]))
    return pairs


def filter_reciprocal_strong_pairs(pair_matches: list[dict]) -> list[dict]:
    best: dict[tuple[int, str], dict] = {}
    for pair in pair_matches:
        if pair["match_status"] != "strong_match":
            continue
        lk = (pair["product_id_a"], pair["store_b"])
        rk = (pair["product_id_b"], pair["store_a"])
        if lk not in best or pair["match_score"] > best[lk]["match_score"]:
            best[lk] = pair
        if rk not in best or pair["match_score"] > best[rk]["match_score"]:
            best[rk] = pair

    reciprocal = []
    for pair in pair_matches:
        if pair["match_status"] != "strong_match":
            continue
        lk = (pair["product_id_a"], pair["store_b"])
        rk = (pair["product_id_b"], pair["store_a"])
        lb = best.get(lk)
        rb = best.get(rk)
        if lb and rb and lb["product_id_a"] == rb["product_id_b"] and lb["product_id_b"] == rb["product_id_a"]:
            reciprocal.append(pair)
    return reciprocal


def build_groups(products: list[ParsedProduce], strong_pairs: list[dict]):
    uf = UnionFind()
    for pair in strong_pairs:
        uf.union(pair["product_id_a"], pair["product_id_b"])

    groups: dict[int, list[ParsedProduce]] = defaultdict(list)
    for item in products:
        root = uf.find(item.raw["id"])
        groups[root].append(item)

    canonical_products = []
    canonical_members = []

    for group_items in sorted(groups.values(), key=lambda g: (-len(g), min(i.raw["id"] for i in g))):
        # Pick canonical name: prefer the longest stripped name
        best = max(group_items, key=lambda i: len(i.stripped_name))
        canonical_name = best.original_name
        produce_type = best.produce_type
        origin = best.origin
        qualifiers = sorted({q for i in group_items for q in i.qualifiers})

        # Build stable key from type + origin + qualifiers
        sig = {
            "category": "fruits-and-vegetables",
            "produce_type": produce_type or "unknown",
            "origin": origin or "unknown",
            "qualifiers": qualifiers,
        }
        digest = hashlib.sha1(json.dumps(sig, sort_keys=True).encode()).hexdigest()[:12]
        canonical_key = f"fruits-vegetables-{slugify(produce_type or 'unknown')}-{slugify(origin or 'unknown')}-{digest}"

        first_seen_at = min(i.raw.get("scraped_at") for i in group_items if i.raw.get("scraped_at"))
        last_seen_at = max(i.raw.get("scraped_at") for i in group_items if i.raw.get("scraped_at"))

        canonical_products.append({
            "canonical_key": canonical_key,
            "canonical_name": canonical_name,
            "brand": None,
            "unified_category": CATEGORY,
            "size_total_value": None,
            "size_base_unit": None,
            "size_display": None,
            "pack_count": None,
            "packaging": None,
            "variant_tokens": qualifiers,
            "member_count": len(group_items),
            "stores_present": sorted({i.raw["store"] for i in group_items}),
            "source_product_ids": sorted(i.raw["id"] for i in group_items),
            "first_seen_at": first_seen_at,
            "last_seen_at": last_seen_at,
        })

        for item in sorted(group_items, key=lambda x: (x.raw["store"], x.raw["id"])):
            canonical_members.append({
                "canonical_key": canonical_key,
                "product_id": item.raw["id"],
                "store": item.raw["store"],
                "name": item.original_name,
                "brand": item.raw.get("brand"),
                "price_sgd": item.raw.get("price_sgd"),
                "unit": item.raw.get("unit"),
                "product_url": item.raw.get("product_url"),
                "scraped_at": item.raw.get("scraped_at"),
                "match_confidence": 1.0 if len(group_items) == 1 else None,
            })

    # Consolidate duplicate canonical keys
    consolidated_products: dict[str, dict] = {}
    consolidated_members: dict[int, dict] = {}
    for row in canonical_products:
        existing = consolidated_products.get(row["canonical_key"])
        if existing is None:
            consolidated_products[row["canonical_key"]] = row
        else:
            existing["member_count"] += row["member_count"]
            existing["stores_present"] = sorted(set(existing["stores_present"]) | set(row["stores_present"]))
            existing["source_product_ids"] = sorted(set(existing["source_product_ids"]) | set(row["source_product_ids"]))
    for row in canonical_members:
        consolidated_members[row["product_id"]] = row

    return list(consolidated_products.values()), list(consolidated_members.values())


def save_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def sync_results_to_supabase(supabase, run_key, canonical_products, canonical_members, pair_matches):
    # Upsert canonical products
    canonical_payloads = [{k: v for k, v in r.items() if k != "source_product_ids"} for r in canonical_products]
    for i in range(0, len(canonical_payloads), 200):
        supabase.table("canonical_products").upsert(canonical_payloads[i:i + 200], on_conflict="canonical_key", ignore_duplicates=False).execute()

    # Fetch IDs
    existing = {}
    offset = 0
    while True:
        resp = supabase.table("canonical_products").select("id,canonical_key").range(offset, offset + 999).execute()
        for row in resp.data or []:
            existing[row["canonical_key"]] = row["id"]
        if len(resp.data or []) < 1000:
            break
        offset += 1000

    # Upsert members
    member_payloads = []
    for row in canonical_members:
        canon_id = existing.get(row["canonical_key"])
        if canon_id is None:
            continue
        member_payloads.append({
            "canonical_product_id": canon_id,
            "product_id": row["product_id"],
            "store": row["store"],
            "name": row["name"],
            "brand": row.get("brand"),
            "price_sgd": row.get("price_sgd"),
            "unit": row.get("unit"),
            "product_url": row.get("product_url"),
            "scraped_at": row.get("scraped_at"),
            "run_key": run_key,
            "match_source": "rule_based_produce_v1",
            "match_confidence": row.get("match_confidence"),
        })
    for i in range(0, len(member_payloads), 200):
        supabase.table("canonical_product_members").upsert(member_payloads[i:i + 200], on_conflict="product_id", ignore_duplicates=False).execute()

    # Upsert candidates
    candidate_payloads = []
    for row in pair_matches:
        a_id, b_id = row["product_id_a"], row["product_id_b"]
        if a_id > b_id:
            a_id, b_id = b_id, a_id
        candidate_payloads.append({
            "run_key": run_key,
            "unified_category": CATEGORY,
            "product_id_a": a_id,
            "product_id_b": b_id,
            "store_a": row["store_a"],
            "store_b": row["store_b"],
            "name_a": row["name_a"],
            "name_b": row["name_b"],
            "brand_a": None, "brand_b": None,
            "size_a": None, "size_b": None,
            "variant_a": row.get("qualifier_a", []),
            "variant_b": row.get("qualifier_b", []),
            "brand_score": row["brand_score"],
            "size_score": row["size_score"],
            "title_score": row["title_score"],
            "variant_score": row["variant_score"],
            "packaging_score": row["packaging_score"],
            "match_score": row["match_score"],
            "match_status": row["match_status"],
            "explanation": row.get("explanation"),
        })
    deduped = {(r["product_id_a"], r["product_id_b"]): r for r in candidate_payloads}
    candidate_payloads = list(deduped.values())
    for i in range(0, len(candidate_payloads), 200):
        supabase.table("product_match_candidates").upsert(candidate_payloads[i:i + 200], on_conflict="run_key,product_id_a,product_id_b", ignore_duplicates=True).execute()

    return {
        "canonical_products_upserted": len(canonical_payloads),
        "canonical_members_upserted": len(member_payloads),
        "match_candidates_upserted": len(candidate_payloads),
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run():
    supabase = get_client()
    print("=" * 70)
    print(f"Produce matching: {CATEGORY}")
    print("=" * 70)

    products: list[ParsedProduce] = []
    store_dates: dict[str, str] = {}

    for store in STORE_ORDER:
        latest_date = get_latest_date_for_store(supabase, store)
        if not latest_date:
            print(f"[{store}] No rows found")
            continue
        store_dates[store] = latest_date
        rows = fetch_products_for_store_date(supabase, store, latest_date)
        print(f"[{store}] fetched {len(rows)} rows for {latest_date}")
        for row in rows:
            products.append(parse_produce(row, latest_date))

    pair_matches = generate_pairwise_matches(products)
    strong_pairs_raw = [r for r in pair_matches if r["match_status"] == "strong_match"]
    strong_pairs = filter_reciprocal_strong_pairs(pair_matches)
    review_pairs = [r for r in pair_matches if r["match_status"] == "review"]
    canonical_products, canonical_members = build_groups(products, strong_pairs)

    latest_dates_slug = "-".join(f"{s}_{d}" for s, d in sorted(store_dates.items()))
    run_key = f"fruits-vegetables-produce-{latest_dates_slug}"
    output_dir = DEFAULT_OUTPUT_BASE / "fruits-and-vegetables" / latest_dates_slug

    summary = {
        "run_key": run_key,
        "category": CATEGORY,
        "store_dates": store_dates,
        "input_products": len(products),
        "strong_match_pairs_raw": len(strong_pairs_raw),
        "strong_match_pairs": len(strong_pairs),
        "review_pairs": len(review_pairs),
        "canonical_groups": len(canonical_products),
        "cross_store_groups": sum(1 for r in canonical_products if len(r["stores_present"]) > 1),
        "generated_at": datetime.now().isoformat(),
    }

    save_json(output_dir / "strong_match_pairs.json", strong_pairs)
    save_json(output_dir / "review_pairs.json", review_pairs)
    save_json(output_dir / "canonical_products.json", canonical_products)
    save_json(output_dir / "canonical_product_members.json", canonical_members)

    sync_summary = sync_results_to_supabase(supabase, run_key, canonical_products, canonical_members, strong_pairs + review_pairs)
    summary["supabase_sync"] = sync_summary
    save_json(output_dir / "summary.json", summary)

    print(f"\nSaved outputs to: {output_dir}")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    run()
