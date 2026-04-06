# pipeline/meat_produce_matching.py
#
# Algorithm 1 adapted for Meat & Seafood and Fruits & Vegetables.
# Same engine as matching.py but with domain-specific tuning:
#   - STOPWORDS: meat/produce terms instead of beverage terms
#   - VARIANT_GROUPS: cut/state variants instead of flavour variants
#   - BRAND_ALIASES: meat/produce brands instead of beverage brands
#   - PACKAGING_TERMS: meat packaging instead of drink packaging
#   - Scoring weights adjusted: brand less important, title/size more important
#
# Best for: packaged/branded products (e.g. "Seara Frozen Chicken Breast 1kg")
# For fresh unbranded cuts, use commodity_matching.py instead.
#
# Usage:
#   python3 pipeline/meat_produce_matching.py "Meat & Seafood"
#   python3 pipeline/meat_produce_matching.py "Fruits & Vegetables"

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

STORE_ORDER = ["fairprice", "coldstorage", "redmart", "shengsiong"]
SUPPORTED_CATEGORIES = ["Meat & Seafood", "Fruits & Vegetables"]
DEFAULT_OUTPUT_BASE = Path("data") / "matching"
FETCH_PAGE_SIZE = 1000

# ── DOMAIN TUNING ─────────────────────────────────────────────────────────────

STOPWORDS = {
    # generic noise
    "the", "and", "with", "for", "plus", "free", "fresh", "premium",
    "selection", "per", "pack", "packet",
    # meat-specific noise (too common to be useful for matching)
    "frozen", "chilled", "fresh", "halal", "organic", "reared",
    "probiotic", "antibiotic", "free", "range", "natural", "marinated",
    "ready", "cook", "cleaned", "gutted", "iqf",
    # produce-specific noise
    "local", "imported", "pesticide", "prepacked", "seedless",
}

# Packaging terms relevant to meat/produce
PACKAGING_TERMS = {
    "tray": {"tray", "trays"},
    "bag": {"bag", "bags", "pouch"},
    "vacuum": {"vacuum", "vac"},
    "whole": {"whole"},
    "fillet": {"fillet", "fillets"},
    "slice": {"slice", "slices", "sliced"},
}

# Variant groups for meat/produce — covers cooking state, cut style, origin
VARIANT_GROUPS = [
    # cooking state
    {"frozen", "iqf"},
    {"fresh", "chilled"},
    # bone variants
    {"boneless", "bone in", "bone-in"},
    {"skinless", "skin on", "skin-on"},
    # cut style
    {"minced", "mince", "ground"},
    {"sliced", "stir fry", "stir-fry", "shabu shabu"},
    {"cubed", "cube", "diced", "dice"},
    {"whole"},
    {"fillet", "fillets"},
    # chicken specific
    {"drumstick"},
    {"wing", "winglet", "mid joint"},
    {"breast"},
    {"thigh"},
    {"leg"},
    # pork specific
    {"belly"},
    {"collar", "shoulder"},
    {"spare rib", "rib"},
    {"loin"},
    # beef specific
    {"ribeye"},
    {"striploin"},
    {"brisket"},
    {"flank"},
    # seafood specific
    {"prawn", "shrimp"},
    {"salmon"},
    {"squid", "cuttlefish", "sotong"},
    {"scallop"},
    {"pomfret"},
    # produce state
    {"raw"},
    {"cooked"},
    # origin (important for produce — australia vs malaysia vs china)
    {"australia", "australian", "aus"},
    {"malaysia", "malaysian"},
    {"thailand", "thai"},
    {"japan", "japanese"},
    {"korea", "korean"},
    {"china", "chinese"},
    {"new zealand", "nz"},
]

# Brand aliases for meat/produce stores
BRAND_ALIASES = {
    # chicken brands
    "farmfresh": "farmfresh",
    "farm fresh": "farmfresh",
    "kee song": "keesong",
    "keesong": "keesong",
    "sakura": "sakura",
    "jean fresh": "jeanfresh",
    "jean": "jeanfresh",
    "aw's market": "awsmarket",
    "aws market": "awsmarket",
    "aw s market": "awsmarket",
    "master grocer": "mastergrocer",
    "hego": "hego",
    "punched foods": "punchedfoods",
    "tegel": "tegel",
    # frozen meat brands
    "seara": "seara",
    "perdigao": "perdigao",
    "danpo": "danpo",
    "rose": "rose",
    "cp": "cp",
    "bfg": "bfg",
    "dalee": "dalee",
    "le bao": "lebao",
    # seafood brands
    "serve by hai sia": "haisia",
    "hai sia": "haisia",
    "catch seafood": "catchseafood",
    "ocean fresh delite": "oceanfreshdelite",
    "pan royal": "panroyal",
    "redmart": "redmart",
    # produce brands
    "pasar": "pasar",
    "agro fresh": "agrofresh",
    "yuan zhen yuan": "yuanzhenyuan",
    "zenxin": "zenxin",
    "zespri": "zespri",
    "driscoll's": "driscolls",
    "driscolls": "driscolls",
    "sunkist": "sunkist",
    "sumifru": "sumifru",
    "yayapapaya": "yayapapaya",
    "simply finest": "simplyfinest",
    "chef": "chef",
    "hokto": "hokto",
}


# ── CORE DATA STRUCTURES (same as matching.py) ────────────────────────────────

@dataclass
class ParsedProduct:
    raw: dict[str, Any]
    parsed_name: str
    normalized_brand: Optional[str]
    tokens: list[str]
    core_tokens: list[str]
    variant_tokens: list[str]
    packaging: Optional[str]
    size_base_unit: Optional[str]
    size_total_value: Optional[float]
    size_display: Optional[str]
    pack_count: Optional[int]
    size_each_value: Optional[float]
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

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


# ── HELPERS (same logic as matching.py) ───────────────────────────────────────

def get_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(url, key)


def slugify(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return value or "unknown"


def stable_number_fragment(value: Optional[float]) -> str:
    if value is None:
        return "na"
    if float(value).is_integer():
        return str(int(value))
    return str(round(float(value), 3)).replace(".", "p")


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.lower()
    text = text.replace("×", " x ").replace("&", " and ")
    text = re.sub(r"[\(\)\[\],/+]", " ", text)
    text = re.sub(r"[^a-z0-9\.\s\u4e00-\u9fff]", " ", text)  # keep Chinese chars
    return re.sub(r"\s+", " ", text).strip()


def normalize_brand(value: Optional[str], name: str) -> Optional[str]:
    brand = normalize_text(value)
    if not brand:
        name_tokens = normalize_text(name).split()
        if not name_tokens:
            return None
        brand = name_tokens[0]
        if len(name_tokens) > 1 and name_tokens[0] in {"le", "mr", "st"}:
            brand = f"{name_tokens[0]} {name_tokens[1]}"
    return BRAND_ALIASES.get(brand, brand) or None


def tokenize(value: str) -> list[str]:
    return [tok for tok in normalize_text(value).split() if tok]


def extract_variant_tokens(name: str) -> list[str]:
    normalized = normalize_text(name)
    found: set[str] = set()
    for group in VARIANT_GROUPS:
        for phrase in sorted(group, key=len, reverse=True):
            if re.search(rf"(^|\s){re.escape(phrase)}($|\s)", normalized):
                found.add(sorted(group)[0])
                break
    return sorted(found)


def extract_packaging(tokens: list[str]) -> Optional[str]:
    token_set = set(tokens)
    for label, variants in PACKAGING_TERMS.items():
        if token_set & variants:
            return label
    return None


def convert_to_base(value: float, unit: str) -> tuple[float, str]:
    unit = unit.lower()
    if unit == "l":
        return value * 1000, "ml"
    if unit == "kg":
        return value * 1000, "g"
    return value, unit


def parse_size_from_text(text: str):
    normalized = normalize_text(text)

    multi = re.search(
        r"(?P<count>\d+)\s*x\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ml|l|g|kg)",
        normalized,
    )
    if multi:
        count = int(multi.group("count"))
        value = float(multi.group("value"))
        unit = multi.group("unit")
        base_value, base_unit = convert_to_base(value, unit)
        return (
            round(base_value * count, 2), base_unit, count,
            f"{count} x {value:g}{unit}", round(base_value, 2),
        )

    single = re.search(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ml|l|g|kg)", normalized)
    if single:
        value = float(single.group("value"))
        unit = single.group("unit")
        base_value, base_unit = convert_to_base(value, unit)
        return round(base_value, 2), base_unit, 1, f"{value:g}{unit}", round(base_value, 2)

    return None, None, None, None, None


def build_core_tokens(tokens: list[str], brand: Optional[str]) -> list[str]:
    brand_parts = set(tokenize(brand or ""))
    core = []
    for token in tokens:
        if token in STOPWORDS:
            continue
        if token in brand_parts:
            continue
        if re.fullmatch(r"\d+(\.\d+)?", token):
            continue
        if token in {"ml", "l", "g", "kg", "x", "s"}:
            continue
        core.append(token)
    return core


def parse_product(row: dict[str, Any], latest_date: str) -> ParsedProduct:
    name = row.get("name") or ""
    brand = normalize_brand(row.get("brand"), name)
    tokens = tokenize(name)
    packaging = extract_packaging(tokens)
    size_total, size_unit, pack_count, size_display, size_each = parse_size_from_text(
        row.get("unit") or name
    )
    core_tokens = build_core_tokens(tokens, brand)
    variant_tokens = extract_variant_tokens(name)

    return ParsedProduct(
        raw=row,
        parsed_name=normalize_text(name),
        normalized_brand=brand,
        tokens=tokens,
        core_tokens=core_tokens,
        variant_tokens=variant_tokens,
        packaging=packaging,
        size_base_unit=size_unit,
        size_total_value=size_total,
        size_display=size_display,
        pack_count=pack_count,
        size_each_value=size_each,
        latest_date=latest_date,
    )


# ── SCORING (same structure, adjusted weights) ────────────────────────────────

def brand_score(a: ParsedProduct, b: ParsedProduct) -> float:
    if a.normalized_brand and b.normalized_brand:
        if a.normalized_brand == b.normalized_brand:
            return 1.0
        a_parts = set(tokenize(a.normalized_brand))
        b_parts = set(tokenize(b.normalized_brand))
        if a_parts & b_parts:
            return 0.8
        return 0.0
    # Brand missing is common in fresh meat — penalise less
    if a.normalized_brand or b.normalized_brand:
        return 0.5
    return 0.4


def size_score(a: ParsedProduct, b: ParsedProduct) -> float:
    if a.size_total_value is None or b.size_total_value is None:
        return 0.55
    if a.size_base_unit != b.size_base_unit:
        return 0.0
    if a.pack_count and b.pack_count and a.pack_count != b.pack_count:
        if a.pack_count > 1 or b.pack_count > 1:
            return 0.0
    larger = max(a.size_total_value, b.size_total_value)
    smaller = min(a.size_total_value, b.size_total_value)
    if larger == 0:
        return 0.0
    ratio = abs(larger - smaller) / larger
    if ratio <= 0.02:
        return 1.0
    if ratio <= 0.10:
        return 0.85
    if ratio <= 0.20:
        return 0.65  # meat packs have more size variation than beverages
    return 0.0


def title_score(a: ParsedProduct, b: ParsedProduct) -> float:
    if not a.core_tokens or not b.core_tokens:
        return fuzz.ratio(a.parsed_name, b.parsed_name) / 100
    left = " ".join(a.core_tokens)
    right = " ".join(b.core_tokens)
    shared = set(a.core_tokens) & set(b.core_tokens)
    union = set(a.core_tokens) | set(b.core_tokens)
    jaccard = len(shared) / len(union) if union else 0.0
    return (
        0.45 * (fuzz.token_sort_ratio(left, right) / 100)
        + 0.35 * (fuzz.ratio(left, right) / 100)
        + 0.20 * jaccard
    )


def variant_score(a: ParsedProduct, b: ParsedProduct) -> float:
    if a.variant_tokens and b.variant_tokens:
        if set(a.variant_tokens) == set(b.variant_tokens):
            return 1.0
        if set(a.variant_tokens) & set(b.variant_tokens):
            return 0.6
        return 0.0
    if a.variant_tokens or b.variant_tokens:
        return 0.35
    return 0.7


def packaging_score(a: ParsedProduct, b: ParsedProduct) -> float:
    if a.packaging and b.packaging:
        return 1.0 if a.packaging == b.packaging else 0.0
    if a.packaging or b.packaging:
        return 0.5
    return 0.7


def unit_price_penalty(a: ParsedProduct, b: ParsedProduct) -> float:
    pa = a.raw.get("price_sgd")
    pb = b.raw.get("price_sgd")
    sa, sb = a.size_total_value, b.size_total_value
    if not pa or not pb or not sa or not sb:
        return 0.0
    if a.size_base_unit != b.size_base_unit:
        return 0.0
    upa = float(pa) / sa
    upb = float(pb) / sb
    if min(upa, upb) == 0:
        return 0.0
    ratio = max(upa, upb) / min(upa, upb)
    if ratio > 4:
        return 0.18
    if ratio > 2.5:
        return 0.10
    return 0.0


def conflicting_variant(a: ParsedProduct, b: ParsedProduct) -> bool:
    if not a.variant_tokens or not b.variant_tokens:
        return False
    return set(a.variant_tokens).isdisjoint(set(b.variant_tokens))


def score_pair(a: ParsedProduct, b: ParsedProduct) -> dict[str, Any]:
    b_score = brand_score(a, b)
    s_score = size_score(a, b)
    t_score = title_score(a, b)
    v_score = variant_score(a, b)
    p_score = packaging_score(a, b)

    # Adjusted weights: title + size matter more than brand for meat/produce
    score = (
        0.20 * b_score
        + 0.30 * s_score
        + 0.35 * t_score
        + 0.10 * v_score
        + 0.05 * p_score
    )

    penalties = []
    if b_score == 0.0:
        penalties.append(("brand_conflict", 0.20))   # softer than beverages (0.25)
    if s_score == 0.0 and a.size_total_value is not None and b.size_total_value is not None:
        penalties.append(("size_conflict", 0.25))
    if conflicting_variant(a, b):
        penalties.append(("variant_conflict", 0.25))  # stronger: boneless vs bone-in = different product
    if p_score == 0.0:
        penalties.append(("packaging_conflict", 0.05))
    if t_score < 0.65:
        penalties.append(("low_title_similarity", 0.15))

    up_penalty = unit_price_penalty(a, b)
    if up_penalty:
        penalties.append(("unit_price_outlier", up_penalty))

    total_penalty = sum(p for _, p in penalties)
    final = max(0.0, min(1.0, score - total_penalty))

    # Slightly looser threshold than beverages — meat names vary more
    if final >= 0.90 and t_score >= 0.78:
        status = "strong_match"
    elif final >= 0.78:
        status = "review"
    else:
        status = "no_match"

    explanation_bits = [
        f"brand={b_score:.2f}", f"size={s_score:.2f}", f"title={t_score:.2f}",
        f"variant={v_score:.2f}", f"packaging={p_score:.2f}",
    ]
    if penalties:
        explanation_bits.append(
            "penalties=" + ",".join(f"{name}:{val:.2f}" for name, val in penalties)
        )

    return {
        "brand_score": round(b_score, 4),
        "size_score": round(s_score, 4),
        "title_score": round(t_score, 4),
        "variant_score": round(v_score, 4),
        "packaging_score": round(p_score, 4),
        "match_score": round(final, 4),
        "match_status": status,
        "explanation": "; ".join(explanation_bits),
    }


def likely_candidate(a: ParsedProduct, b: ParsedProduct) -> bool:
    if a.raw["store"] == b.raw["store"]:
        return False
    if a.normalized_brand and b.normalized_brand and brand_score(a, b) == 0.0:
        return False
    if a.size_total_value and b.size_total_value and a.size_base_unit == b.size_base_unit:
        larger = max(a.size_total_value, b.size_total_value)
        smaller = min(a.size_total_value, b.size_total_value)
        if larger and smaller / larger < 0.6:  # looser than beverages (0.7)
            return False
    shared_core = set(a.core_tokens) & set(b.core_tokens)
    if len(shared_core) == 0 and brand_score(a, b) < 0.8:
        return False
    return True


# ── SUPABASE FETCHING (identical to matching.py) ──────────────────────────────

def get_latest_date_for_store(supabase, store: str, category: str) -> Optional[str]:
    rows = (
        supabase.table("products")
        .select("scraped_at")
        .eq("store", store)
        .eq("unified_category", category)
        .order("scraped_at", desc=True)
        .limit(1)
        .execute()
        .data
    )
    if not rows:
        return None
    return rows[0]["scraped_at"][:10]


def fetch_products_for_store_date(supabase, store: str, category: str, date_str: str) -> list[dict]:
    start = f"{date_str}T00:00:00"
    end = f"{(date.fromisoformat(date_str) + timedelta(days=1)).isoformat()}T00:00:00"
    all_rows = []
    offset = 0
    while True:
        resp = (
            supabase.table("products")
            .select("id,name,brand,price_sgd,original_price_sgd,discount_sgd,unit,unified_category,category_slug,store,product_url,scraped_at")
            .eq("store", store)
            .eq("unified_category", category)
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


# ── GROUP BUILDING & SYNCING (identical to matching.py) ───────────────────────

def choose_canonical_name(group_items: list[ParsedProduct]) -> str:
    best = max(group_items, key=lambda i: (len(i.core_tokens), len(i.raw.get("name") or ""), 1 if i.raw.get("brand") else 0))
    return best.raw.get("name") or "Unknown Product"


def choose_canonical_brand(group_items: list[ParsedProduct]) -> Optional[str]:
    counts: dict[str, int] = defaultdict(int)
    for item in group_items:
        if item.normalized_brand:
            counts[item.normalized_brand] += 1
    if not counts:
        return None
    return max(counts.items(), key=lambda x: (x[1], len(x[0])))[0]


def choose_canonical_size(group_items):
    sized = [i for i in group_items if i.size_total_value is not None and i.size_base_unit]
    if not sized:
        return None, None, None, None
    best = max(sized, key=lambda i: (i.pack_count or 0, i.size_total_value or 0))
    return best.size_total_value, best.size_base_unit, best.size_display, best.pack_count


def build_canonical_key(category, canonical_brand, canonical_name, size_total, size_unit, pack_count, packaging, variant_tokens, group_items) -> str:
    representative_tokens = sorted({token for item in group_items for token in item.core_tokens})[:8]
    signature = {
        "category": slugify(category),
        "brand": slugify(canonical_brand or "unknown"),
        "name": normalize_text(canonical_name),
        "size_total": stable_number_fragment(size_total),
        "size_unit": size_unit or "na",
        "pack_count": pack_count or 0,
        "packaging": packaging or "na",
        "variant_tokens": sorted(variant_tokens),
        "core_tokens": representative_tokens,
    }
    digest = hashlib.sha1(json.dumps(signature, sort_keys=True, ensure_ascii=True).encode()).hexdigest()[:12]
    return f"{slugify(category)}-{slugify(canonical_brand or 'unknown')[:20]}-{digest}"


def build_groups(parsed_products, strong_pairs, category):
    uf = UnionFind()
    for pair in strong_pairs:
        uf.union(pair["product_id_a"], pair["product_id_b"])

    groups: dict[int, list[ParsedProduct]] = defaultdict(list)
    for item in parsed_products:
        root = uf.find(item.raw["id"])
        groups[root].append(item)

    canonical_products = []
    canonical_members = []

    for group_items in sorted(groups.values(), key=lambda g: (-len(g), min(i.raw["id"] for i in g))):
        canonical_name = choose_canonical_name(group_items)
        canonical_brand = choose_canonical_brand(group_items)
        size_total, size_unit, size_display, pack_count = choose_canonical_size(group_items)
        packaging = max((i.packaging for i in group_items if i.packaging), key=lambda p: sum(1 for i in group_items if i.packaging == p), default=None)
        variant_tokens = sorted({v for i in group_items for v in i.variant_tokens})
        canonical_key = build_canonical_key(category, canonical_brand, canonical_name, size_total, size_unit, pack_count, packaging, variant_tokens, group_items)
        first_seen_at = min(i.raw.get("scraped_at") for i in group_items if i.raw.get("scraped_at"))
        last_seen_at = max(i.raw.get("scraped_at") for i in group_items if i.raw.get("scraped_at"))

        canonical_products.append({
            "canonical_key": canonical_key,
            "canonical_name": canonical_name,
            "brand": canonical_brand,
            "unified_category": category,
            "size_total_value": size_total,
            "size_base_unit": size_unit,
            "size_display": size_display,
            "pack_count": pack_count,
            "packaging": packaging,
            "variant_tokens": variant_tokens,
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
                "name": item.raw["name"],
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
            continue
        existing["member_count"] += row["member_count"]
        existing["stores_present"] = sorted(set(existing["stores_present"]) | set(row["stores_present"]))
        existing["source_product_ids"] = sorted(set(existing["source_product_ids"]) | set(row["source_product_ids"]))
        existing["variant_tokens"] = sorted(set(existing["variant_tokens"]) | set(row["variant_tokens"]))
        timestamps = [t for t in [existing["first_seen_at"], row["first_seen_at"]] if t]
        existing["first_seen_at"] = min(timestamps) if timestamps else None
        timestamps = [t for t in [existing["last_seen_at"], row["last_seen_at"]] if t]
        existing["last_seen_at"] = max(timestamps) if timestamps else None
        if len(row["canonical_name"]) > len(existing["canonical_name"]):
            existing["canonical_name"] = row["canonical_name"]

    for row in canonical_members:
        consolidated_members[row["product_id"]] = row

    return list(consolidated_products.values()), list(consolidated_members.values())


def generate_pairwise_matches(parsed_products):
    pairs = []
    items = sorted(parsed_products, key=lambda i: (i.raw["store"], i.raw["id"]))
    for i, left in enumerate(items):
        for right in items[i + 1:]:
            if left.raw["store"] == right.raw["store"]:
                continue
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
                "name_a": left.raw["name"],
                "name_b": right.raw["name"],
                "brand_a": left.normalized_brand,
                "brand_b": right.normalized_brand,
                "size_a": left.size_display,
                "size_b": right.size_display,
                "variant_a": left.variant_tokens,
                "variant_b": right.variant_tokens,
                **scores,
            })
    pairs.sort(key=lambda r: (-r["match_score"], r["store_a"], r["store_b"], r["name_a"]))
    return pairs


def filter_reciprocal_strong_pairs(pair_matches):
    best_by_product_store: dict[tuple[int, str], dict] = {}
    for pair in pair_matches:
        if pair["match_status"] != "strong_match":
            continue
        left_key = (pair["product_id_a"], pair["store_b"])
        right_key = (pair["product_id_b"], pair["store_a"])
        if left_key not in best_by_product_store or pair["match_score"] > best_by_product_store[left_key]["match_score"]:
            best_by_product_store[left_key] = pair
        if right_key not in best_by_product_store or pair["match_score"] > best_by_product_store[right_key]["match_score"]:
            best_by_product_store[right_key] = pair

    reciprocal = []
    for pair in pair_matches:
        if pair["match_status"] != "strong_match":
            continue
        left_key = (pair["product_id_a"], pair["store_b"])
        right_key = (pair["product_id_b"], pair["store_a"])
        left_best = best_by_product_store.get(left_key)
        right_best = best_by_product_store.get(right_key)
        if left_best and right_best:
            if left_best["product_id_a"] == right_best["product_id_b"] and left_best["product_id_b"] == right_best["product_id_a"]:
                reciprocal.append(pair)
    return reciprocal


def save_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def sync_results_to_supabase(supabase, run_key, category, canonical_products, canonical_members, pair_matches):
    # Fetch existing canonical product IDs by canonical_key
    existing = {}
    offset = 0
    while True:
        resp = supabase.table("canonical_products").select("id,canonical_key").range(offset, offset + 999).execute()
        for row in resp.data or []:
            existing[row["canonical_key"]] = row["id"]
        if len(resp.data or []) < 1000:
            break
        offset += 1000

    # Upsert canonical products
    canonical_payloads = []
    for row in canonical_products:
        payload = {k: v for k, v in row.items() if k != "source_product_ids"}
        canonical_payloads.append(payload)

    for i in range(0, len(canonical_payloads), 200):
        supabase.table("canonical_products").upsert(
            canonical_payloads[i:i + 200],
            on_conflict="canonical_key",
            ignore_duplicates=False,
        ).execute()

    # Re-fetch IDs after upsert
    existing = {}
    offset = 0
    while True:
        resp = supabase.table("canonical_products").select("id,canonical_key").range(offset, offset + 999).execute()
        for row in resp.data or []:
            existing[row["canonical_key"]] = row["id"]
        if len(resp.data or []) < 1000:
            break
        offset += 1000

    # Upsert canonical members
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
            "match_source": "rule_based_meat_produce_v1",
            "match_confidence": row.get("match_confidence"),
        })

    for i in range(0, len(member_payloads), 200):
        supabase.table("canonical_product_members").upsert(
            member_payloads[i:i + 200],
            on_conflict="product_id",
            ignore_duplicates=False,
        ).execute()

    # Upsert match candidates
    candidate_payloads = []
    for row in pair_matches:
        a_id = row["product_id_a"]
        b_id = row["product_id_b"]
        if a_id > b_id:
            a_id, b_id = b_id, a_id
        candidate_payloads.append({
            "run_key": run_key,
            "unified_category": category,
            "product_id_a": a_id,
            "product_id_b": b_id,
            "store_a": row["store_a"],
            "store_b": row["store_b"],
            "name_a": row["name_a"],
            "name_b": row["name_b"],
            "brand_a": row.get("brand_a"),
            "brand_b": row.get("brand_b"),
            "size_a": row.get("size_a"),
            "size_b": row.get("size_b"),
            "variant_a": row.get("variant_a", []),
            "variant_b": row.get("variant_b", []),
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
        supabase.table("product_match_candidates").upsert(
            candidate_payloads[i:i + 200],
            on_conflict="run_key,product_id_a,product_id_b",
            ignore_duplicates=True,
        ).execute()

    return {
        "canonical_products_upserted": len(canonical_payloads),
        "canonical_members_upserted": len(member_payloads),
        "match_candidates_upserted": len(candidate_payloads),
    }


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run(category: str) -> dict[str, Any]:
    if category not in SUPPORTED_CATEGORIES:
        raise ValueError(f"Category must be one of: {SUPPORTED_CATEGORIES}")

    supabase = get_client()
    print("=" * 70)
    print(f"Meat/Produce matching: {category}")
    print("=" * 70)

    products: list[ParsedProduct] = []
    store_dates: dict[str, str] = {}

    for store in STORE_ORDER:
        latest_date = get_latest_date_for_store(supabase, store, category)
        if not latest_date:
            print(f"[{store}] No rows found for {category}")
            continue
        store_dates[store] = latest_date
        rows = fetch_products_for_store_date(supabase, store, category, latest_date)
        print(f"[{store}] fetched {len(rows)} rows for {latest_date}")
        for row in rows:
            products.append(parse_product(row, latest_date))

    pair_matches = generate_pairwise_matches(products)
    strong_pairs_raw = [r for r in pair_matches if r["match_status"] == "strong_match"]
    strong_pairs = filter_reciprocal_strong_pairs(pair_matches)
    review_pairs = [r for r in pair_matches if r["match_status"] == "review"]
    canonical_products, canonical_members = build_groups(products, strong_pairs, category)

    latest_dates_slug = "-".join(f"{s}_{d}" for s, d in sorted(store_dates.items()))
    run_key = f"{slugify(category)}-meat-produce-{latest_dates_slug}"
    output_dir = DEFAULT_OUTPUT_BASE / slugify(category) / latest_dates_slug

    summary = {
        "run_key": run_key,
        "category": category,
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
    save_json(output_dir / "summary.json", summary)

    sync_summary = sync_results_to_supabase(
        supabase, run_key, category,
        canonical_products, canonical_members,
        strong_pairs + review_pairs,
    )
    summary["supabase_sync"] = sync_summary
    save_json(output_dir / "summary.json", summary)

    print(f"\nSaved outputs to: {output_dir}")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 pipeline/meat_produce_matching.py <category>")
        print(f"  Categories: {SUPPORTED_CATEGORIES}")
        sys.exit(1)
    run(sys.argv[1])
