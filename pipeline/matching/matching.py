import json
import os
import re
import hashlib
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
DEFAULT_CATEGORY = "Beverages"
SUPPORTED_CATEGORIES = [
    "Beverages",
    "Dairy",
    "Staples",
    "Snacks & Confectionery",
    "Bakery & Breakfast",
]
DEFAULT_OUTPUT_BASE = Path("data") / "matching"
FETCH_PAGE_SIZE = 1000

STOPWORDS = {
    "the",
    "and",
    "with",
    "for",
    "plus",
    "free",
    "drink",
    "drinks",
    "beverage",
    "beverages",
    "fresh",
    "premium",
    "selection",
    "pack",
    "packet",
    "bottle",
    "bottles",
    "can",
    "cans",
    "carton",
    "tea",
    "bags",
    "bag",
    "instant",
    "case",
    "ctn",
    "per",
}

PACKAGING_TERMS = {
    "bottle": {"bottle", "bottles", "btl", "pet"},
    "can": {"can", "cans"},
    "carton": {"carton", "cartons", "uht", "box"},
    "packet": {"packet", "packets", "pkt", "packs", "pack"},
    "sachet": {"sachet", "sachets", "sticks", "stick"},
    "tea_bag": {"tea", "teabag", "teabags", "bags", "bag"},
}

VARIANT_GROUPS = [
    {"zero", "zero sugar", "zs"},
    {"original", "orig", "classic"},
    {"less sugar", "reduced sugar", "light"},
    {"unsweetened", "no sugar", "plain"},
    {"chocolate", "cocoa", "malt", "choc"},
    {"vanilla"},
    {"strawberry"},
    {"lychee"},
    {"lemon"},
    {"lime"},
    {"orange"},
    {"grape"},
    {"apple"},
    {"peach"},
    {"mango"},
    {"jasmine"},
    {"green tea"},
    {"oolong"},
    {"houjicha", "roasted tea"},
    {"barley"},
    {"decaf", "decaffeinated"},
    {"hazelnut"},
    {"soy"},
    {"almond"},
    {"oat"},
]

BRAND_ALIASES = {
    "coca cola": "coke",
    "coca-cola": "coke",
    "100 plus": "100plus",
    "fairprice": "fairprice",
    "f&n": "fn",
    "f n": "fn",
    "pokka": "pokka",
    "milo": "milo",
    "nescafe": "nescafe",
    "nestle": "nestle",
    "marigold": "marigold",
    "schweppes": "schweppes",
    "sanitarium": "sanitarium",
}


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
        ra = self.find(a)
        rb = self.find(b)
        if ra != rb:
            self.parent[rb] = ra


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


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = value.lower()
    text = text.replace("×", " x ")
    text = text.replace("&", " and ")
    text = re.sub(r"[\(\)\[\],/+]", " ", text)
    text = re.sub(r"[^a-z0-9\.\s]", " ", text)
    return normalize_whitespace(text)


def normalize_brand(value: Optional[str], name: str) -> Optional[str]:
    brand = normalize_text(value)
    if not brand:
        name_tokens = normalize_text(name).split()
        if not name_tokens:
            return None
        brand = name_tokens[0]
        if len(name_tokens) > 1 and name_tokens[0] in {"100", "old", "mr", "dr"}:
            brand = f"{name_tokens[0]} {name_tokens[1]}"
    return BRAND_ALIASES.get(brand, brand) or None


def tokenize(value: str) -> list[str]:
    return [tok for tok in normalize_text(value).split() if tok]


def canonical_variant(token_phrase: str) -> str:
    for group in VARIANT_GROUPS:
        if token_phrase in group:
            return sorted(group)[0]
    return token_phrase


def extract_variant_tokens(name: str) -> list[str]:
    normalized = normalize_text(name)
    found: set[str] = set()

    for group in VARIANT_GROUPS:
        for phrase in sorted(group, key=len, reverse=True):
            if re.search(rf"(^|\s){re.escape(phrase)}($|\s)", normalized):
                found.add(sorted(group)[0])

    return sorted(found)


def extract_packaging(tokens: list[str]) -> Optional[str]:
    token_set = set(tokens)
    for label, variants in PACKAGING_TERMS.items():
        if token_set & variants:
            return label
    return None


def parse_size_from_text(
    text: str,
) -> tuple[Optional[float], Optional[str], Optional[int], Optional[str], Optional[float]]:
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
            round(base_value * count, 2),
            base_unit,
            count,
            f"{count} x {value:g}{unit}",
            round(base_value, 2),
        )

    single = re.search(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ml|l|g|kg)", normalized)
    if single:
        value = float(single.group("value"))
        unit = single.group("unit")
        base_value, base_unit = convert_to_base(value, unit)
        return round(base_value, 2), base_unit, 1, f"{value:g}{unit}", round(base_value, 2)

    count_only = re.search(r"(?P<count>\d+)\s*s\b", normalized)
    if count_only:
        count = int(count_only.group("count"))
        return float(count), "count", count, f"{count}s", 1.0

    return None, None, None, None, None


def convert_to_base(value: float, unit: str) -> tuple[float, str]:
    unit = unit.lower()
    if unit == "l":
        return value * 1000, "ml"
    if unit == "kg":
        return value * 1000, "g"
    return value, unit


def build_core_tokens(tokens: list[str], brand: Optional[str]) -> list[str]:
    brand_parts = set(tokenize(brand or ""))
    core: list[str] = []
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
    size_total, size_unit, pack_count, size_display, size_each_value = parse_size_from_text(
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
        size_each_value=size_each_value,
        latest_date=latest_date,
    )


def brand_score(a: ParsedProduct, b: ParsedProduct) -> float:
    if a.normalized_brand and b.normalized_brand:
        if a.normalized_brand == b.normalized_brand:
            return 1.0
        a_parts = set(tokenize(a.normalized_brand))
        b_parts = set(tokenize(b.normalized_brand))
        if a_parts & b_parts:
            return 0.8
        return 0.0
    if a.normalized_brand or b.normalized_brand:
        return 0.45
    return 0.3


def size_score(a: ParsedProduct, b: ParsedProduct) -> float:
    if a.size_total_value is None or b.size_total_value is None:
        return 0.55
    if a.size_base_unit != b.size_base_unit:
        return 0.0

    if a.pack_count and b.pack_count and a.pack_count != b.pack_count:
        if a.pack_count > 1 or b.pack_count > 1:
            return 0.0

    if a.size_each_value is not None and b.size_each_value is not None:
        each_larger = max(a.size_each_value, b.size_each_value)
        each_smaller = min(a.size_each_value, b.size_each_value)
        if each_larger == 0:
            return 0.0
        each_ratio = abs(each_larger - each_smaller) / each_larger
        if each_ratio > 0.03:
            return 0.0

    larger = max(a.size_total_value, b.size_total_value)
    smaller = min(a.size_total_value, b.size_total_value)
    if larger == 0:
        return 0.0
    ratio = abs(larger - smaller) / larger
    if ratio <= 0.02:
        return 1.0
    if ratio <= 0.05:
        return 0.88
    if ratio <= 0.12:
        return 0.7
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
    sa = a.size_total_value
    sb = b.size_total_value
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
        return 0.1
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

    score = (
        0.30 * b_score
        + 0.30 * s_score
        + 0.25 * t_score
        + 0.10 * v_score
        + 0.05 * p_score
    )

    penalties = []
    if b_score == 0.0:
        penalties.append(("brand_conflict", 0.25))
    if s_score == 0.0 and a.size_total_value is not None and b.size_total_value is not None:
        penalties.append(("size_conflict", 0.25))
    if conflicting_variant(a, b):
        penalties.append(("variant_conflict", 0.2))
    if p_score == 0.0:
        penalties.append(("packaging_conflict", 0.08))
    if t_score < 0.68:
        penalties.append(("low_title_similarity", 0.12))

    up_penalty = unit_price_penalty(a, b)
    if up_penalty:
        penalties.append(("unit_price_outlier", up_penalty))

    total_penalty = sum(p for _, p in penalties)
    final = max(0.0, min(1.0, score - total_penalty))

    if final >= 0.93 and t_score >= 0.82:
        status = "strong_match"
    elif final >= 0.80:
        status = "review"
    else:
        status = "no_match"

    explanation_bits = [
        f"brand={b_score:.2f}",
        f"size={s_score:.2f}",
        f"title={t_score:.2f}",
        f"variant={v_score:.2f}",
        f"packaging={p_score:.2f}",
    ]
    if penalties:
        explanation_bits.append(
            "penalties=" + ",".join(f"{name}:{value:.2f}" for name, value in penalties)
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
        if larger and smaller / larger < 0.7:
            return False
        if a.pack_count and b.pack_count and a.pack_count != b.pack_count:
            if a.pack_count > 1 or b.pack_count > 1:
                return False

    shared_core = set(a.core_tokens) & set(b.core_tokens)
    if len(shared_core) == 0 and brand_score(a, b) < 0.8:
        return False
    if len(shared_core) < 2 and brand_score(a, b) < 1.0:
        return False

    return True


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


def fetch_products_for_store_date(
    supabase, store: str, category: str, date_str: str
) -> list[dict[str, Any]]:
    start = f"{date_str}T00:00:00"
    end = f"{(date.fromisoformat(date_str) + timedelta(days=1)).isoformat()}T00:00:00"
    all_rows: list[dict[str, Any]] = []
    offset = 0

    while True:
        resp = (
            supabase.table("products")
            .select(
                "id,name,brand,price_sgd,original_price_sgd,discount_sgd,unit,unified_category,category_slug,store,product_url,scraped_at"
            )
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


def choose_canonical_name(group_items: list[ParsedProduct]) -> str:
    best = max(
        group_items,
        key=lambda item: (
            len(item.core_tokens),
            len(item.raw.get("name") or ""),
            1 if item.raw.get("brand") else 0,
        ),
    )
    return best.raw.get("name") or "Unknown Product"


def choose_canonical_brand(group_items: list[ParsedProduct]) -> Optional[str]:
    counts: dict[str, int] = defaultdict(int)
    for item in group_items:
        if item.normalized_brand:
            counts[item.normalized_brand] += 1
    if not counts:
        return None
    return max(counts.items(), key=lambda x: (x[1], len(x[0])))[0]


def choose_canonical_size(
    group_items: list[ParsedProduct],
) -> tuple[Optional[float], Optional[str], Optional[str], Optional[int]]:
    sized = [i for i in group_items if i.size_total_value is not None and i.size_base_unit]
    if not sized:
        return None, None, None, None
    best = max(sized, key=lambda i: (i.pack_count or 0, i.size_total_value or 0))
    return best.size_total_value, best.size_base_unit, best.size_display, best.pack_count


def build_canonical_key(
    category: str,
    canonical_brand: Optional[str],
    canonical_name: str,
    size_total: Optional[float],
    size_unit: Optional[str],
    pack_count: Optional[int],
    packaging: Optional[str],
    variant_tokens: list[str],
    group_items: list[ParsedProduct],
) -> str:
    representative_tokens = sorted(
        {token for item in group_items for token in item.core_tokens}
    )[:8]
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
    digest = hashlib.sha1(
        json.dumps(signature, sort_keys=True, ensure_ascii=True).encode("utf-8")
    ).hexdigest()[:12]
    brand_part = slugify(canonical_brand or "unknown")[:20]
    return f"{slugify(category)}-{brand_part}-{digest}"


def build_groups(
    parsed_products: list[ParsedProduct],
    strong_pairs: list[dict[str, Any]],
    category: str,
):
    uf = UnionFind()
    for pair in strong_pairs:
        uf.union(pair["product_id_a"], pair["product_id_b"])

    groups: dict[int, list[ParsedProduct]] = defaultdict(list)
    for item in parsed_products:
        root = uf.find(item.raw["id"])
        groups[root].append(item)

    canonical_products = []
    canonical_members = []
    sorted_groups = sorted(groups.values(), key=lambda items: (-len(items), min(i.raw["id"] for i in items)))

    for group_items in sorted_groups:
        canonical_name = choose_canonical_name(group_items)
        canonical_brand = choose_canonical_brand(group_items)
        size_total, size_unit, size_display, pack_count = choose_canonical_size(group_items)
        packaging = max(
            (i.packaging for i in group_items if i.packaging),
            key=lambda p: sum(1 for i in group_items if i.packaging == p),
            default=None,
        )
        variant_tokens = sorted({v for i in group_items for v in i.variant_tokens})
        canonical_key = build_canonical_key(
            category,
            canonical_brand,
            canonical_name,
            size_total,
            size_unit,
            pack_count,
            packaging,
            variant_tokens,
            group_items,
        )
        first_seen_at = min(i.raw.get("scraped_at") for i in group_items if i.raw.get("scraped_at"))
        last_seen_at = max(i.raw.get("scraped_at") for i in group_items if i.raw.get("scraped_at"))

        canonical_products.append(
            {
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
            }
        )

        for item in sorted(group_items, key=lambda x: (x.raw["store"], x.raw["id"])):
            canonical_members.append(
                {
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
                }
            )

    consolidated_products: dict[str, dict[str, Any]] = {}
    consolidated_members: dict[int, dict[str, Any]] = {}

    for row in canonical_products:
        existing = consolidated_products.get(row["canonical_key"])
        if existing is None:
            consolidated_products[row["canonical_key"]] = row
            continue

        existing["member_count"] += row["member_count"]
        existing["stores_present"] = sorted(
            set(existing["stores_present"]) | set(row["stores_present"])
        )
        existing["source_product_ids"] = sorted(
            set(existing["source_product_ids"]) | set(row["source_product_ids"])
        )
        existing["variant_tokens"] = sorted(
            set(existing["variant_tokens"]) | set(row["variant_tokens"])
        )
        timestamps = [t for t in [existing["first_seen_at"], row["first_seen_at"]] if t]
        existing["first_seen_at"] = min(timestamps) if timestamps else None
        timestamps = [t for t in [existing["last_seen_at"], row["last_seen_at"]] if t]
        existing["last_seen_at"] = max(timestamps) if timestamps else None
        if len(row["canonical_name"]) > len(existing["canonical_name"]):
            existing["canonical_name"] = row["canonical_name"]

    for row in canonical_members:
        consolidated_members[row["product_id"]] = row

    return list(consolidated_products.values()), list(consolidated_members.values())


def generate_pairwise_matches(parsed_products: list[ParsedProduct]) -> list[dict[str, Any]]:
    pairs = []
    items = sorted(parsed_products, key=lambda item: (item.raw["store"], item.raw["id"]))

    for i, left in enumerate(items):
        for right in items[i + 1:]:
            if left.raw["store"] == right.raw["store"]:
                continue
            if not likely_candidate(left, right):
                continue

            scores = score_pair(left, right)
            if scores["match_status"] == "no_match":
                continue

            pairs.append(
                {
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
                }
            )

    pairs.sort(key=lambda row: (-row["match_score"], row["store_a"], row["store_b"], row["name_a"]))
    return pairs


def filter_reciprocal_strong_pairs(pair_matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_product_store: dict[tuple[int, str], dict[str, Any]] = {}

    for pair in pair_matches:
        if pair["match_status"] != "strong_match":
            continue

        left_key = (pair["product_id_a"], pair["store_b"])
        right_key = (pair["product_id_b"], pair["store_a"])

        left_best = best_by_product_store.get(left_key)
        if left_best is None or pair["match_score"] > left_best["match_score"]:
            best_by_product_store[left_key] = pair

        right_best = best_by_product_store.get(right_key)
        if right_best is None or pair["match_score"] > right_best["match_score"]:
            best_by_product_store[right_key] = pair

    filtered = []
    seen: set[tuple[int, int]] = set()
    for pair in pair_matches:
        if pair["match_status"] != "strong_match":
            continue
        left_key = (pair["product_id_a"], pair["store_b"])
        right_key = (pair["product_id_b"], pair["store_a"])
        if best_by_product_store.get(left_key) is not pair:
            continue
        if best_by_product_store.get(right_key) is not pair:
            continue
        edge = tuple(sorted((pair["product_id_a"], pair["product_id_b"])))
        if edge in seen:
            continue
        seen.add(edge)
        filtered.append(pair)

    filtered.sort(
        key=lambda row: (-row["match_score"], row["store_a"], row["store_b"], row["name_a"])
    )
    return filtered


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def table_exists(supabase, table_name: str) -> bool:
    try:
        supabase.table(table_name).select("*").limit(1).execute()
        return True
    except Exception:
        return False


def batched(rows: list[dict[str, Any]], batch_size: int = 200):
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]


def sync_results_to_supabase(
    supabase,
    run_key: str,
    category: str,
    canonical_products: list[dict[str, Any]],
    canonical_members: list[dict[str, Any]],
    pair_matches: list[dict[str, Any]],
) -> dict[str, Any]:
    required_tables = [
        "canonical_products",
        "canonical_product_members",
        "product_match_candidates",
    ]
    missing_tables = [name for name in required_tables if not table_exists(supabase, name)]
    if missing_tables:
        return {
            "synced": False,
            "missing_tables": missing_tables,
        }

    canonical_payloads = [
        {
            "canonical_key": row["canonical_key"],
            "canonical_name": row["canonical_name"],
            "brand": row["brand"],
            "unified_category": row["unified_category"],
            "size_total_value": row["size_total_value"],
            "size_base_unit": row["size_base_unit"],
            "size_display": row["size_display"],
            "pack_count": row["pack_count"],
            "packaging": row["packaging"],
            "variant_tokens": row["variant_tokens"],
            "member_count": row["member_count"],
            "stores_present": row["stores_present"],
            "first_seen_at": row["first_seen_at"],
            "last_seen_at": row["last_seen_at"],
            "updated_at": datetime.now().isoformat(),
        }
        for row in canonical_products
    ]
    for batch in batched(canonical_payloads):
        supabase.table("canonical_products").upsert(
            batch,
            on_conflict="canonical_key",
        ).execute()

    canonical_id_map: dict[str, int] = {}
    canonical_keys = [row["canonical_key"] for row in canonical_products]
    for batch_keys in batched([{"canonical_key": key} for key in canonical_keys], batch_size=100):
        keys_only = [row["canonical_key"] for row in batch_keys]
        fetched = (
            supabase.table("canonical_products")
            .select("id,canonical_key")
            .in_("canonical_key", keys_only)
            .execute()
            .data
        )
        for row in fetched or []:
            canonical_id_map[row["canonical_key"]] = row["id"]

    member_payloads = []
    for row in canonical_members:
        canonical_product_id = canonical_id_map.get(row["canonical_key"])
        if canonical_product_id is None:
            continue
        member_payloads.append(
            {
                "canonical_product_id": canonical_product_id,
                "product_id": row["product_id"],
                "store": row["store"],
                "name": row["name"],
                "brand": row["brand"],
                "price_sgd": row["price_sgd"],
                "unit": row["unit"],
                "product_url": row["product_url"],
                "scraped_at": row["scraped_at"],
                "run_key": run_key,
                "match_source": "rule_based_v1",
                "match_confidence": row.get("match_confidence"),
            }
        )
    for batch in batched(member_payloads):
        supabase.table("canonical_product_members").upsert(
            batch,
            on_conflict="product_id",
        ).execute()

    candidate_payloads = [
        {
            "run_key": run_key,
            "unified_category": category,
            **row,
        }
        for row in pair_matches
    ]
    for batch in batched(candidate_payloads):
        supabase.table("product_match_candidates").upsert(
            batch,
            on_conflict="run_key,product_id_a,product_id_b",
        ).execute()

    return {
        "synced": True,
        "canonical_products_upserted": len(canonical_payloads),
        "canonical_members_upserted": len(member_payloads),
        "match_candidates_upserted": len(candidate_payloads),
    }


def run(category: str = DEFAULT_CATEGORY) -> dict[str, Any]:
    if category not in SUPPORTED_CATEGORIES:
        raise ValueError(
            f"matching.py only supports packaged-goods categories: {SUPPORTED_CATEGORIES}. "
            "Use vegetable_produce_matching.py for Fruits & Vegetables and "
            "meat_produce_matching.py for Meat & Seafood."
        )

    supabase = get_client()

    print("=" * 70)
    print(f"Matching category: {category}")
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
    strong_pairs_raw = [row for row in pair_matches if row["match_status"] == "strong_match"]
    strong_pairs = filter_reciprocal_strong_pairs(pair_matches)
    review_pairs = [row for row in pair_matches if row["match_status"] == "review"]
    canonical_products, canonical_members = build_groups(products, strong_pairs, category)

    latest_dates_slug = "-".join(
        f"{store}_{date_str}" for store, date_str in sorted(store_dates.items())
    )
    run_key = f"{slugify(category)}-{latest_dates_slug}"
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
        "cross_store_groups": sum(1 for row in canonical_products if len(row["stores_present"]) > 1),
        "generated_at": datetime.now().isoformat(),
    }

    save_json(output_dir / "strong_match_pairs_raw.json", strong_pairs_raw)
    save_json(output_dir / "strong_match_pairs.json", strong_pairs)
    save_json(output_dir / "review_pairs.json", review_pairs)
    save_json(output_dir / "canonical_products.json", canonical_products)
    save_json(output_dir / "canonical_product_members.json", canonical_members)
    save_json(output_dir / "review_samples_top20.json", {
        "strong_matches": strong_pairs[:20],
        "review_matches": review_pairs[:20],
        "canonical_products": canonical_products[:20],
    })

    sync_summary = sync_results_to_supabase(
        supabase,
        run_key,
        category,
        canonical_products,
        canonical_members,
        strong_pairs + review_pairs,
    )
    summary["supabase_sync"] = sync_summary
    save_json(output_dir / "summary.json", summary)

    print(f"\nSaved review outputs to: {output_dir}")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    import sys

    arg = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CATEGORY
    run(arg)
