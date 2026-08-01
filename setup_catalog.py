#!/usr/bin/env python3
"""
Full Shopizer catalog bootstrap — run once after a fresh database.
Creates: brands → root categories → leaf categories → products.
"""
import json, sys, urllib.request, urllib.error
import os

BASE_URL    = os.environ.get("SHOPIZER_BASE_URL", "http://localhost:8081")
ADMIN_EMAIL = os.environ.get("SHOPIZER_ADMIN_EMAIL", "admin@shopizer.com")
ADMIN_PASS  = os.environ.get("SHOPIZER_ADMIN_PASS", "password")
STORE_CODE  = os.environ.get("SHOPIZER_STORE_CODE", "DEFAULT")
LANG        = os.environ.get("SHOPIZER_LANG", "en")
PAGE_SIZE   = 200
HTTP_TIMEOUT_SECONDS = int(os.environ.get("SHOPIZER_SETUP_HTTP_TIMEOUT_SECONDS", "30"))
MAX_LISTING_PAGES = int(os.environ.get("SHOPIZER_SETUP_MAX_LISTING_PAGES", "25"))
CLEANUP_ENABLED = os.environ.get("SHOPIZER_SETUP_CLEANUP_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
# Load-test catalogs should not exhaust inventory before the system bottlenecks.
DEFAULT_INVENTORY_QTY = 100000
CATEGORY_ID_CACHE = {}

# ── http ──────────────────────────────────────────────────────────────────────

def _call(method, path, body=None, token=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(f"{BASE_URL}{path}", data=data, method=method, headers=headers)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as r:
        raw = r.read()
        return json.loads(raw) if raw else {}

def post(path, body, token=None): return _call("POST", path, body, token)
def get(path, token=None):        return _call("GET",  path, None, token)

def login():
    return post("/api/v1/private/login", {"username": ADMIN_EMAIL, "password": ADMIN_PASS})["token"]

def delete(path, token):
    req = urllib.request.Request(f"{BASE_URL}{path}", method="DELETE")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as r:
        return r.status

def announce_step(label):
    print(f"\n→ Starting {label}...")

def find_category_id(code, token):
    cached = CATEGORY_ID_CACHE.get(code)
    if cached is not None:
        return cached
    try:
        resp = get(f"/api/v1/category?lang={LANG}&store={STORE_CODE}&count={PAGE_SIZE}", token)
        for c in resp.get("categories", []):
            ccode = c.get("code")
            cid = c.get("id")
            if ccode and cid is not None:
                CATEGORY_ID_CACHE[ccode] = cid
        return CATEGORY_ID_CACHE.get(code)
    except Exception:
        pass
    return None

def manufacturer_exists(code, token):
    try:
        resp = get(f"/api/v1/private/manufacturer/unique?code={code}", token)
        return bool(resp.get("exists"))
    except Exception:
        return False

def product_exists(sku, token):
    try:
        resp = get(f"/api/v1/private/product/unique?code={sku}", token)
        return bool(resp.get("exists"))
    except Exception:
        return False

def get_product_by_sku(sku):
    try:
        return get(f"/api/v2/product/{sku}?lang={LANG}&store={STORE_CODE}")
    except Exception:
        return None

def find_product_id_by_sku(sku, token=None):
    product = get_product_by_sku(sku)
    if product and product.get("id") is not None:
        return product.get("id")
    if token and product_exists(sku, token):
        return "exists"
    return None

def find_manufacturer_id(code, token):
    page = 0
    while True:
        try:
            resp = get(f"/api/v1/private/manufacturers?page={page}&count={PAGE_SIZE}", token)
        except Exception:
            return None
        manufacturers = resp.get("manufacturers", [])
        for manufacturer in manufacturers:
            if manufacturer.get("code") == code:
                return manufacturer.get("id")
        if len(manufacturers) < PAGE_SIZE:
            break
        page += 1
    return None

def _is_duplicate_error(body):
    lowered = (body or "").lower()
    return "duplicate entry" in lowered or "already exists" in lowered

def list_public_products(page=0, count=PAGE_SIZE, category_id=None):
    path = f"/api/v1/products?page={page}&count={count}&lang={LANG}&store={STORE_CODE}"
    if category_id is not None:
        path += f"&category={category_id}"
    resp = get(path)
    return resp.get("products", [])

def list_public_products_by_category_slug(category_slug, page=0, count=PAGE_SIZE):
    path = (
        f"/api/v2/products/category/{category_slug}"
        f"?page={page}&count={count}&lang={LANG}&store={STORE_CODE}"
    )
    resp = get(path)
    return resp.get("products", [])

def collect_public_products_by_category_id(category_id):
    page = 0
    out = []
    while page < MAX_LISTING_PAGES:
        batch = list_public_products(page=page, category_id=category_id)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    else:
        print(f"  ! category id={category_id} listing reached page cap ({MAX_LISTING_PAGES}); continuing with partial results")
    return out

def collect_public_products_for_category_slug(category_slug):
    page = 0
    out = []
    while page < MAX_LISTING_PAGES:
        batch = list_public_products_by_category_slug(category_slug, page=page)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    else:
        print(f"  ! category {category_slug} slug listing reached page cap ({MAX_LISTING_PAGES}); continuing with partial results")
    return out

def _dedupe_public_products(products):
    deduped = {}
    for product in products:
        sku = product.get("sku")
        if sku:
            deduped[sku] = product
        else:
            deduped[f"id:{product.get('id')}"] = product
    return list(deduped.values())

def collect_public_products(category_id=None):
    if category_id is not None:
        return collect_public_products_by_category_id(category_id)

    global_listing = []
    page = 0
    while page < MAX_LISTING_PAGES:
        batch = list_public_products(page=page)
        if not batch:
            break
        global_listing.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        page += 1
    else:
        print(f"  ! global product listing reached page cap ({MAX_LISTING_PAGES}); continuing with partial results")

    if global_listing:
        return _dedupe_public_products(global_listing)

    # Some deployments expose an empty global /api/v1/products listing even
    # though category listings are populated. Fall back to category-scoped
    # public listings before concluding the catalog is missing.
    category_scoped = []
    for category_code in sorted(MANAGED_CATEGORY_CODES):
        category_id = find_category_id(category_code, None)
        if category_id:
            category_scoped.extend(collect_public_products_by_category_id(category_id))

    if category_scoped:
        return _dedupe_public_products(category_scoped)

    # Last resort: storefront browse path by category slug.
    category_slug_scoped = []
    for category_code in sorted(MANAGED_CATEGORY_CODES):
        category_slug_scoped.extend(collect_public_products_for_category_slug(category_code))
    return _dedupe_public_products(category_slug_scoped)

def collect_managed_products_for_cleanup(token):
    managed = {}
    for category_code in sorted(MANAGED_CATEGORY_CODES):
        category_id = find_category_id(category_code, None)
        if not category_id:
            print(f"  ! {category_code:<20} category not found")
            continue

        print(f"  · scanning {category_code:<20} (id={category_id})", flush=True)
        try:
            visible = collect_public_products_by_category_id(category_id)
        except Exception as e:
            print(f"    ! category-id scan failed for {category_code}: {type(e).__name__}: {e}")
            visible = []
        if not visible:
            try:
                visible = collect_public_products_for_category_slug(category_code)
            except Exception as e:
                print(f"    ! category-slug scan failed for {category_code}: {type(e).__name__}: {e}")
                visible = []

        for product in visible:
            sku = product.get("sku")
            if sku in MANAGED_SKUS:
                managed[sku] = product

        print(f"    found {len(visible)} public product(s) in {category_code}")

    missing_skus = sorted(MANAGED_SKUS - set(managed.keys()))
    if missing_skus:
        print(f"  · direct SKU lookup for {len(missing_skus)} managed SKU(s) not found in public listings")
    recovered = 0
    unresolved = 0
    for i, sku in enumerate(missing_skus, 1):
        pid = find_product_id_by_sku(sku, token)
        if pid:
            managed[sku] = {"sku": sku, "id": None if pid == "exists" else pid}
            recovered += 1
        else:
            unresolved += 1
        if i % 50 == 0 or i == len(missing_skus):
            print(f"    checked {i}/{len(missing_skus)} missing SKU(s) via direct lookup")

    if recovered:
        print(f"  ↩ recovered {recovered} managed product(s) via direct SKU lookup")
    if unresolved:
        print(f"  ! still could not resolve {unresolved} managed SKU(s) by direct lookup")

    return list(managed.values())

# ── catalog definitions ───────────────────────────────────────────────────────

BRANDS = [
    ("urbanthread", "UrbanThread"),
    ("nordstyle",   "NordStyle"),
    ("velourex",    "Velourex"),
    ("casualco",    "CasualCo"),
    ("steetpeak",   "SteetPeak"),
]

ROOT_CATS = [
    ("men",         "Men"),
    ("women",       "Women"),
    ("accessories", "Accessories"),
]

LEAF_CATS = [
    ("men-tshirts",   "T-Shirts", "men"),
    ("men-jeans",     "Jeans",    "men"),
    ("men-jackets",   "Jackets",  "men"),
    ("men-hoodies",   "Hoodies",  "men"),
    ("women-dresses", "Dresses",  "women"),
    ("women-tops",    "Tops",     "women"),
    ("women-jeans",   "Jeans",    "women"),
    ("women-jackets", "Jackets",  "women"),
    ("bags",          "Bags",     "accessories"),
    ("hats",          "Hats",     "accessories"),
    ("belts",         "Belts",    "accessories"),
]

def p(sku, name, desc, price, cat, brand, qty=DEFAULT_INVENTORY_QTY):
    return {
        "sku": sku, "available": True, "visible": True, "productShipeable": True,
        "manufacturer": {"code": brand},
        "categories": [{"code": cat}],
        "inventory": {
            "sku": f"{sku}-inv", "quantity": qty,
            "price": {"price": price, "defaultPrice": True, "code": "DEFAULT_PRICE"},
        },
        "descriptions": [{"language": "en", "name": name, "description": desc, "friendlyUrl": sku}],
    }

PRODUCTS = [
    # ── Men's T-Shirts ────────────────────────────────────────────────────────
    p("mt-classic-crew",     "Classic Crew Tee",
      "A timeless crew neck in 100% combed cotton. The foundation of any wardrobe.",
      19.99, "men-tshirts", "casualco"),
    p("mt-vintage-wash",     "Vintage Wash Crew Tee",
      "Garment-dyed for a broken-in feel from day one. Relaxed crew neck, 100% ring-spun cotton.",
      22.99, "men-tshirts", "urbanthread"),
    p("mt-oversized-drop",   "Oversized Drop Shoulder Tee",
      "Street-ready silhouette with dropped shoulders and a boxy body. Tonal chest hit.",
      27.99, "men-tshirts", "steetpeak"),
    p("mt-acid-wash",        "Acid Wash Street Tee",
      "Each piece is uniquely faded thanks to a reactive acid-wash process.",
      24.99, "men-tshirts", "steetpeak"),
    p("mt-essential-v",      "Essential V-Neck Tee",
      "The everyday V-neck. Lightweight single-jersey cotton, taped neck seam, clean lines.",
      18.99, "men-tshirts", "casualco"),
    p("mt-longline-black",   "Longline Black Tee",
      "Extended body length hits at the mid-thigh. Curved hem, raw-edge cuffs, heavy 240gsm cotton.",
      29.99, "men-tshirts", "steetpeak"),
    p("mt-henley-grey",      "Grey Henley Neck Tee",
      "Three-button placket gives the classic henley its character. Soft slub cotton.",
      26.99, "men-tshirts", "nordstyle"),
    p("mt-logo-emboss",      "Embossed Logo Tee",
      "Subtle tone-on-tone emboss logo on the chest. Premium pima cotton, pre-washed finish.",
      32.99, "men-tshirts", "urbanthread"),
    p("mt-linen-blend",      "Linen Blend Tee Natural",
      "55% linen / 45% cotton blend keeps you cool in summer. Natural undyed colourway.",
      34.99, "men-tshirts", "nordstyle"),
    p("mt-muscle-fit",       "Muscle Fit White Tee",
      "Contoured for an athletic build. Stretch cotton blend with crew neck.",
      21.99, "men-tshirts", "casualco"),
    p("mt-tie-dye",          "Tie-Dye Surf Tee",
      "Hand-tied and dip-dyed in a surf-inspired blue-green gradient. Soft jersey.",
      28.99, "men-tshirts", "steetpeak"),
    p("mt-thermal-ls",       "Thermal Long Sleeve Tee",
      "Waffle-knit thermal in brushed cotton. Slim crew neck, raglan sleeves, deep navy.",
      38.99, "men-tshirts", "nordstyle"),
    p("mt-raglan-blue",      "Raglan Baseball Tee Blue",
      "Classic raglan cut with contrast 3/4 sleeves. White body, navy sleeves.",
      23.99, "men-tshirts", "casualco"),
    p("mt-abstract-paint",   "Abstract Brushstroke Tee",
      "Front graphic is a large-format abstract oil-paint print.",
      34.99, "men-tshirts", "urbanthread"),
    p("mt-slub-olive",       "Slub Cotton Tee Olive",
      "Irregular slub cotton gives natural texture and depth to this muted olive tone.",
      22.99, "men-tshirts", "nordstyle"),
    p("mt-pima-crew",        "Pima Cotton Crew Tee",
      "Premium long-staple pima cotton, silky smooth handfeel, reinforced shoulder tape.",
      28.99, "men-tshirts", "urbanthread"),
    p("mt-mesh-training",    "Mesh Training Tee",
      "Breathable polyester mesh panels, moisture-wicking finish, reflective hem print.",
      29.99, "men-tshirts", "steetpeak"),
    p("mt-boxy-heavy",       "Boxy Fit Heavy Tee",
      "280gsm heavyweight cotton, boxy cut, twin-needle hem, boxed-in chest embroidery.",
      32.99, "men-tshirts", "steetpeak"),
    p("mt-mock-neck",        "Mock Neck Tee Black",
      "Short mock neck collar, slim torso, ribbed cuffs. Minimal Scandinavian cut.",
      26.99, "men-tshirts", "nordstyle"),
    p("mt-waffle-henley",    "Waffle Henley Tee",
      "Thermal waffle-knit texture, three-button placket, slightly boxy cut.",
      32.99, "men-tshirts", "urbanthread"),

    # ── Men's Jeans ───────────────────────────────────────────────────────────
    p("mj-slim-indigo",      "Slim Indigo Jeans",
      "Classic slim-cut in mid-weight indigo denim. 5-pocket, clean finish.",
      59.99, "men-jeans", "urbanthread"),
    p("mj-relaxed-taper",    "Relaxed Taper Jeans Ecru",
      "Relaxed through the thigh, tapered below the knee. Raw ecru selvedge-style finish.",
      69.99, "men-jeans", "casualco"),
    p("mj-washed-black",     "Washed Black Slim Jeans",
      "True-black denim enzyme-washed for softness. Slim straight cut, 5-pocket.",
      74.99, "men-jeans", "urbanthread"),
    p("mj-cargo-denim",      "Cargo Denim Jeans",
      "Six-pocket cargo styling in mid-weight denim. Utility loops, straight-leg silhouette.",
      84.99, "men-jeans", "steetpeak"),
    p("mj-distressed-blue",  "Distressed Blue Jeans",
      "Heavy distressing on thighs and knees. Authentic broken-in look, slim straight cut.",
      79.99, "men-jeans", "steetpeak"),
    p("mj-wide-leg",         "Wide Leg Loose Jeans",
      "High-rise wide leg inspired by 90s denim. Clean seams, generous drape.",
      89.99, "men-jeans", "casualco"),
    p("mj-skinny-stretch",   "Skinny Stretch Jeans",
      "2% elastane for stretch comfort in a five-pocket skinny silhouette. Deep black.",
      59.99, "men-jeans", "casualco"),
    p("mj-light-wash",       "Light Wash Regular Jeans",
      "Classic light wash denim in a regular straight fit. Faded thighs, clean knee.",
      67.99, "men-jeans", "urbanthread"),
    p("mj-selvedge",         "Selvedge Straight Jeans",
      "Japanese 14oz selvedge denim, sanforized, raw indigo. Straight cut, red ID.",
      129.99, "men-jeans", "velourex"),
    p("mj-ripped-knee",      "Ripped Knee Skinny Jeans",
      "Heavy blowout rips at the knees. Black-grey wash, skinny, stacked at ankle.",
      72.99, "men-jeans", "steetpeak"),
    p("mj-double-knee",      "Double Knee Work Jeans",
      "Reinforced double knee panel in workwear cut. Medium indigo, straight leg.",
      94.99, "men-jeans", "nordstyle"),
    p("mj-coated-black",     "Coated Black Slim Jeans",
      "PU coating gives these denim-base jeans a sleek matte sheen.",
      84.99, "men-jeans", "velourex"),
    p("mj-biker-denim",      "Biker Denim Jeans",
      "Quilted panels on knees, zip ankle cuffs, super-skinny leg. Dark wash.",
      77.99, "men-jeans", "steetpeak"),
    p("mj-stonewash",        "Stonewash Straight Jeans",
      "Traditional stonewash delivers a classic uniform fade. Straight cut, relaxed seat.",
      62.99, "men-jeans", "casualco"),
    p("mj-patchwork",        "Patchwork Denim Jeans",
      "Handcrafted patchwork panels in contrasting indigo and black denim.",
      94.99, "men-jeans", "urbanthread"),
    p("mj-bootcut-mid",      "Bootcut Mid Wash",
      "Classic bootcut leg opening flares slightly from the knee. Mid indigo wash.",
      69.99, "men-jeans", "casualco"),
    p("mj-japanese-selvedge","Japanese Selvedge Denim",
      "Unwashed loomstate Japanese selvedge, 15oz. Fades beautifully with wear.",
      149.99, "men-jeans", "nordstyle"),
    p("mj-loose-90s",        "Loose 90s Fit Jeans",
      "Oversized 90s-inspired loose fit. Sits at natural waist, roomy leg.",
      79.99, "men-jeans", "steetpeak"),
    p("mj-stretch-skinny",   "Stretch Skinny Jeans",
      "3% elastane for all-day comfort. Skinny from hip to ankle, mid blue.",
      59.99, "men-jeans", "urbanthread"),
    p("mj-faded-vintage",    "Faded Vintage Jeans",
      "Factory-faded to look decade-worn. Mid blue with subtle whiskering.",
      74.99, "men-jeans", "casualco"),

    # ── Men's Jackets ─────────────────────────────────────────────────────────
    p("mjk-bomber-navy",     "Navy Bomber Jacket",
      "Classic MA-1 silhouette in navy ripstop nylon. Rib-knit collar and cuffs.",
      99.99, "men-jackets", "steetpeak"),
    p("mjk-denim-trucker",   "Denim Trucker Jacket",
      "Mid-wash denim trucker with chest pockets and a classic boxy fit.",
      89.99, "men-jackets", "casualco"),
    p("mjk-leather-black",   "Black Leather Biker Jacket",
      "Full-grain cowhide with asymmetric zip, snap lapels, quilted shoulder panels.",
      249.99, "men-jackets", "velourex"),
    p("mjk-windbreaker",     "Lightweight Windbreaker",
      "Packable ripstop nylon windbreaker with taped seams and hidden hood.",
      89.99, "men-jackets", "nordstyle"),
    p("mjk-overshirt",       "Brushed Overshirt Jacket",
      "Heavy brushed-cotton overshirt. Chest pockets, snap buttons, boxy fit.",
      79.99, "men-jackets", "casualco"),
    p("mjk-sherpa",          "Sherpa Lined Jacket",
      "Washed cotton shell with full sherpa fleece lining. Trucker-style, button front.",
      119.99, "men-jackets", "nordstyle"),
    p("mjk-parka-olive",     "Olive Military Parka",
      "M65-inspired field parka in olive ripstop. Four flap pockets, drawcord hem.",
      149.99, "men-jackets", "urbanthread"),
    p("mjk-coach-tan",       "Tan Coach Jacket",
      "Coach-collar jacket in washed twill. Two snap pockets, relaxed chest.",
      109.99, "men-jackets", "casualco"),
    p("mjk-harrington",      "Harrington Jacket Beige",
      "Classic Harrington cut in poly-cotton blend. Tartan lining, elastic cuffs.",
      99.99, "men-jackets", "nordstyle"),
    p("mjk-varsity",         "Varsity Letter Jacket",
      "Wool-blend body with leather sleeves in a classic baseball jacket cut.",
      139.99, "men-jackets", "steetpeak"),
    p("mjk-cord-brown",      "Corduroy Jacket Brown",
      "Fine-wale corduroy trucker in warm tobacco brown. Three-panel back, copper buttons.",
      119.99, "men-jackets", "velourex"),
    p("mjk-wax-cotton",      "Wax Cotton Field Jacket",
      "British-style waxed cotton field jacket with storm flap and bellows pockets.",
      169.99, "men-jackets", "nordstyle"),
    p("mjk-blazer-navy",     "Slim Blazer Navy",
      "Single-breasted slim blazer in wool-blend navy. Notch lapel, two-button.",
      179.99, "men-jackets", "velourex"),
    p("mjk-reversible",      "Reversible Puffer Jacket",
      "Two looks — quilted black one side, ripstop red on the other. Recycled fill.",
      159.99, "men-jackets", "urbanthread"),
    p("mjk-track-jacket",    "Track Jacket Black/White",
      "Retro track jacket in contrast-stripe poly. Full zip, stand collar.",
      69.99, "men-jackets", "steetpeak"),
    p("mjk-moleskin",        "Moleskin Field Jacket",
      "Dense cotton moleskin in olive field cut. Five pockets, gun patch, action pleat.",
      134.99, "men-jackets", "nordstyle"),
    p("mjk-wool-topcoat",    "Wool Topcoat Camel",
      "Single-breasted wool-blend topcoat in camel. Notch lapel, knee-length.",
      249.99, "men-jackets", "nordstyle"),
    p("mjk-moto-leather",    "Moto Leather Jacket",
      "Cafe-racer cut cowhide leather moto jacket. Asymmetric zip, zipped cuffs.",
      299.99, "men-jackets", "velourex"),
    p("mjk-military-m65",    "M-65 Field Jacket",
      "Military M-65 silhouette, four utility pockets, drawstring waist, olive drab.",
      139.99, "men-jackets", "steetpeak"),
    p("mjk-denim-sherpa",    "Sherpa-Lined Denim Jacket",
      "Trucker-cut denim jacket with a warm sherpa-fleece body lining.",
      99.99, "men-jackets", "urbanthread"),

    # ── Men's Hoodies ─────────────────────────────────────────────────────────
    p("mh-zip-grey",         "Zip-Up Hoodie Grey",
      "Full-zip marl grey hoodie in mid-weight cotton-poly blend. Two zip pockets.",
      54.99, "men-hoodies", "nordstyle"),
    p("mh-pullover-black",   "Pullover Hoodie Black",
      "Classic pullover in 300gsm loop-back cotton. Deep kangaroo pocket.",
      49.99, "men-hoodies", "casualco"),
    p("mh-vintage-wash",     "Vintage Wash Hoodie",
      "Garment-washed to lived-in softness. Heavy 380gsm fleece, metal zipper.",
      59.99, "men-hoodies", "urbanthread"),
    p("mh-tech-fleece",      "Tech Fleece Hoodie Navy",
      "Engineered spacer fleece keeps warmth without bulk. Slim fit, zip pocket.",
      79.99, "men-hoodies", "steetpeak"),
    p("mh-quarter-zip",      "Quarter-Zip Fleece Hoodie",
      "Grid-pattern midlayer fleece with a quarter zip and low-profile attached hood.",
      69.99, "men-hoodies", "nordstyle"),
    p("mh-oversized-cream",  "Oversized Cream Hoodie",
      "Boxy oversized fit in 350gsm French Terry. Cream colourway, raw hem.",
      74.99, "men-hoodies", "nordstyle"),
    p("mh-essential-forest", "Essential Hoodie Forest",
      "The everyday pullover in muted forest green. 280gsm cotton fleece.",
      54.99, "men-hoodies", "casualco"),
    p("mh-heavyweight",      "Heavyweight Hoodie Charcoal",
      "450gsm double-faced cotton. A substantial hoodie that holds its shape.",
      84.99, "men-hoodies", "urbanthread"),
    p("mh-colorblock",       "Colorblock Hoodie Red/Navy",
      "Two-tone colorblock in bold red and navy. Half-zip, funnel neck.",
      74.99, "men-hoodies", "steetpeak"),
    p("mh-sherpa-hood",      "Sherpa Fleece Hoodie Tan",
      "Full-zip with sherpa fleece lining and sherpa-lined hood. Cosy tan colourway.",
      89.99, "men-hoodies", "nordstyle"),
    p("mh-organic",          "Organic Cotton Hoodie White",
      "GOTS-certified organic cotton fleece in off-white natural. Unbleached, undyed.",
      79.99, "men-hoodies", "nordstyle"),
    p("mh-zip-camel",        "Zip-Up Hoodie Camel",
      "Full-zip in warm camel-toned fleece. Slightly tapered fit, flat-lock seams.",
      64.99, "men-hoodies", "casualco"),
    p("mh-heavy-fleece",     "Heavyweight Fleece Hoodie",
      "500gsm brushed fleece interior hoodie. Oversized cut, kangaroo pocket.",
      74.99, "men-hoodies", "steetpeak"),
    p("mh-tech-zip-black",   "Tech Zip Hoodie Black",
      "Full-zip technical fleece hoodie. Bonded zip pockets, black on black.",
      94.99, "men-hoodies", "velourex"),
    p("mh-embroidered-logo", "Embroidered Logo Hoodie Navy",
      "Chest-embroidered logo hoodie in midweight cotton fleece. Navy.",
      69.99, "men-hoodies", "urbanthread"),
    p("mh-tie-dye-hood",     "Tie-Dye Hoodie Purple",
      "Hand-dyed spiral tie-dye in purple and indigo tones. Unisex fit.",
      69.99, "men-hoodies", "urbanthread"),
    p("mh-cargo-pocket",     "Cargo-Pocket Hoodie",
      "Utility hoodie with flap cargo pockets on the body. Military green.",
      74.99, "men-hoodies", "steetpeak"),
    p("mh-reflective",       "Reflective Print Hoodie",
      "Reflective silver screen print across chest and back. Charcoal body.",
      79.99, "men-hoodies", "velourex"),
    p("mh-thermal-lined",    "Thermal Waffle Lined Hoodie",
      "Fleece exterior, waffle-thermal interior. Engineered for cold commutes.",
      89.99, "men-hoodies", "casualco"),
    p("mh-crop-hoodie",      "Cropped Hoodie Lilac",
      "Above-the-waist crop cut, rib-trim hem. Pastel lilac, soft French terry.",
      59.99, "men-hoodies", "nordstyle"),

    # ── Women's Dresses ───────────────────────────────────────────────────────
    p("wd-midi-floral",      "Floral Midi Dress",
      "Flowy midi with a delicate floral print on cream background. Elasticated waist.",
      79.99, "women-dresses", "casualco"),
    p("wd-wrap-beige",       "Wrap Dress Beige",
      "Classic wrap silhouette in soft beige viscose. Tie waist, knee length.",
      84.99, "women-dresses", "nordstyle"),
    p("wd-shirt-white",      "White Shirt Dress",
      "Crisp cotton poplin shirt dress with a belted waist. Classic midi length.",
      74.99, "women-dresses", "casualco"),
    p("wd-slip-satin",       "Satin Slip Dress Black",
      "Bias-cut satin slip in deep black. Adjustable spaghetti straps, side split.",
      89.99, "women-dresses", "velourex"),
    p("wd-maxi-linen",       "Linen Maxi Dress Sand",
      "Relaxed linen-cotton maxi with button-front bodice and tiered skirt.",
      99.99, "women-dresses", "nordstyle"),
    p("wd-knit-ribbed",      "Ribbed Knit Mini Dress",
      "Body-con ribbed mini in soft viscose-blend knit. Crew neck, long sleeves.",
      79.99, "women-dresses", "velourex"),
    p("wd-smock-floral",     "Smock Floral Dress Blue",
      "Gathered smocked bodice with full midi skirt in a delicate blue floral print.",
      74.99, "women-dresses", "casualco"),
    p("wd-prairie-midi",     "Prairie Midi Dress Sage",
      "Flowy prairie silhouette in sage-green cotton lawn. Ruffle hem, puff sleeve.",
      94.99, "women-dresses", "nordstyle"),
    p("wd-denim-dress",      "Denim Pinafore Dress",
      "Mid-wash denim pinafore. Chest bib, adjustable straps, patch pockets.",
      79.99, "women-dresses", "urbanthread"),
    p("wd-pleated-midi",     "Pleated Satin Midi Dress",
      "Knife-pleated satin midi in dusty rose. V-neck, thin straps, flutter movement.",
      109.99, "women-dresses", "velourex"),
    p("wd-stripe-shirt",     "Stripe Shirt Dress Navy",
      "Relaxed shirt dress in navy/white Breton stripe. Belted waist, rolled cuffs.",
      84.99, "women-dresses", "nordstyle"),
    p("wd-sweater-mini",     "Sweater Mini Dress Cream",
      "Oversized sweater-knit mini in off-white cream. Crew neck, dropped shoulder.",
      89.99, "women-dresses", "nordstyle"),
    p("wd-tennis-white",     "Tennis Pleated Mini Dress",
      "Sporty pleated tennis dress in bright white. Built-in shorts, zip back.",
      69.99, "women-dresses", "steetpeak"),
    p("wd-tiered-maxi",      "Tiered Cotton Maxi Dress",
      "Three-tier cotton-voile maxi in warm white. Cami straps, smocked waist.",
      94.99, "women-dresses", "casualco"),
    p("wd-blazer-dress",     "Blazer Dress Grey",
      "Double-breasted blazer dress in charcoal grey. Belted waist, knee-length.",
      129.99, "women-dresses", "velourex"),
    p("wd-slip-satin-ivory", "Satin Slip Dress Ivory",
      "Bias-cut satin slip with spaghetti straps. Ivory, midi length, cowl back.",
      79.99, "women-dresses", "velourex"),
    p("wd-puff-sleeve",      "Puff Sleeve Mini Dress",
      "Statement puff-sleeve mini in crisp cotton poplin. Smocked waist.",
      69.99, "women-dresses", "casualco"),
    p("wd-knit-sweater",     "Knit Sweater Dress",
      "Soft merino-blend knit, rollneck collar, long sleeves. Cream midi.",
      99.99, "women-dresses", "nordstyle"),
    p("wd-silk-midi",        "Silk Bias Midi",
      "Pure silk bias-cut midi dress in blush pink. Slip strap, V neckline.",
      189.99, "women-dresses", "velourex"),
    p("wd-chiffon-tiered",   "Chiffon Tiered Maxi",
      "Floaty tiered chiffon maxi in dusty pink. Square neck, smocked back.",
      94.99, "women-dresses", "casualco"),

    # ── Women's Tops ──────────────────────────────────────────────────────────
    p("wt-ribbed-tank",      "Ribbed Tank Top",
      "Fitted ribbed vest in stretch cotton-modal blend. Scoop neck, wide straps.",
      24.99, "women-tops", "urbanthread"),
    p("wt-linen-blouse",     "Linen Relaxed Blouse",
      "Airy linen-cotton blend blouse with relaxed fit and roll-up sleeves.",
      44.99, "women-tops", "nordstyle"),
    p("wt-crop-knit",        "Crop Knit Top Lilac",
      "Ribbed crop knit in pastel lilac. Stretch fit, crew neck, long sleeves.",
      39.99, "women-tops", "velourex"),
    p("wt-oversized-tee",    "Oversized BF Tee White",
      "Boyfriend-cut oversized tee in clean white. 180gsm cotton, relaxed fit.",
      29.99, "women-tops", "casualco"),
    p("wt-satin-cami",       "Satin Cami Top Black",
      "Slinky satin cami in true black. Adjustable straps, V-neck, bralette panel.",
      44.99, "women-tops", "velourex"),
    p("wt-button-front",     "Button Front Crop Top",
      "Cotton-linen button-front crop in ecru. Square neck, short sleeves, boxy fit.",
      34.99, "women-tops", "nordstyle"),
    p("wt-turtleneck",       "Ribbed Turtleneck Top Cream",
      "Fitted ribbed turtleneck in cream viscose. Long sleeves, second-skin silhouette.",
      44.99, "women-tops", "nordstyle"),
    p("wt-stripe-breton",    "Breton Stripe Top Navy",
      "Classic Breton stripe in navy and white. Boat neck, 3/4 sleeves, 100% cotton.",
      49.99, "women-tops", "nordstyle"),
    p("wt-off-shoulder",     "Off Shoulder Top White",
      "Flirty off-shoulder in smocked cotton. Puff sleeves, ruffle edge.",
      42.99, "women-tops", "velourex"),
    p("wt-puff-sleeve",      "Puff Sleeve Blouse Pink",
      "Oversized puff sleeves on fitted bodice. Soft pink cotton poplin, button back.",
      54.99, "women-tops", "casualco"),
    p("wt-corset-top",       "Corset Style Top Black",
      "Boned corset-style top in matte black satin. Lace-up back, sweetheart neckline.",
      59.99, "women-tops", "velourex"),
    p("wt-mesh-top",         "Sheer Mesh Top Black",
      "Semi-transparent mesh in black. Long sleeves, crew neck.",
      34.99, "women-tops", "steetpeak"),
    p("wt-utility-shirt",    "Utility Tie-Front Shirt",
      "Cotton utility shirt tied at the front hem. Chest pockets, short sleeves.",
      49.99, "women-tops", "urbanthread"),
    p("wt-knit-vest",        "Knit Vest Top Camel",
      "Sleeveless knit vest in camel-tone ribbed fabric. V-neck, cropped length.",
      44.99, "women-tops", "nordstyle"),
    p("wt-silk-cami",        "Silk Cami Top Ivory",
      "Pure silk camisole with adjustable spaghetti straps. V-neck, ivory.",
      79.99, "women-tops", "velourex"),
    p("wt-cashmere-sweater", "Cashmere Crew Sweater",
      "100% grade-A cashmere crew-neck sweater. Soft oatmeal, classic fit.",
      189.99, "women-tops", "nordstyle"),
    p("wt-cable-jumper",     "Cable Jumper Cream",
      "Chunky cable-knit cream jumper. Relaxed fit, rolled crew neck.",
      99.99, "women-tops", "nordstyle"),
    p("wt-button-cardigan",  "Button Cardigan Sage",
      "Mid-weight sage-green button cardigan. Vintage-inspired small pearl buttons.",
      69.99, "women-tops", "casualco"),
    p("wt-boho-embroidery",  "Boho Embroidered Blouse",
      "Cotton gauze blouse with multi-color embroidery at yoke. Blouson cut.",
      64.99, "women-tops", "casualco"),
    p("wt-organic-tee",      "Organic Cotton Tee",
      "GOTS-certified organic cotton crew tee. Boxy fit, neutral oat color.",
      32.99, "women-tops", "casualco"),

    # ── Women's Jeans ─────────────────────────────────────────────────────────
    p("wj-highrise-mom",     "High-Rise Mom Jeans",
      "Relaxed seat and tapered leg. High-waisted with a clean stonewash.",
      64.99, "women-jeans", "casualco"),
    p("wj-skinny-black",     "Skinny Black Jeans",
      "Classic skinny in stretch denim. High-rise, jet-black rinse.",
      59.99, "women-jeans", "casualco"),
    p("wj-wide-crop",        "Wide Crop Jeans Ecru",
      "Full wide-leg cut cropped at the ankle in ecru-toned raw denim.",
      79.99, "women-jeans", "nordstyle"),
    p("wj-straight-light",   "Straight Light Wash Jeans",
      "Classic straight cut in pale acid-wash denim. Clean finish, ankle length.",
      69.99, "women-jeans", "casualco"),
    p("wj-barrel-leg",       "Barrel Leg Jeans Mid Wash",
      "Curved barrel leg for roomy thigh tapering to narrower ankle. Mid-wash indigo.",
      89.99, "women-jeans", "nordstyle"),
    p("wj-low-rise",         "Low Rise Straight Jeans",
      "Low-rise waistband on straight silhouette. Dark wash, 5-pocket, slight stretch.",
      74.99, "women-jeans", "steetpeak"),
    p("wj-flare-indigo",     "Flare Indigo Jeans",
      "70s-inspired flare from the knee. Deep indigo wash, high rise, raw hem.",
      84.99, "women-jeans", "velourex"),
    p("wj-vintage-mom",      "Vintage Mom Jeans Light",
      "Authentically faded mom jeans in pale stone wash. Relaxed seat, tapered below.",
      69.99, "women-jeans", "urbanthread"),
    p("wj-bootcut",          "Bootcut Jeans Dark Wash",
      "Traditional bootcut flare from the knee. Dark navy-blue wash, mid-rise.",
      72.99, "women-jeans", "casualco"),
    p("wj-paperbag",         "Paperbag Waist Jeans",
      "Gathered paperbag waistband with tie detail. Relaxed tapered leg, mid wash.",
      89.99, "women-jeans", "nordstyle"),
    p("wj-90s-straight",     "90s Straight Jeans Ecru",
      "Ecru (undyed) denim in 90s-inspired straight cut. Natural tones, raw selvedge.",
      74.99, "women-jeans", "urbanthread"),
    p("wj-ripped-skinny",    "Ripped Skinny Jeans Grey",
      "Heavily distressed grey-wash skinny. Rips at thigh and knee.",
      69.99, "women-jeans", "steetpeak"),
    p("wj-patchwork",        "Patchwork Wide Jeans",
      "Mixed light-and-dark denim panels in wide-leg silhouette. Each pair unique.",
      94.99, "women-jeans", "urbanthread"),
    p("wj-wide-leg",         "Wide-Leg Denim",
      "High-rise wide-leg jeans in rigid denim. Full leg drop, cropped length.",
      79.99, "women-jeans", "nordstyle"),
    p("wj-black-skinny",     "Jet Black Skinny",
      "Jet-black skinny jeans, high-rise, stretch cotton. Never fades.",
      59.99, "women-jeans", "urbanthread"),
    p("wj-cargo-utility",    "Cargo Utility Denim",
      "Wide-leg denim with thigh cargo pockets. Stone-washed mid-blue.",
      79.99, "women-jeans", "steetpeak"),
    p("wj-flared-high",      "High-Rise Flare",
      "High-rise 70s-inspired flare. Hits at natural waist, dramatic leg drop.",
      79.99, "women-jeans", "velourex"),
    p("wj-boyfriend",        "Boyfriend Fit Jeans",
      "Slouchy boyfriend-cut jeans with subtle distressing. Rolled hem.",
      69.99, "women-jeans", "urbanthread"),
    p("wj-paperbag-waist",   "Paperbag Waist Denim",
      "Paperbag-waist belted denim. Wide leg, self-tie belt at waist.",
      74.99, "women-jeans", "casualco"),
    p("wj-ecru-straight",    "Ecru Straight Jeans",
      "Off-white ecru straight-leg jeans. Mid rise, summer-weight denim.",
      64.99, "women-jeans", "casualco"),

    # ── Women's Jackets ───────────────────────────────────────────────────────
    p("wjk-blazer-camel",    "Camel Oversized Blazer",
      "Oversized power blazer in camel double-face fabric. Three-button, notch lapel.",
      149.99, "women-jackets", "velourex"),
    p("wjk-denim-oversized", "Oversized Denim Jacket",
      "Roomy oversized trucker in mid-wash indigo denim. Copper buttons.",
      89.99, "women-jackets", "urbanthread"),
    p("wjk-leather-crop",    "Crop Leather Jacket Black",
      "Waist-length leather jacket in full-grain black cowhide. Asymmetric zip.",
      219.99, "women-jackets", "velourex"),
    p("wjk-blazer-check",    "Check Blazer Taupe",
      "Single-breasted blazer in fine taupe-and-ivory windowpane check. Slim, 2-button.",
      139.99, "women-jackets", "nordstyle"),
    p("wjk-puffer-pink",     "Cropped Puffer Jacket Pink",
      "Boxy cropped puffer in soft blush-pink. Recycled fill, high neck, zip front.",
      99.99, "women-jackets", "nordstyle"),
    p("wjk-trench",          "Classic Trench Coat Beige",
      "Double-breasted trench in gabardine beige. Storm flap, D-ring belt, epaulettes.",
      179.99, "women-jackets", "nordstyle"),
    p("wjk-biker-tan",       "Tan Biker Jacket",
      "Supple tan leather biker with quilted lining, buckle-trim collar, zip pockets.",
      199.99, "women-jackets", "velourex"),
    p("wjk-teddy-coat",      "Teddy Bear Coat Cream",
      "Statement teddy fleece coat in ivory cream. Oversize, dropped shoulder.",
      169.99, "women-jackets", "nordstyle"),
    p("wjk-shacket",         "Flannel Shacket Plaid",
      "Oversized shirt-jacket in red-and-navy flannel plaid. Snap buttons.",
      89.99, "women-jackets", "casualco"),
    p("wjk-satin-bomber",    "Satin Bomber Jacket Ivory",
      "Glossy satin bomber in ivory. Rib-knit cuffs and hem, embroidered back detail.",
      119.99, "women-jackets", "velourex"),
    p("wjk-tweed",           "Bouclé Tweed Jacket",
      "French-style bouclé tweed in cream-and-gold weave. Collarless, single button.",
      159.99, "women-jackets", "velourex"),
    p("wjk-utility-khaki",   "Utility Jacket Khaki",
      "Safari-style utility jacket in khaki cotton-twill. Four flap pockets.",
      109.99, "women-jackets", "urbanthread"),
    p("wjk-leather-moto",    "Leather Moto Jacket",
      "Cropped cowhide leather moto with asymmetric zip. Zip cuffs, waist belt.",
      299.99, "women-jackets", "velourex"),
    p("wjk-wool-peacoat",    "Wool Peacoat Navy",
      "Double-breasted wool-blend peacoat in navy. Anchor-detailed buttons.",
      199.99, "women-jackets", "nordstyle"),
    p("wjk-parka-olive",     "Parka with Faux Fur",
      "Longline olive parka with removable faux-fur hood trim. Drawcord waist.",
      179.99, "women-jackets", "steetpeak"),
    p("wjk-denim-crop",      "Cropped Denim Jacket",
      "Cropped classic trucker denim jacket. Mid blue wash, slightly distressed.",
      79.99, "women-jackets", "urbanthread"),
    p("wjk-belted-trench",   "Belted Trench Stone",
      "Belted stone-color trench, double-breasted, knee length, epaulets.",
      169.99, "women-jackets", "nordstyle"),
    p("wjk-corduroy-sherpa", "Corduroy Sherpa Jacket",
      "Wide-wale corduroy jacket with sherpa body lining. Warm rust color.",
      119.99, "women-jackets", "casualco"),
    p("wjk-cropped-blazer",  "Cropped Tuxedo Blazer",
      "Cropped tuxedo blazer in black wool-blend. Satin shawl lapel.",
      139.99, "women-jackets", "velourex"),
    p("wjk-kimono-robe",     "Printed Kimono Jacket",
      "Longline printed kimono-style jacket, open front. Floral on black.",
      89.99, "women-jackets", "casualco"),

    # ── Bags ──────────────────────────────────────────────────────────────────
    p("bg-canvas-tote",      "Canvas Tote Bag",
      "Heavy-duty natural canvas tote with leather handles. Spacious open interior.",
      34.99, "bags", "urbanthread"),
    p("bg-crossbody",        "Crossbody Bag",
      "Compact pebbled leather crossbody with gold-tone chain strap.",
      49.99, "bags", "velourex"),
    p("bg-leather-tote",     "Leather Structured Tote Black",
      "Full-grain leather structured tote. Double handles, internal zip pocket.",
      129.99, "bags", "velourex"),
    p("bg-mini-backpack",    "Mini Backpack Quilted",
      "Quilted nylon mini backpack in black. Adjustable straps, gold hardware.",
      79.99, "bags", "nordstyle"),
    p("bg-bucket-bag",       "Bucket Bag Tan",
      "Tan pebbled-leather bucket bag with drawstring closure and crossbody strap.",
      109.99, "bags", "velourex"),
    p("bg-clutch-satin",     "Satin Clutch Bag Gold",
      "Evening clutch in champagne satin with gold-frame clasp. Wrist strap.",
      69.99, "bags", "velourex"),
    p("bg-shopper-linen",    "Linen Shopper Bag Natural",
      "Spacious oversized shopper in natural linen-canvas. Leather handles.",
      49.99, "bags", "nordstyle"),
    p("bg-saddle-bag",       "Saddle Bag Brown",
      "Tan-brown leather saddle bag with top flap and D-ring strap.",
      119.99, "bags", "velourex"),
    p("bg-hobo-bag",         "Hobo Bag Soft Leather Black",
      "Slouchy hobo in buttery soft black leather. Single strap, zip top, suede lining.",
      139.99, "bags", "velourex"),
    p("bg-belt-bag",         "Belt Bag Black",
      "Compact belt bag in black nylon. Zip compartment, adjustable web strap.",
      44.99, "bags", "steetpeak"),
    p("bg-canvas-backpack",  "Canvas Backpack Olive",
      "Medium waxed-canvas backpack in olive. Laptop sleeve, front zip pocket.",
      69.99, "bags", "urbanthread"),
    p("bg-chain-bag",        "Chain Strap Bag Silver",
      "Mini bag in quilted black leather with silver chain strap. Clasp closure.",
      89.99, "bags", "velourex"),
    p("bg-duffle-small",     "Small Duffle Bag Navy",
      "Weekender-style small duffle in navy canvas. Leather trim, zip top.",
      74.99, "bags", "urbanthread"),
    p("bg-gym-tote",         "Gym Tote Bag Black",
      "Large-format gym tote in ripstop nylon. Zip top, shoe compartment.",
      39.99, "bags", "steetpeak"),
    p("bg-weekender",        "Waxed Canvas Weekender",
      "Oversized weekender bag in waxed canvas with leather trim. Zip top.",
      149.99, "bags", "nordstyle"),
    p("bg-backpack-roll",    "Rolltop Backpack",
      "Rolltop commuter backpack in water-resistant nylon. Padded laptop sleeve.",
      99.99, "bags", "steetpeak"),
    p("bg-work-tote",        "Leather Work Tote",
      "Structured full-grain leather work tote. Fits a 15-inch laptop.",
      229.99, "bags", "nordstyle"),
    p("bg-quilted-chain",    "Quilted Chain Bag",
      "Diamond-quilted leather shoulder bag with gold-tone chain strap.",
      149.99, "bags", "velourex"),
    p("bg-hobo-leather",     "Leather Hobo Bag",
      "Slouchy leather hobo with single strap. Soft cowhide, minimal hardware.",
      169.99, "bags", "nordstyle"),
    p("bg-shell-crossbody",  "Shell Shape Crossbody",
      "Curved shell-shape leather crossbody. Magnetic flap, slim silhouette.",
      109.99, "bags", "urbanthread"),

    # ── Hats ──────────────────────────────────────────────────────────────────
    p("ht-baseball-black",   "Classic Baseball Cap",
      "Structured six-panel cap in classic black. Adjustable strap, embossed logo.",
      22.99, "hats", "steetpeak"),
    p("ht-beanie-grey",      "Ribbed Beanie Grey",
      "Fine-gauge ribbed beanie in heather grey. Acrylic-wool blend.",
      18.99, "hats", "nordstyle"),
    p("ht-bucket-khaki",     "Bucket Hat Khaki",
      "Classic cotton-twill bucket hat in muted khaki. Flexible brim, sweatband.",
      24.99, "hats", "steetpeak"),
    p("ht-wide-brim",        "Wide Brim Sun Hat Natural",
      "Woven straw wide-brim sun hat in natural. Grosgrain band, packable.",
      39.99, "hats", "nordstyle"),
    p("ht-snapback",         "Snapback Cap White",
      "Structured six-panel snapback in clean white. Flat brim, adjustable.",
      27.99, "hats", "steetpeak"),
    p("ht-dad-hat",          "Dad Hat Washed Black",
      "Unstructured curved-brim dad cap in faded black wash. Velcro strap.",
      22.99, "hats", "steetpeak"),
    p("ht-cable-beanie",     "Cable Knit Beanie Cream",
      "Chunky cable-knit beanie in off-white cream. Pom-pom top, ribbed cuff.",
      24.99, "hats", "nordstyle"),
    p("ht-trapper",          "Trapper Hat Sherpa",
      "Sherpa-lined trapper hat with ear flaps that tie up. Plaid outer shell.",
      39.99, "hats", "nordstyle"),
    p("ht-fisherman",        "Fisherman Beanie Black",
      "Close-fitting fisherman-style short cuff beanie in dense black ribbed cotton.",
      19.99, "hats", "urbanthread"),
    p("ht-cowboy",           "Straw Cowboy Hat Tan",
      "Classic western straw cowboy hat in natural tan. Wide brim, chin cord.",
      49.99, "hats", "casualco"),
    p("ht-beret-wool",       "Wool Beret Burgundy",
      "Classic French beret in 100% boiled wool. Burgundy colourway, inner sweatband.",
      34.99, "hats", "nordstyle"),
    p("ht-trucker",          "Mesh Trucker Cap",
      "Two-tone trucker in navy front panel with white mesh back. Snapback.",
      24.99, "hats", "steetpeak"),
    p("ht-newsboy",          "Newsboy Cap Herringbone",
      "Eight-panel newsboy cap in wool-blend herringbone. Brim button, lined.",
      42.99, "hats", "nordstyle"),
    p("ht-bucket-denim",     "Denim Bucket Hat",
      "Washed denim bucket hat in mid-blue indigo. Double seam brim.",
      27.99, "hats", "urbanthread"),
    p("ht-chunky-beanie",    "Chunky Knit Beanie Camel",
      "Oversized slouchy beanie in thick-gauge knit. Warm camel tone.",
      29.99, "hats", "nordstyle"),
    p("ht-wool-fedora",      "Wool Felt Fedora",
      "Classic wool-felt fedora with grosgrain ribbon band. Pinched crown.",
      59.99, "hats", "nordstyle"),
    p("ht-straw-panama",     "Straw Panama Hat",
      "Hand-woven straw Panama hat with black band. Structured brim.",
      69.99, "hats", "casualco"),
    p("ht-baker-boy",        "Baker Boy Hat Plaid",
      "Six-panel baker boy cap in forest-green tartan plaid. Button top.",
      37.99, "hats", "nordstyle"),
    p("ht-winter-aviator",   "Aviator Winter Hat",
      "Shearling-lined aviator hat with pilot-style earflaps. Leather exterior.",
      79.99, "hats", "velourex"),
    p("ht-linen-summer",     "Linen Summer Cap",
      "Lightweight unstructured linen cap in natural flax. Breathable crown.",
      29.99, "hats", "casualco"),

    # ── Belts ─────────────────────────────────────────────────────────────────
    p("blt-leather-pin",     "Leather Pin Belt",
      "Classic full-grain leather belt with matte-silver pin buckle. 3.5cm wide.",
      29.99, "belts", "casualco"),
    p("blt-leather-black",   "Classic Black Leather Belt",
      "Full-grain cowhide leather belt, 3.5cm wide, matte-black buckle.",
      34.99, "belts", "casualco"),
    p("blt-woven-stretch",   "Woven Stretch Belt",
      "Elasticated woven belt, no holes, sliding brass buckle. Navy.",
      29.99, "belts", "urbanthread"),
    p("blt-tan-classic",     "Tan Leather Belt",
      "Tan oiled-leather belt with antique brass buckle. 3.5cm width.",
      34.99, "belts", "casualco"),
    p("blt-reversible",      "Reversible Dress Belt",
      "Reversible black-to-brown leather dress belt with rotating buckle.",
      49.99, "belts", "nordstyle"),
    p("blt-western-studs",   "Western Studded Belt",
      "Tooled leather western belt with silver studs and engraved buckle.",
      59.99, "belts", "steetpeak"),
    p("blt-canvas-web",      "Canvas Webbing Belt",
      "Olive canvas webbing belt with D-ring buckle. Military-inspired.",
      24.99, "belts", "steetpeak"),
    p("blt-braided-leather", "Braided Leather Belt",
      "Hand-braided leather belt in tan. No holes, slide to any buckle position.",
      44.99, "belts", "nordstyle"),
    p("blt-chain-novelty",   "Silver Chain Belt",
      "Decorative silver-tone chain belt. Fashion piece, lobster clasp.",
      39.99, "belts", "velourex"),
    p("blt-corset-wide",     "Wide Corset Belt",
      "Wide 8cm leather corset belt. Lace-through closure, cinches the waist.",
      59.99, "belts", "velourex"),
    p("blt-skinny-red",      "Skinny Red Belt",
      "Slim 1.5cm red patent-finish leather belt. Small square silver buckle.",
      24.99, "belts", "velourex"),
    p("blt-suede-wrap",      "Suede Wrap Belt",
      "Long suede wrap belt with tie closure, no buckle. Tan colorway.",
      39.99, "belts", "casualco"),
    p("blt-elastic-waist",   "Elastic Stretch Belt",
      "Fully elastic stretch waist belt with interlocking buckle. Black.",
      22.99, "belts", "urbanthread"),
    p("blt-utility-nylon",   "Utility Nylon Belt",
      "Nylon utility belt with quick-release plastic buckle. Lightweight.",
      19.99, "belts", "steetpeak"),
    p("blt-oversize-buckle", "Oversized Buckle Belt",
      "Black leather belt with oversized square silver buckle. Statement piece.",
      54.99, "belts", "velourex"),
    p("blt-burgundy",        "Burgundy Leather Belt",
      "Rich burgundy full-grain leather belt. Antique-silver ribbon buckle.",
      39.99, "belts", "nordstyle"),
    p("blt-two-tone",        "Two-Tone Leather Belt",
      "Two-tone cognac-and-navy leather belt. Classic silver pin buckle.",
      44.99, "belts", "casualco"),
    p("blt-monogram-buckle", "Monogram Buckle Belt",
      "Smooth black leather belt with engraved monogram buckle. 3cm wide.",
      49.99, "belts", "velourex"),
    p("blt-rope-knot",       "Rope Knot Belt",
      "Natural cotton rope belt with simple knot closure. Beachy, relaxed feel.",
      19.99, "belts", "casualco"),
    p("blt-grommet-white",   "Grommet White Belt",
      "White leather belt with silver grommet rows. Edgy, punk-inspired.",
      44.99, "belts", "steetpeak"),
]

MANAGED_SKUS = {prod["sku"] for prod in PRODUCTS}
MANAGED_CATEGORY_CODES = {code for code, _, _ in LEAF_CATS}

# ── setup functions ───────────────────────────────────────────────────────────

def create_brands(token):
    print("\n── Brands ──────────────────────────────────────────────────────────")
    for code, name in BRANDS:
        if manufacturer_exists(code, token):
            mid = find_manufacturer_id(code, token)
            if mid:
                print(f"  ↩ {code:<16} already exists (id={mid})")
            else:
                print(f"  ↩ {code:<16} already exists")
            continue
        try:
            r = post("/api/v1/private/manufacturer",
                     {"code": code,
                      "descriptions": [{"language": "en", "name": name, "friendlyUrl": code}]},
                     token)
            print(f"  ✓ {code:<16} id={r.get('id')}")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code == 409 or _is_duplicate_error(body):
                mid = find_manufacturer_id(code, token)
                if mid:
                    print(f"  ↩ {code:<16} already exists (id={mid})")
                else:
                    print(f"  ↩ {code:<16} already exists")
            else:
                print(f"  ✗ {code:<16} {e.code} {body[:60]}")


def create_categories(token):
    print("\n── Categories ──────────────────────────────────────────────────────")
    ids = {}

    for code, name in ROOT_CATS:
        try:
            r = post("/api/v1/private/category",
                     {"code": code, "visible": True,
                      "descriptions": [{"language": "en", "name": name, "friendlyUrl": code}]},
                     token)
            ids[code] = r.get("id")
            print(f"  ✓ {code:<20} id={ids[code]}")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code == 409 or _is_duplicate_error(body):
                cid = find_category_id(code, token)
                ids[code] = cid
                print(f"  ↩ {code:<20} already exists (id={cid})")
            else:
                print(f"  ✗ {code:<20} {e.code} {body[:60]}")

    for code, name, parent_code in LEAF_CATS:
        payload = {
            "code": code, "visible": True,
            "descriptions": [{"language": "en", "name": name, "friendlyUrl": code}],
        }
        parent_id = ids.get(parent_code)
        if parent_id:
            payload["parent"] = {"id": parent_id}
        try:
            r = post("/api/v1/private/category", payload, token)
            print(f"  ✓ {code:<20} id={r.get('id')}  parent={parent_code}")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code == 409 or _is_duplicate_error(body):
                cid = find_category_id(code, token)
                print(f"  ↩ {code:<20} already exists (id={cid})")
            else:
                print(f"  ✗ {code:<20} {e.code} {body[:60]}")


def create_products(token):
    print("\n── Products ─────────────────────────────────────────────────────────")
    total, ok, fail = len(PRODUCTS), 0, 0
    for i, prod in enumerate(PRODUCTS, 1):
        if product_exists(prod["sku"], token):
            ok += 1
            pid = find_product_id_by_sku(prod["sku"], token)
            suffix = f"id={pid}" if isinstance(pid, int) else "already exists"
            print(f"  ↩  [{i:03d}/{total}] {prod['sku']:<34} {suffix}")
            continue
        try:
            r = post("/api/v1/private/product", prod, token)
            ok += 1
            print(f"  [{i:03d}/{total}] {prod['sku']:<34} id={r.get('id')}")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            if e.code == 409 or _is_duplicate_error(body):
                ok += 1
                pid = find_product_id_by_sku(prod["sku"], token)
                suffix = f"id={pid}" if isinstance(pid, int) else "already exists"
                print(f"  ↩  [{i:03d}/{total}] {prod['sku']:<34} {suffix}")
            else:
                fail += 1
                print(f"  ✗  [{i:03d}/{total}] {prod['sku']:<34} {e.code} {body[:80]}")
        if i % 80 == 0:
            print("  [refreshing token…]")
            token = login()
    print(f"\n  OK: {ok}/{total}   Failures: {fail}")
    return token


def delete_managed_products(token):
    print("\n── Cleanup Existing Managed Products ────────────────────────────────")
    if not CLEANUP_ENABLED:
        print("  Cleanup disabled by SHOPIZER_SETUP_CLEANUP_ENABLED=false")
        return
    managed = collect_managed_products_for_cleanup(token)
    if not managed:
        print("  No previously seeded managed products found.")
        return

    print(f"  Located {len(managed)} managed product(s) to delete")
    deleted = 0
    for i, prod in enumerate(managed, 1):
        pid = prod.get("id")
        sku = prod.get("sku", "<unknown>")
        if not pid:
            print(f"  ! Skipping {sku:<34} (product exists but id could not be resolved)")
            continue
        try:
            delete(f"/api/v1/private/product/{pid}", token)
            deleted += 1
            print(f"  ✓ [{i:03d}/{len(managed)}] deleted {sku:<34} id={pid}")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  ✗ [{i:03d}/{len(managed)}] delete {sku:<34} {e.code} {body[:80]}")
        if i % 80 == 0:
            print("  [refreshing token…]")
            token = login()

    print(f"\n  Deleted: {deleted}/{len(managed)} managed products")


def verify_catalog():
    print("\n── Verification ─────────────────────────────────────────────────────")
    global_public = list_public_products(page=0, count=PAGE_SIZE)
    public = collect_public_products()
    print(f"  Global /api/v1/products page 0 returned {len(global_public)} product(s)")
    print(f"  Aggregated public listing returned {len(public)} product(s)")
    by_sku = {p.get("sku"): p for p in public if p.get("sku")}
    missing_skus = sorted(MANAGED_SKUS - set(by_sku.keys()))
    hidden_skus = []
    if missing_skus:
        for sku in missing_skus:
            if get_product_by_sku(sku):
                hidden_skus.append(sku)
    if missing_skus:
        print(f"  ✗ Missing {len(missing_skus)} managed SKUs from public product listing")
        for sku in missing_skus:
            print(f"    - {sku}")
        if hidden_skus:
            print(f"  ! {len(hidden_skus)} of those SKU(s) are retrievable directly by /api/v2/product/{{sku}} but hidden from public listings")
    else:
        print(f"  ✓ All {len(MANAGED_SKUS)} managed SKUs are visible in public listings")

    category_ids = {code: find_category_id(code, None) for code in MANAGED_CATEGORY_CODES}
    thin_categories = []
    for code, cid in sorted(category_ids.items()):
        if not cid:
            thin_categories.append((code, 0))
            continue
        visible = list_public_products(page=0, count=20, category_id=cid)
        if not visible:
            visible = list_public_products_by_category_slug(code, page=0, count=20)
        if len(visible) < 20:
            thin_categories.append((code, len(visible)))

    if thin_categories:
        print("  ✗ Some managed categories still expose fewer than 20 products on page 0:")
        for code, count in thin_categories:
            print(f"    - {code}: {count}")
    else:
        print("  ✓ Every managed leaf category exposes at least 20 products on page 0")

    return not missing_skus and not thin_categories


def main():
    announce_step("authentication")
    print("Connecting…")
    token = login()
    print("Authenticated.")
    announce_step("brand setup")
    create_brands(token)
    announce_step("category setup")
    create_categories(token)
    announce_step("managed product cleanup")
    delete_managed_products(token)
    announce_step("product creation")
    create_products(token)
    announce_step("catalog verification")
    verified = verify_catalog()
    if verified:
        print("\nDone — catalog is ready.")
        return 0
    print("\nCatalog verification failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
