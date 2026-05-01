#!/usr/bin/env python3
"""
AirportAccidents.com — Page Generator
======================================
Generates all static HTML pages from templates + data.

Output structure:
  dist/
    index.html
    state/{code}/index.html                       (54 state hubs)
    {accident-slug}/index.html                    (25 accident hubs)
    {airport-slug}/index.html                     (522 airport hubs)
    {airport-slug}/{accident-slug}/index.html     (13,050 leaf pages)

Usage:
  python3 scripts/generate.py                  # everything
  python3 scripts/generate.py --dry-run        # count only
  python3 scripts/generate.py --clean          # wipe dist/ first
  python3 scripts/generate.py --states
  python3 scripts/generate.py --airports
  python3 scripts/generate.py --accidents
  python3 scripts/generate.py --leaves
  python3 scripts/generate.py --state CA
  python3 scripts/generate.py --airport LAX
  python3 scripts/generate.py --accident slip-and-fall
  python3 scripts/generate.py --airport LAX --accident slip-and-fall
  python3 scripts/generate.py --phase 1|2|3|4
"""

import csv, json, os, re, sys, shutil, argparse
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT          = Path(__file__).parent.parent
DATA_DIR      = ROOT / "data"
TEMPLATES_DIR = ROOT / "templates"
PUBLIC        = ROOT / "public"
DIST          = ROOT / "dist"
ASSETS_SRC    = ROOT / "assets"

AIRPORTS_CSV   = DATA_DIR / "us_airports_full.csv"
ACCIDENTS_JSON = DATA_DIR / "accidents_database_v2.json"
CROSSREF_CSV   = DATA_DIR / "airports_accidents_crossref.csv"
PROFILES_JSON  = DATA_DIR / "airport_profiles.json"
TMPL_STATE     = TEMPLATES_DIR / "state-hub.html"
TMPL_AIRPORT   = TEMPLATES_DIR / "airport-hub.html"
TMPL_ACCIDENT  = TEMPLATES_DIR / "accident-hub.html"
TMPL_LEAF      = TEMPLATES_DIR / "leaf-page.html"
HOMEPAGE_SRC   = PUBLIC / "index.html"
BASE_URL       = "https://airportaccidents.com"

# ── Site config — edit site.config.json to change these ─────────────────────
_cfg = json.loads((ROOT / "site.config.json").read_text()) if (ROOT / "site.config.json").exists() else {}
PHONE_DISPLAY  = _cfg.get("phone_display", "1-800-555-0199")
PHONE_DIGITS   = _cfg.get("phone_digits",  "18005550199")
SITE_NAME      = _cfg.get("site_name",     "AirportAccidents.com")
BASE_URL       = _cfg.get("base_url",      BASE_URL)
GTM_ID         = _cfg.get("gtm_id",        "")
FORM_WEBHOOK   = _cfg.get("form_webhook",  "")

STATE_LEGAL = {
    "Alabama":        {"sol":"2 years",   "notice":"6 months",   "gov":"Birmingham Airport Authority"},
    "Alaska":         {"sol":"2 years",   "notice":"120 days",   "gov":"Ted Stevens Anchorage Int'l Airport"},
    "Arizona":        {"sol":"2 years",   "notice":"180 days",   "gov":"City of Phoenix Aviation Dept"},
    "Arkansas":       {"sol":"3 years",   "notice":"1 year",     "gov":"Little Rock National Airport Auth"},
    "California":     {"sol":"2 years",   "notice":"6 months",   "gov":"Los Angeles World Airports (LAWA)"},
    "Colorado":       {"sol":"2 years",   "notice":"182 days",   "gov":"Denver International Airport"},
    "Connecticut":    {"sol":"2 years",   "notice":"6 months",   "gov":"CT Airport Authority"},
    "Florida":        {"sol":"2 years",   "notice":"3 years (sovereign immunity)","gov":"Miami-Dade Aviation Dept"},
    "Georgia":        {"sol":"2 years",   "notice":"6 months",   "gov":"City of Atlanta Dept of Aviation"},
    "Guam":           {"sol":"2 years",   "notice":"6 months",   "gov":"Antonio B. Won Pat Int'l Airport"},
    "Hawaii":         {"sol":"2 years",   "notice":"None required","gov":"Hawaii Dept of Transportation"},
    "Idaho":          {"sol":"2 years",   "notice":"180 days",   "gov":"Boise Airport Authority"},
    "Illinois":       {"sol":"2 years",   "notice":"1 year",     "gov":"Chicago Dept of Aviation"},
    "Indiana":        {"sol":"2 years",   "notice":"180 days",   "gov":"Indianapolis Airport Authority"},
    "Iowa":           {"sol":"2 years",   "notice":"60 days",    "gov":"Des Moines Airport Authority"},
    "Kansas":         {"sol":"2 years",   "notice":"120 days",   "gov":"Wichita Airport Authority"},
    "Kentucky":       {"sol":"1 year",    "notice":"90 days",    "gov":"Louisville Regional Airport Auth"},
    "Louisiana":      {"sol":"1 year",    "notice":"None required","gov":"Louis Armstrong Int'l Airport"},
    "Maine":          {"sol":"6 years",   "notice":"None required","gov":"Portland Int'l Jetport"},
    "Maryland":       {"sol":"3 years",   "notice":"1 year",     "gov":"Maryland Aviation Administration"},
    "Massachusetts":  {"sol":"3 years",   "notice":"None required","gov":"Massport (Logan Airport)"},
    "Michigan":       {"sol":"3 years",   "notice":"None required","gov":"Wayne County Airport Authority"},
    "Minnesota":      {"sol":"3 years",   "notice":"180 days",   "gov":"Metropolitan Airports Commission"},
    "Mississippi":    {"sol":"3 years",   "notice":"90 days",    "gov":"Jackson Municipal Airport Auth"},
    "Missouri":       {"sol":"5 years",   "notice":"90 days",    "gov":"Kansas City Aviation Dept"},
    "Montana":        {"sol":"3 years",   "notice":"None required","gov":"Billings Logan Int'l Airport"},
    "Nebraska":       {"sol":"4 years",   "notice":"1 year",     "gov":"Omaha Airport Authority"},
    "Nevada":         {"sol":"2 years",   "notice":"6 months",   "gov":"Clark County Dept of Aviation (LAS)"},
    "New Hampshire":  {"sol":"3 years",   "notice":"None required","gov":"Manchester-Boston Regional Airport"},
    "New Jersey":     {"sol":"2 years",   "notice":"90 days",    "gov":"Port Authority of NY & NJ"},
    "New Mexico":     {"sol":"3 years",   "notice":"90 days",    "gov":"Albuquerque Int'l Sunport"},
    "New York":       {"sol":"1.5 years", "notice":"90 days",    "gov":"Port Authority of NY & NJ"},
    "North Carolina": {"sol":"3 years",   "notice":"None required","gov":"Charlotte Douglas Int'l Airport"},
    "North Dakota":   {"sol":"6 years",   "notice":"None required","gov":"Hector Int'l Airport, Fargo"},
    "Ohio":           {"sol":"2 years",   "notice":"None required","gov":"Columbus Regional Airport Auth"},
    "Oklahoma":       {"sol":"2 years",   "notice":"1 year",     "gov":"Will Rogers World Airport"},
    "Oregon":         {"sol":"2 years",   "notice":"180 days",   "gov":"Port of Portland"},
    "Pennsylvania":   {"sol":"2 years",   "notice":"6 months",   "gov":"Philadelphia Airport (city-owned)"},
    "Puerto Rico":    {"sol":"1 year",    "notice":"None required","gov":"Luis Munoz Marin Int'l Airport"},
    "Rhode Island":   {"sol":"3 years",   "notice":"None required","gov":"Rhode Island Airport Corp"},
    "South Carolina": {"sol":"3 years",   "notice":"None required","gov":"Charleston Int'l Airport"},
    "South Dakota":   {"sol":"3 years",   "notice":"None required","gov":"Rapid City Regional Airport"},
    "Tennessee":      {"sol":"1 year",    "notice":"1 year",     "gov":"Metro Nashville Airport Auth"},
    "Texas":          {"sol":"2 years",   "notice":"None (most)","gov":"DFW Airport Board / City of Houston"},
    "US Virgin Islands":{"sol":"2 years", "notice":"6 months",   "gov":"Virgin Islands Port Authority"},
    "Utah":           {"sol":"4 years",   "notice":"1 year",     "gov":"Salt Lake City Corp (SLC)"},
    "Vermont":        {"sol":"3 years",   "notice":"None required","gov":"Burlington Int'l Airport"},
    "Virginia":       {"sol":"2 years",   "notice":"None required","gov":"Metropolitan Washington Airports Auth"},
    "Washington":     {"sol":"3 years",   "notice":"None required","gov":"Port of Seattle"},
    "West Virginia":  {"sol":"2 years",   "notice":"None required","gov":"Yeager Airport, Charleston"},
    "Wisconsin":      {"sol":"3 years",   "notice":"120 days",   "gov":"Milwaukee County Airport"},
    "Wyoming":        {"sol":"4 years",   "notice":"None required","gov":"Jackson Hole Airport"},
}
DEFAULT_LEGAL = {"sol":"2-3 years","notice":"60-180 days (varies)","gov":"Local airport authority"}

TYPE_LABELS = {"large_hub":"Large Hub","medium_hub":"Medium Hub","small_hub":"Small Hub","non_hub":"Regional Airport"}
OPERATOR_TYPE_LABELS = {"city":"city-operated","county":"county-operated","port_authority":"port authority",
                        "state":"state-operated","joint_authority":"joint authority","federal":"federal"}
CATEGORY_LABELS = {
    "premises_liability":"Premises Liability","vehicle_liability":"Vehicle Liability",
    "federal_liability":"Federal / Aviation Law","workers_compensation":"Workers Compensation",
    "security_liability":"Security Liability","construction_liability":"Construction Liability",
    "environmental_liability":"Environmental Liability","medical_negligence":"Medical Negligence",
    "property_damage":"Property Damage","airline_liability":"Airline Liability",
    "negligence":"General Negligence","civil_rights":"Civil Rights",
}

ALL_ACCIDENTS = [
    ("slip-and-fall","\U0001f6b6","Slip & Fall"),
    ("jet-bridge-boarding","\u2708\ufe0f","Jet Bridge & Boarding"),
    ("baggage-claim","\U0001f9f3","Baggage Claim"),
    ("vehicle-accidents","\U0001f697","Vehicle & Baggage Cart"),
    ("security-checkpoint","\U0001f6c2","TSA / Security Checkpoint"),
    ("escalator-elevator","\u2b06\ufe0f","Escalator & Elevator"),
    ("food-court-restaurant","\u2615","Restaurant & Food Court"),
    ("shuttle-bus-ground-transportation","\U0001f68c","Shuttle Bus & Tram"),
    ("parking-lot-curbside","\U0001f17f\ufe0f","Parking Lot & Curbside"),
    ("assault-security-failure","\u26a0\ufe0f","Assault & Security"),
    ("disabled-passenger-assistance","\u267f","Disabled Passenger"),
    ("construction-zone","\U0001f6a7","Construction Zone"),
    ("boarding-stairs-ramps","\U0001fab5","Boarding Stairs"),
    ("worker-accidents","\U0001f477","Worker Injuries"),
    ("tarmac-airside","\U0001f6e9\ufe0f","Tarmac & Airside"),
    ("luggage-cart-conveyor","\U0001f6d2","Luggage Cart"),
    ("international-travel-claims","\U0001f310","International Claims"),
    ("toxic-exposure","\u2623\ufe0f","Toxic Exposure / PFAS"),
    ("lost-delayed-luggage","\U0001f4e6","Lost & Delayed Luggage"),
    ("rental-car-accidents","\U0001f699","Rental Car Facility"),
    ("medical-emergency-negligence","\U0001f3e5","Medical Emergency"),
    ("child-unaccompanied-minor","\U0001f476","Unaccompanied Minor"),
    ("slip-fall-wet-weather","\U0001f327\ufe0f","Wet Weather Falls"),
    ("airline-delay-cancellation-injury","\u23f3","Delay / Cancellation"),
    ("retail-shop-injuries","\U0001f3ea","Retail Shop Injuries"),
]


def load_data():
    with open(AIRPORTS_CSV)   as f: airports  = list(csv.DictReader(f))
    with open(ACCIDENTS_JSON) as f: accidents = json.load(f)
    with open(CROSSREF_CSV)   as f: crossref  = list(csv.DictReader(f))
    with open(PROFILES_JSON)  as f: profiles  = json.load(f)
    # Load all cv_*.json variation files and merge into one dict
    variations = {}
    cv_map = {
        "liable_section_labels":  "cv_liable_section_labels.json",
        "liable_section_titles":  "cv_liable_section_titles.json",
        "liable_intro_variants":  "cv_liable_intro_variants.json",
        "context_section_titles": "cv_context_section_titles.json",
        "context_section_labels": "cv_context_section_labels.json",
        "steps_section_titles":   "cv_steps_section_titles.json",
        "steps_section_labels":   "cv_steps_section_labels.json",
        "evidence_section_titles":"cv_evidence_section_titles.json",
        "legal_section_titles":   "cv_legal_section_titles.json",
        "legal_section_labels":   "cv_legal_section_labels.json",
        "other_accidents_titles": "cv_other_accidents_titles.json",
        "other_accidents_sub":    "cv_other_accidents_sub.json",
        "cta_titles":             "cv_cta_titles.json",
        "cta_sub":                "cv_cta_sub.json",
        "hero_eyebrow_variants":  "cv_hero_eyebrow.json",
        "form_titles":            "cv_form_titles.json",
        "what_to_do_intro":       "cv_what_to_do_intro.json",
        "evidence_intro":         "cv_evidence_intro.json",
        "trust_items":            "cv_trust_items.json",
        "sticky_bar_text":        "cv_sticky_bar_text.json",
        "form_submit_labels":     "cv_form_submit_labels.json",
        "nav_cta_labels":         "cv_nav_cta_labels.json",
        "phone_labels":           "cv_phone_labels.json",
        "step_titles":            "cv_step_titles.json",
        "step_bodies":            "cv_step_bodies.json",
        "evidence_item_notes":    "cv_evidence_item_notes.json",
        "form_hint":              "cv_form_hint.json",
        "form_card_titles":       "cv_form_card_titles.json",
        "form_card_subs":         "cv_form_card_subs.json",
    }
    for key, fname in cv_map.items():
        fpath = DATA_DIR / fname
        if fpath.exists():
            with open(fpath) as f:
                variations[key] = json.load(f)
        else:
            # fallback to old monolithic file if individual not found
            variations[key] = []
    return airports, accidents, crossref, profiles, variations


def write_page(path, html):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")


def pick(variations, key, seed, **fmt):
    """Pick a variant deterministically by seed, then format with airport/accident vars.
    Same airport+accident always gets the same variant — different airports get different ones.
    """
    pool = variations.get(key, ["{" + key + "}"])
    idx  = seed % len(pool)
    text = pool[idx]
    # Replace {placeholders} in the variant text
    for k, v in fmt.items():
        text = text.replace("{" + k + "}", str(v))
    # Clean up any unreplaced placeholders
    text = re.sub(r'\{[a-z_]+\}', '', text).strip()
    return text


def make_seed(airport_slug, accident_slug=""):
    """Deterministic seed from airport+accident slug — stable across rebuilds."""
    import hashlib
    h = hashlib.md5(f"{airport_slug}:{accident_slug}".encode()).digest()
    return int.from_bytes(h[:4], "big")


def render(template, context):
    # Always inject site-wide config so templates never have hardcoded values
    full_ctx = {
        "phone_display": PHONE_DISPLAY,
        "phone_digits":  PHONE_DIGITS,
        "site_name":     SITE_NAME,
    }
    full_ctx.update(context)

    def replace_if(m):
        k, inner = m.group(1).strip(), m.group(2)
        v = full_ctx.get(k)
        return inner if v and str(v) not in ("False","0","") else ""
    html = re.sub(r'\{\{#if\s+([\w_]+)\}\}(.*?)\{\{/if\}\}', replace_if, template, flags=re.DOTALL)
    for k, v in full_ctx.items():
        html = html.replace("{{" + k + "}}", str(v) if v is not None else "")
    html = re.sub(r'\{\{[\w_]+\}\}', '', html)
    return html


def parse_list(value):
    if isinstance(value, list): return value
    if isinstance(value, str):
        try:
            p = json.loads(value)
            if isinstance(p, list): return p
        except Exception: pass
        return [v.strip() for v in value.split('|') if v.strip()]
    return []


def li(text):
    return f'<li class="lp-li">{text}</li>'


def truncate(text, max_len=160):
    if len(text) <= max_len: return text
    return text[:max_len-3].rsplit(' ',1)[0] + '...'


def sol_short(sol_note):
    m = re.search(r'[\d.]+(?:[\-\u2013][\d.]+)?\s*years?', sol_note, re.I)
    return m.group(0) if m else "2-3 years"


# ── Content builders ──────────────────────────────────────────────────────────

def build_hero_intro(airport, acc, profile):
    name    = airport['airport_name']; city = airport['city']; state = airport['state']
    iata    = airport['iata_code'] or airport['faa_code']
    tier    = profile.get('passenger_tier', 'regional')
    climate = profile.get('climate_zone', 'temperate')
    snow    = profile.get('avg_annual_snow_inches', 0)
    notice  = profile.get('notice_of_claim_days', 0)
    op      = profile.get('airport_operator_name', f"{city} Airport Authority")
    const   = profile.get('construction_active', False)
    acc_name = acc['accident_name'].lower()

    if tier == 'mega':
        base = f"{name} is one of the busiest airports in the world — {acc_name} incidents occur here at a volume most travelers never hear about."
    elif tier == 'major':
        base = f"{name} handles millions of passengers annually, and {acc_name} incidents are a documented, recurring problem that airport management is legally obligated to address."
    elif tier == 'regional':
        base = f"{name} is a regional airport serving {city} — and while smaller than major hubs, it carries the same legal duty of care for {acc_name} incidents as any large commercial airport."
    else:
        base = f"{name} may be a smaller airport, but every commercial airport in {state} carries the same legal liability standards for {acc_name} victims."

    if climate == 'northern_winter' and snow > 30:
        climate_note = f" {city}'s heavy snowfall makes outdoor areas at {iata} among the most hazardous in the region during winter months."
    elif climate in ('coastal_rain', 'tropical'):
        climate_note = f" {city}'s wet climate means terminal entrances and curbside at {iata} are frequently wet year-round."
    elif climate == 'desert_dry':
        climate_note = f" {city}'s extreme heat creates additional risk at {iata}'s outdoor areas."
    else:
        climate_note = ""

    const_note = f" {name} is currently undergoing active construction, creating additional hazard zones adjacent to operating areas." if const else ""

    if notice > 0:
        notice_note = f" Critically, claims against {op} require a Notice of Claim within {notice} days — missing this permanently bars your case."
    else:
        notice_note = f" {op} does not require a pre-suit Notice of Claim, but evidence at {iata} still disappears within 72 hours."

    return f"{base}{climate_note}{const_note}{notice_note}"


def build_context_para(airport, acc, profile):
    name    = airport['airport_name']; iata = airport['iata_code'] or airport['faa_code']
    food    = profile.get('food_operator', 'food service operators')
    park    = profile.get('parking_operator', 'parking operators')
    handler = profile.get('ground_handler_primary', 'ground handling companies')
    op      = profile.get('airport_operator_name', f"{airport['city']} Airport Authority")
    acc_cat = acc.get('category', 'premises_liability')
    acc_id  = acc.get('accident_id', '')
    op_type = OPERATOR_TYPE_LABELS.get(profile.get('operator_type', 'city'), 'public entity')

    if acc_cat == 'premises_liability':
        if 'food' in acc_id:
            return (f"At {name}, food and beverage operations are managed by {food} — the actual legally liable party for restaurant "
                    f"and food court injuries at {iata}. When a slip occurs near a café or a customer suffers food poisoning, "
                    f"it is {food}'s insurance that responds, not the brand names on the signs.")
        elif 'parking' in acc_id:
            return (f"Parking facilities at {name} are operated by {park} — an independent contractor with its own liability policy "
                    f"separate from the airport authority. Injuries in parking garages or curbside areas at {iata} primarily involve "
                    f"{park} as defendant, alongside the airport authority that owns the underlying property.")
        else:
            return (f"At {name}, premises liability is shared between {op} (the {op_type} that owns the terminal) and the "
                    f"individual contractors operating within it. {food} is responsible for food court areas, {park} controls parking, "
                    f"and cleaning contractors manage the floors — each carries their own insurance and each can be named as a defendant.")
    elif acc_cat == 'vehicle_liability':
        return (f"Ground vehicle incidents at {name} primarily involve {handler}, the ground handling company responsible for "
                f"baggage tugs, carts, and ramp vehicles at {iata}. When a cart strikes a passenger, {handler}'s liability "
                f"insurance is the first target — with {op} and the airlines as secondary defendants.")
    elif acc_cat == 'federal_liability':
        return (f"Federal law governs this accident type at {name} in ways that differ materially from standard personal injury claims. "
                f"TSA checkpoint injuries require filing under the Federal Tort Claims Act before any lawsuit can proceed — "
                f"a step frequently missed by {iata} victims that permanently bars their claims.")
    elif acc_cat == 'workers_compensation':
        return (f"Worker injuries at {name} require a two-track legal strategy: workers compensation against the direct employer "
                f"(often {handler}), and third-party tort claims against any non-employer who contributed. At {iata}, multiple "
                f"contractors share the terminal — identifying all liable third parties beyond the direct employer is critical.")
    elif acc_cat == 'security_liability':
        return (f"Security failure cases at {name} depend on prior incident history at the specific location. If {iata}'s parking "
                f"garage or terminal area has a documented pattern of prior assaults, that history is the foundation of a negligent "
                f"security claim against {park}, the security contractor, and {op}.")
    else:
        return (f"At {name}, this type of accident claim involves {op} as the primary defendant — a {op_type} entity with its own "
                f"insurance, legal team, and procedural requirements that differ from standard personal injury cases in {airport['city']}.")


def build_liable_cards(airport, acc, profile):
    food    = profile.get('food_operator', 'Food service operator')
    park    = profile.get('parking_operator', 'Parking operator')
    handler = profile.get('ground_handler_primary', 'Ground handling company')
    handler2= profile.get('ground_handler_secondary') or ''
    security= profile.get('security_contractor', 'Security contractor')
    op      = profile.get('airport_operator_name', f"{airport['city']} Airport Authority")
    iata    = airport['iata_code'] or airport['faa_code']
    op_type = OPERATOR_TYPE_LABELS.get(profile.get('operator_type', 'city'), 'public entity')
    notice  = profile.get('notice_of_claim_days', 0)

    def enrich(party):
        p = party.lower()
        if 'airport authority' in p or 'airport operator' in p:
            note = f"As the {op_type} operating {airport['airport_name']}, {op} is liable for common areas and maintenance."
            if notice: note += f" Claims require Notice of Claim within {notice} days."
            return op, note
        elif 'food' in p or 'concess' in p or 'restaurant' in p:
            return food, f"{food} operates food & beverage at {iata} — the actual liable party, not the brand names displayed."
        elif 'parking' in p:
            return park, f"{park} manages parking at {iata} with its own liability policy independent of the airport."
        elif 'ground' in p or 'baggage handling' in p:
            note = f"{handler} is the primary ground handler at {iata}."
            if handler2: note += f" {handler2} also operates at {iata}."
            return handler, note
        elif 'security' in p and 'tsa' not in p:
            return security, f"{security} provides security at {iata} and is independently liable for security failures."
        elif 'airline' in p:
            return f"Airlines at {iata}", f"All carriers at {iata} are liable for gate areas, jet bridges, and boarding equipment."
        return party, ""

    cards = []
    for party in parse_list(acc.get('primary_liable_parties', []))[:4]:
        name, note = enrich(party)
        cards.append(
            f'<div class="lp-liable-card lp-liable-card--primary" role="listitem">'
            f'<div class="lp-liable-card__rank">Primary liable party</div>'
            f'<div class="lp-liable-card__name">{name}</div>'
            f'{"<div class=\'lp-liable-card__note\'>" + note + "</div>" if note else ""}'
            f'</div>')
    for party in parse_list(acc.get('secondary_liable_parties', []))[:3]:
        name, note = enrich(party)
        cards.append(
            f'<div class="lp-liable-card" role="listitem">'
            f'<div class="lp-liable-card__rank">Secondary liable party</div>'
            f'<div class="lp-liable-card__name">{name}</div>'
            f'{"<div class=\'lp-liable-card__note\'>" + note + "</div>" if note else ""}'
            f'</div>')
    return "\n".join(cards)


def build_hazard_items(airport, acc, profile):
    hazards = profile.get('airport_specific_hazards', [])
    zones   = profile.get('notable_accident_zones', [])
    items = [
        f'<div class="lp-hazard-item" role="listitem">'
        f'<span class="lp-hazard-item__icon" aria-hidden="true">\u26a0\ufe0f</span>'
        f'<span class="lp-hazard-item__text">{h}</span></div>'
        for h in hazards[:3]
    ]
    if zones:
        z = " and ".join(zones[:2])
        items.append(
            f'<div class="lp-hazard-item" role="listitem">'
            f'<span class="lp-hazard-item__icon" aria-hidden="true">\U0001f4cd</span>'
            f'<span class="lp-hazard-item__text">High-risk zones at this airport include: <strong>{z}</strong></span></div>')
    return "\n".join(items)


def build_evidence_timeline(acc, variations=None, seed=0):
    v = variations or {}
    ev = parse_list(acc.get('key_evidence', []))
    items_data = [
        ("CRITICAL\n24-72h",   "lp-evidence-item__time--critical", ev[0] if ev else "CCTV footage",       pick_pool(v,"evidence_item_notes","slot_1_cctv", seed)),
        ("HIGH\n1 week",       "lp-evidence-item__time--high",     ev[1] if len(ev)>1 else "Maintenance logs", pick_pool(v,"evidence_item_notes","slot_2_logs", seed+1)),
        ("HIGH\n1-2 weeks",    "lp-evidence-item__time--high",     ev[2] if len(ev)>2 else "Incident report",  pick_pool(v,"evidence_item_notes","slot_3_incident", seed+2)),
        ("STANDARD\nPreserve", "lp-evidence-item__time--standard", ev[3] if len(ev)>3 else "Medical records",  pick_pool(v,"evidence_item_notes","slot_4_medical", seed+3)),
    ]
    return "\n".join(
        f'<div class="lp-evidence-item" role="listitem">'
        f'<div class="lp-evidence-item__time {tc}">{tl}</div>'
        f'<div><div class="lp-evidence-item__name">{nm}</div>'
        f'<div class="lp-evidence-item__note">{nt}</div></div></div>'
        for tl, tc, nm, nt in items_data
    )


def build_steps(airport, acc, profile, variations=None, seed=0):
    v = variations or {}
    iata   = airport['iata_code'] or airport['faa_code']
    name   = airport['airport_name']
    notice = profile.get('notice_of_claim_days', 0)
    op     = profile.get('airport_operator_name', f"{airport['city']} Airport Authority")
    fmt = dict(airport=name, iata=iata, op=op,
               food_op=profile.get('food_operator','food operators'),
               park_op=profile.get('parking_operator','parking operators'),
               handler=profile.get('ground_handler_primary','ground handlers'),
               accident_lower=acc['accident_name'].lower())
    steps = [
        (pick_pool(v,"step_titles","medical",    seed,    **fmt),
         pick_pool(v,"step_bodies","medical",    seed+1,  **fmt)),
        (pick_pool(v,"step_titles","report",     seed+2,  **fmt),
         pick_pool(v,"step_bodies","report",     seed+3,  **fmt)),
        (pick_pool(v,"step_titles","photograph", seed+4,  **fmt),
         pick_pool(v,"step_bodies","photograph", seed+5,  **fmt)),
        (pick_pool(v,"step_titles","preservation",seed+6, **fmt),
         pick_pool(v,"step_bodies","preservation",seed+7, **fmt)),
    ]
    if notice > 0:
        steps.append((f"File Notice of Claim within {notice} days",
            f"Claims against {op} require a Notice of Claim within {notice} days. Missing this permanently bars your case — it is the single most critical deadline in your claim."))
    if acc.get('ftca_applies'):
        steps.append(("File TSA administrative claim first (FTCA)",
            "Before any lawsuit for TSA injuries, submit an administrative claim to DHS/TSA. Skipping this step permanently bars your case."))
    if acc.get('montreal_convention'):
        steps.append(("Note the 2-year Montreal Convention deadline",
            "International flight claims have a hard 2-year limitation period. There are no exceptions — file before this window closes."))
    return "\n".join(
        f'<div class="lp-step" role="listitem">'
        f'<div class="lp-step__num" aria-hidden="true">{i}</div>'
        f'<div><div class="lp-step__title">{t}</div><div class="lp-step__body">{b}</div></div></div>'
        for i,(t,b) in enumerate(steps[:6],1)
    )


def build_location_options(acc):
    zones = parse_list(acc.get('primary_airport_zones',[]))+parse_list(acc.get('secondary_airport_zones',[]))
    opts = ['<option value="">Select area where incident occurred...</option>']
    for z in zones:
        v = z.lower().replace(' ','-').replace('/','-')
        opts.append(f'<option value="{v}">{z}</option>')
    return "\n".join(opts)


def build_other_accidents(airport_slug, current_slug):
    return "\n".join(
        f'<a href="/{airport_slug}/{slug}/" class="lp-other-accident" role="listitem">'
        f'<span class="lp-other-accident__icon" aria-hidden="true">{icon}</span>'
        f'<span>{name}</span></a>'
        for slug, icon, name in ALL_ACCIDENTS if slug != current_slug
    )


def build_footer_airport_links(airport_slug, current_acc_slug):
    links = [f'<a href="/{airport_slug}/{slug}/">{icon} {name}</a>'
             for slug, icon, name in ALL_ACCIDENTS if slug != current_acc_slug]
    return "\n".join(links[:5])


def build_footer_accident_links(acc_slug, airport_slug, airports):
    major = [a for a in airports if a['type'] == 'large_hub' and a['slug'] != airport_slug][:6]
    return "\n".join(f'<a href="/{a["slug"]}/{acc_slug}/">{a["iata_code"] or a["faa_code"]} — {a["city"]}</a>' for a in major)


def build_banners(profile, acc):
    const = ""
    if profile.get('construction_active'):
        notes = profile.get('construction_notes', 'Active construction project')
        const = (f'<div class="lp-construction-banner">'
                 f'<span class="lp-construction-banner__icon" aria-hidden="true">\U0001f6a7</span>'
                 f'<div><div class="lp-construction-banner__title">Active Construction at This Airport</div>'
                 f'<div class="lp-construction-banner__text">{notes}</div></div></div>')
    alt = ""
    if profile.get('high_altitude_medical_risk') and acc.get('accident_id') == 'medical-emergency-negligence':
        elev = profile.get('elevation_ft', 5000)
        alt = (f'<div class="lp-altitude-banner">'
               f'<span class="lp-altitude-banner__icon" aria-hidden="true">\u26f0\ufe0f</span>'
               f'<div><div class="lp-altitude-banner__title">High Altitude Medical Risk ({elev:,} ft)</div>'
               f'<div class="lp-altitude-banner__text">This airport elevation significantly increases cardiac and respiratory stress.</div>'
               f'</div></div>')
    pfas = ""
    if profile.get('pfas_risk') == 'high' and acc.get('accident_id') == 'toxic-exposure':
        pfas = ('<div class="lp-construction-banner lp-construction-banner--pfas">'
                '<span class="lp-construction-banner__icon" aria-hidden="true">\u2623\ufe0f</span>'
                '<div><div class="lp-construction-banner__title" style="color:#FCA5A5;">Elevated PFAS Risk at This Airport</div>'
                '<div class="lp-construction-banner__text">Documented PFAS contamination from firefighting foam operations at this facility.</div>'
                '</div></div>')
    return const, alt, pfas


def build_notice_box(profile):
    days = profile.get('notice_of_claim_days', 0)
    if not days: return ""
    op = profile.get('airport_operator_name', 'the airport authority')
    return (f'<div class="lp-notice-box" role="alert">'
            f'<div class="lp-notice-box__title">\u26a0 Notice of Claim Required — {days} Days</div>'
            f'<p class="lp-notice-box__text">Claims against {op} require a formal Notice of Claim within {days} days. '
            f'Missing this deadline permanently bars your case with no exceptions.</p></div>')


# ── Hub generators ────────────────────────────────────────────────────────────

def airport_card_html(a):
    slug=a['slug']; iata=a['iata_code'] or a['faa_code']
    tcss={"large_hub":"type--large","medium_hub":"type--medium","small_hub":"type--small","non_hub":"type--non"}.get(a['type'],"type--non")
    lc="sh-airport-card--large" if a['type']=='large_hub' else ""
    return (f'<a href="/{slug}/" class="sh-airport-card {lc}" role="listitem">'
            f'<div class="sh-airport-card__code">{iata}</div>'
            f'<div class="sh-airport-card__info">'
            f'<div class="sh-airport-card__name">{a["airport_name"]}</div>'
            f'<div class="sh-airport-card__city">{a["city"]}</div></div>'
            f'<span class="sh-airport-card__type {tcss}">{TYPE_LABELS.get(a["type"],"Airport")}</span>'
            f'<span class="sh-airport-card__arrow" aria-hidden="true">&rarr;</span></a>')

def nearby_html(a):
    return (f'<a href="/{a["slug"]}/" class="airport-item" role="listitem">'
            f'<div class="airport-item__code">{a["iata_code"] or a["faa_code"]}</div>'
            f'<div><div class="airport-item__city">{a["city"]}</div>'
            f'<div class="airport-item__name">{a["airport_name"]}</div></div></a>')

def zone_card(z, primary=True):
    return (f'<div class="card" role="listitem">'
            f'<div class="aoh-zone__tag">{"Primary Zone" if primary else "Secondary Zone"}</div>'
            f'<div class="aoh-zone__title">{z}</div></div>')

def company_pill(c):
    return f'<div class="aoh-company-pill" role="listitem">{c}</div>'

def airport_link(a, acc_slug):
    return (f'<a href="/{a["slug"]}/{acc_slug}/" class="airport-item" role="listitem">'
            f'<div class="airport-item__code">{a["iata_code"] or a["faa_code"]}</div>'
            f'<div><div class="airport-item__city">{a["city"]}, {a["state_code"]}</div>'
            f'<div class="airport-item__name">{a["airport_name"]}</div></div></a>')


def generate_states(airports, tmpl, dist, filter_code=None):
    by_state = defaultdict(list)
    for a in airports: by_state[a['state']].append(a)
    n = 0
    for state_name, sa in sorted(by_state.items()):
        sc = sa[0]['state_code']
        if filter_code and sc.upper() != filter_code.upper(): continue
        legal = STATE_LEGAL.get(state_name, DEFAULT_LEGAL)
        lg=[a for a in sa if a['type']=='large_hub']; me=[a for a in sa if a['type']=='medium_hub']
        sm=[a for a in sa if a['type']=='small_hub'];  rg=[a for a in sa if a['type']=='non_hub']
        opts="\n".join(f'<option value="{a["slug"]}">{a["airport_name"]} ({a["iata_code"] or a["faa_code"]})</option>'
                       for a in sorted(sa,key=lambda x:(x['type']!='large_hub',x['airport_name'])))
        foot=sorted(sa,key=lambda x:({"large_hub":0,"medium_hub":1,"small_hub":2,"non_hub":3}[x['type']],x['airport_name']))[:6]
        ctx={
            "state":state_name,"state_code":sc,"state_code_lower":sc.lower(),
            "airport_count":len(sa),"large_hub_count":len(lg),"medium_hub_count":len(me),
            "small_hub_count":len(sm),"regional_count":len(rg),
            "sol":legal["sol"],"notice_deadline":legal["notice"],"gov_entity":legal["gov"],
            "large_hubs_html":"\n".join(airport_card_html(a) for a in lg),
            "medium_hubs_html":"\n".join(airport_card_html(a) for a in me),
            "small_hubs_html":"\n".join(airport_card_html(a) for a in sm),
            "regional_hubs_html":"\n".join(airport_card_html(a) for a in rg),
            "state_airport_options_html":opts,
            "footer_airports_html":"\n".join(f'<a href="/{a["slug"]}/">{a["airport_name"]} ({a["iata_code"] or a["faa_code"]})</a>' for a in foot),
        }
        write_page(dist/"state"/sc.lower()/"index.html", render(tmpl, ctx))
        n += 1
        print(f"  \u2713 state/{sc.lower()}/ \u2014 {state_name} ({len(sa)} airports)")
    return n


def generate_airport_hubs(airports, profiles, tmpl, dist, filter_iata=None):
    by_state = defaultdict(list)
    for a in airports: by_state[a['state']].append(a)
    n = 0
    for airport in airports:
        iata = airport['iata_code'] or airport['faa_code']
        if filter_iata and iata.upper() != filter_iata.upper(): continue
        profile = profiles.get(airport['slug'], {})
        legal   = STATE_LEGAL.get(airport['state'], DEFAULT_LEGAL)
        nearby  = [a for a in by_state[airport['state']] if a['slug'] != airport['slug']]
        ctx = {
            "airport_name":airport['airport_name'],"airport_slug":airport['slug'],
            "city":airport['city'],"state":airport['state'],
            "state_code":airport['state_code'],"state_code_lower":airport['state_code'].lower(),
            "iata_code":iata,"faa_code":airport['faa_code'],
            "airport_type_label":TYPE_LABELS.get(airport['type'],"Airport"),
            "airport_operator":profile.get('airport_operator_name', legal["gov"]),
            "nearby_airports_html":"\n".join(nearby_html(a) for a in nearby[:12]),
        }
        write_page(dist/airport['slug']/"index.html", render(tmpl, ctx))
        n += 1
        if n % 50 == 0: print(f"  ... {n} airport hubs generated")
    print(f"  \u2713 {n} airport hub pages generated")
    return n


def generate_accident_hubs(accidents, airports, tmpl, dist, filter_slug=None):
    sorted_a = sorted(airports, key=lambda x:({"large_hub":0,"medium_hub":1,"small_hub":2,"non_hub":3}[x['type']],x['airport_name']))
    featured = [a for a in sorted_a if a['type'] in ('large_hub','medium_hub')][:40]
    n = 0
    for acc in accidents:
        if filter_slug and acc['slug'] != filter_slug: continue
        ftca_a = ('<div class="alert-box"><div class="alert-box__title">\u26a0 FTCA Required</div>'
                  '<p class="alert-box__text">TSA injury claims must file administratively with DHS before any lawsuit.</p></div>') if acc.get('ftca_applies') else ""
        mont_a = ('<div class="warning-box"><div class="warning-box__title">Montreal Convention \u2014 2-Year Hard Deadline</div>'
                  '<p class="warning-box__text">International flight claims: 2-year absolute deadline, no exceptions.</p></div>') if acc.get('montreal_convention') else ""
        fn = "FTCA Federal Filing Required" if acc.get('ftca_applies') else "Montreal Convention Applies" if acc.get('montreal_convention') else "Federal aviation law applies" if acc.get('federal_law_involved') else "All liable parties pursued"
        ctx = {
            "accident_name":acc['accident_name'],"accident_slug":acc['slug'],
            "category_label":CATEGORY_LABELS.get(acc.get('category',''),'Personal Injury'),
            "description":acc.get('description',''),"frequency_label":acc.get('frequency_label','Common'),
            "danger_label":acc.get('danger_label','High'),"severity_score":acc.get('severity_score',7),
            "average_settlement_range":acc.get('average_settlement_range','Varies'),
            "sol_short":sol_short(acc.get('statute_of_limitations_note','2-3 years')),
            "evidence_preservation_urgency_upper":str(acc.get('evidence_preservation_urgency','HIGH')).upper(),
            "evidence_notes":acc.get('evidence_notes',''),"federal_note":fn,
            "severity_class":"pill--danger" if acc.get('danger_level') in ('very_high','extreme') else "pill--warning",
            "freq_class":"pill--danger" if acc.get('frequency')=='very_common' else "pill--warning" if acc.get('frequency')=='common' else "pill--neutral",
            "primary_liable_html":"\n".join(f'<li>{li(p)}</li>' for p in parse_list(acc.get('primary_liable_parties',[]))),
            "secondary_liable_html":"\n".join(f'<li>{li(p)}</li>' for p in parse_list(acc.get('secondary_liable_parties',[]))),
            "compensable_damages_html":"\n".join(f'<li>{li(d)}</li>' for d in parse_list(acc.get('compensable_damages',[]))),
            "common_injuries_html":"\n".join(f'<li>{li(i)}</li>' for i in parse_list(acc.get('common_injuries',[]))),
            "key_evidence_html":"\n".join(f'<li>{li(e)}</li>' for e in parse_list(acc.get('key_evidence',[]))),
            "defenses_html":"\n".join(f'<li>{li(d)}</li>' for d in parse_list(acc.get('liable_party_defenses',[]))),
            "counter_html":"\n".join(f'<li>{li(c)}</li>' for c in parse_list(acc.get('plaintiff_counter_strategies',[]))),
            "primary_zones_html":"\n".join(zone_card(z,True) for z in parse_list(acc.get('primary_airport_zones',[]))[:4]),
            "secondary_zones_html":"\n".join(zone_card(z,False) for z in parse_list(acc.get('secondary_airport_zones',[]))[:4]),
            "companies_html":"\n".join(company_pill(c) for c in parse_list(acc.get('airport_businesses_involved',[]))[:24]),
            "categories_html":"\n".join(f'<span class="aoh-category-tag">{c}</span>' for c in parse_list(acc.get('business_categories',[]))),
            "airport_links_html":"\n".join(airport_link(a,acc['slug']) for a in featured),
            "ftca_alert":ftca_a,"montreal_alert":mont_a,
        }
        write_page(dist/acc['slug']/"index.html", render(tmpl, ctx))
        n += 1
        print(f"  \u2713 {acc['slug']}/ \u2014 {acc['accident_name']}")
    return n


# ── Leaf page generator ───────────────────────────────────────────────────────

def generate_leaf_pages(airports, accidents, profiles, tmpl, dist,
                        filter_airport=None, filter_accident=None, phase=None):
    phase_types = {1:{'large_hub'},2:{'medium_hub'},3:{'small_hub'},4:{'non_hub'}}
    allowed = phase_types.get(phase) if phase else None
    type_order = {"large_hub":0,"medium_hub":1,"small_hub":2,"non_hub":3}
    sorted_airports = sorted(airports, key=lambda x:(type_order.get(x['type'],4),x['airport_name']))
    n = 0

    for airport in sorted_airports:
        iata = airport['iata_code'] or airport['faa_code']
        if filter_airport and iata.upper() != filter_airport.upper(): continue
        if allowed and airport['type'] not in allowed: continue

        profile    = profiles.get(airport['slug'], {})
        state_leg  = STATE_LEGAL.get(airport['state'], DEFAULT_LEGAL)
        op         = profile.get('airport_operator_name', f"{airport['city']} Airport Authority")
        notice_days= profile.get('notice_of_claim_days', 0)

        for acc in accidents:
            if filter_accident and acc['slug'] != filter_accident: continue

            canonical = f"{BASE_URL}/{airport['slug']}/{acc['slug']}/"
            pg_title  = f"{acc['accident_name']} at {airport['airport_name']} | {airport['city']}, {airport['state']} | Free Legal Help"
            meta_desc = truncate(
                f"Injured in a {acc['accident_name'].lower()} at {airport['airport_name']} in {airport['city']}, {airport['state']}? "
                f"Find out who is liable, what evidence to preserve, and what your case is worth. "
                f"Free consultation — {state_leg['sol']} statute of limitations in {airport['state']}."
            )

            const_b, alt_b, pfas_b = build_banners(profile, acc)

            if notice_days > 0:
                notice_label = "Notice of claim"; notice_value = f"{notice_days} days"; notice_color = "var(--color-danger)"
            else:
                notice_label = "Notice required"; notice_value = "Not required"; notice_color = "var(--color-success)"

            ftca_alert = ('<div class="alert-box"><div class="alert-box__title">\u26a0 FTCA — Mandatory Administrative Filing First</div>'
                          '<p class="alert-box__text">TSA injuries require administrative filing with DHS/TSA before any lawsuit. Missing this step permanently bars your case.</p></div>') if acc.get('ftca_applies') else ""
            montreal_alert = ('<div class="warning-box"><div class="warning-box__title">Montreal Convention \u2014 2-Year Hard Deadline</div>'
                               '<p class="warning-box__text">International flight claims: strict 2-year deadline, no exceptions, no extensions.</p></div>') if acc.get('montreal_convention') else ""

            injuries_h     = "\n".join(li(i) for i in parse_list(acc.get('common_injuries',[]))[:6])
            damages_h      = "\n".join(li(d) for d in parse_list(acc.get('compensable_damages',[]))[:6])
            value_h        = "\n".join(li(f) for f in parse_list(acc.get('factors_increasing_value',[]))[:5])
            op_type_label  = OPERATOR_TYPE_LABELS.get(profile.get('operator_type','city'),'public entity')

            faq_liable   = (f"At {airport['airport_name']}, primary liable parties for {acc['accident_name'].lower()} include "
                            f"{op} as airport operator and any contractors managing the zone where the incident occurred. Multiple parties often share liability.")
            faq_deadline = (f"In {airport['state']}, the statute of limitations is {state_leg['sol']}. "
                            + (f"Claims against {op} also require Notice of Claim within {notice_days} days. " if notice_days else "")
                            + ("TSA injuries require FTCA filing first. " if acc.get('ftca_applies') else "")
                            + "Contact an attorney immediately.")
            faq_evidence = (f"After a {acc['accident_name'].lower()} at {airport['airport_name']}, preserve: "
                            f"{chr(44).join(parse_list(acc.get('key_evidence',[]))[:4])}. "
                            f"CCTV at {iata} overwrites in 24-72 hours.")

            ctx = {
                "page_title":pg_title,"meta_description":meta_desc,"canonical_url":canonical,
                "og_title":f"{acc['accident_name']} at {airport['airport_name']} | Free Legal Help",
                "airport_name":airport['airport_name'],"airport_slug":airport['slug'],
                "city":airport['city'],"state":airport['state'],
                "state_code":airport['state_code'],"state_code_lower":airport['state_code'].lower(),
                "iata_code":iata,"faa_code":airport['faa_code'],
                "airport_type_label":TYPE_LABELS.get(airport['type'],'Airport'),
                "airport_operator_name":op,"operator_type_label":op_type_label,
                "accident_name":acc['accident_name'],"accident_name_lower":acc['accident_name'].lower(),
                "accident_slug":acc['slug'],"severity_score":acc.get('severity_score',7),
                "average_settlement_range":acc.get('average_settlement_range','Varies'),
                "legal_standard":acc.get('legal_standard','Negligence'),
                "liability_notes":acc.get('liability_notes',''),"frequency_label":acc.get('frequency_label','Common'),
                "sol_state":state_leg['sol'],
                "notice_label":notice_label,"notice_value":notice_value,"notice_color":notice_color,
                "nearby_courthouse":profile.get('nearby_courthouse','U.S. District Court'),
                "h1_title":f"{acc['accident_name']} at {airport['airport_name']}",
                "hero_intro":build_hero_intro(airport,acc,profile),
                "airport_accident_context_para":build_context_para(airport,acc,profile),
                "liable_intro_para":(f"At {airport['airport_name']}, responsibility for {acc['accident_name'].lower()} incidents "
                                     f"is divided among {op}, {profile.get('food_operator','food operators')}, "
                                     f"{profile.get('parking_operator','parking operators')}, and {profile.get('ground_handler_primary','ground handlers')}. "
                                     f"Identifying every liable party before the statute of limitations expires is critical."),
                "what_to_do_intro":f"After a {acc['accident_name'].lower()} at {airport['airport_name']}, the next 72 hours are critical. Evidence disappears fast and legal deadlines begin immediately.",
                "evidence_intro":f"The single most important action after any accident at {airport['airport_name']} is to initiate evidence preservation before records are overwritten or sealed.",
                "construction_banner_html":const_b,"altitude_banner_html":alt_b,"pfas_banner_html":pfas_b,
                "hazard_items_html":build_hazard_items(airport,acc,profile),
                "liable_cards_html":build_liable_cards(airport,acc,profile),
                "notice_box_html":build_notice_box(profile),
                "evidence_timeline_html":build_evidence_timeline(acc),
                "steps_html":build_steps(airport,acc,profile),
                "location_options_html":build_location_options(acc),
                "injuries_html":injuries_h,"damages_html":damages_h,"value_factors_html":value_h,
                "ftca_alert_html":ftca_alert,"montreal_alert_html":montreal_alert,
                "other_accidents_html":build_other_accidents(airport['slug'],acc['slug']),
                "footer_airport_links_html":build_footer_airport_links(airport['slug'],acc['slug']),
                "footer_accident_links_html":build_footer_accident_links(acc['slug'],airport['slug'],airports),
                "form_title":f"Free {iata} {acc['accident_name']} Review",
                "form_subtitle":f"Tell us what happened at {airport['airport_name']}. An attorney reviews within 24 hours.",
                "form_placeholder":f"Describe what happened at {airport['airport_name']}...",
                "submit_label":f"Review My {iata} Case \u2192",
                "cta_title":f"Injured in a {acc['accident_name']} at {airport['airport_name']}?",
                "cta_sub":f"Evidence at {iata} disappears within 72 hours and {airport['state']}'s {state_leg['sol']} statute of limitations has already started.",
                "cta_btn_label":f"Start My Free {iata} Case Review \u2192",
                "faq_liable_answer":faq_liable,"faq_deadline_answer":faq_deadline,"faq_evidence_answer":faq_evidence,
            }
            # Collect (path, html) pairs then batch-write in parallel
            out_path = dist/airport['slug']/acc['slug']/"index.html"
            yield out_path, render(tmpl, ctx)

    return  # generator exhausted


def generate_leaf_pages(airports, accidents, profiles, variations, tmpl, dist,
                        filter_airport=None, filter_accident=None, phase=None):
    """Wrapper that runs the leaf generator with parallel file writes."""
    phase_types = {1:{'large_hub'},2:{'medium_hub'},3:{'small_hub'},4:{'non_hub'}}
    allowed = phase_types.get(phase) if phase else None

    n = 0
    def _write(args):
        path, html = args
        write_page(path, html)
        return 1

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = []
        for airport in sorted(airports, key=lambda x:({"large_hub":0,"medium_hub":1,"small_hub":2,"non_hub":3}.get(x['type'],4),x['airport_name'])):
            iata = airport['iata_code'] or airport['faa_code']
            if filter_airport and iata.upper() != filter_airport.upper(): continue
            if allowed and airport['type'] not in allowed: continue
            profile   = profiles.get(airport['slug'], {})
            state_leg = STATE_LEGAL.get(airport['state'], DEFAULT_LEGAL)
            op        = profile.get('airport_operator_name', f"{airport['city']} Airport Authority")
            notice_days = profile.get('notice_of_claim_days', 0)
            for acc in accidents:
                if filter_accident and acc['slug'] != filter_accident: continue
                canonical = f"{BASE_URL}/{airport['slug']}/{acc['slug']}/"
                pg_title  = f"{acc['accident_name']} at {airport['airport_name']} | {airport['city']}, {airport['state']} | Free Legal Help"
                meta_desc = truncate(
                    f"Injured in a {acc['accident_name'].lower()} at {airport['airport_name']} in {airport['city']}, {airport['state']}? "
                    f"Find out who is liable, what evidence to preserve, and what your case is worth. "
                    f"Free consultation — {state_leg['sol']} statute of limitations in {airport['state']}."
                )
                seed = make_seed(airport['slug'], acc['slug'])
                const_b, alt_b, pfas_b = build_banners(profile, acc)
                if notice_days > 0:
                    notice_label = "Notice of claim"; notice_value = f"{notice_days} days"; notice_color = "var(--color-danger)"
                else:
                    notice_label = "Notice required"; notice_value = "Not required"; notice_color = "var(--color-success)"
                ftca_alert = ('<div class="alert-box"><div class="alert-box__title">\u26a0 FTCA \u2014 Mandatory Administrative Filing First</div>'
                              '<p class="alert-box__text">TSA injuries require administrative filing with DHS/TSA before any lawsuit. Missing this step permanently bars your case.</p></div>') if acc.get('ftca_applies') else ""
                montreal_alert = ('<div class="warning-box"><div class="warning-box__title">Montreal Convention \u2014 2-Year Hard Deadline</div>'
                                   '<p class="warning-box__text">International flight claims: strict 2-year deadline, no exceptions, no extensions.</p></div>') if acc.get('montreal_convention') else ""
                injuries_h = "\n".join(li(i) for i in parse_list(acc.get('common_injuries',[]))[:6])
                damages_h  = "\n".join(li(d) for d in parse_list(acc.get('compensable_damages',[]))[:6])
                value_h    = "\n".join(li(f) for f in parse_list(acc.get('factors_increasing_value',[]))[:5])
                op_type_label = OPERATOR_TYPE_LABELS.get(profile.get('operator_type','city'),'public entity')
                faq_liable   = (f"At {airport['airport_name']}, primary liable parties for {acc['accident_name'].lower()} include "
                                f"{op} as airport operator and any contractors managing the zone where the incident occurred.")
                faq_deadline = (f"In {airport['state']}, the statute of limitations is {state_leg['sol']}. "
                                + (f"Claims against {op} also require Notice of Claim within {notice_days} days. " if notice_days else "")
                                + ("TSA injuries require FTCA filing first. " if acc.get('ftca_applies') else "")
                                + "Contact an attorney immediately.")
                faq_evidence = (f"After a {acc['accident_name'].lower()} at {airport['airport_name']}, preserve: "
                                f"{chr(44).join(parse_list(acc.get('key_evidence',[]))[:4])}. CCTV at {iata} overwrites in 24-72 hours.")
                ctx = {
                    "page_title":pg_title,"meta_description":meta_desc,"canonical_url":canonical,
                    "og_title":f"{acc['accident_name']} at {airport['airport_name']} | Free Legal Help",
                    "airport_name":airport['airport_name'],"airport_slug":airport['slug'],
                    "city":airport['city'],"state":airport['state'],
                    "state_code":airport['state_code'],"state_code_lower":airport['state_code'].lower(),
                    "iata_code":iata,"faa_code":airport['faa_code'],
                    "airport_type_label":TYPE_LABELS.get(airport['type'],'Airport'),
                    "airport_operator_name":op,"operator_type_label":op_type_label,
                    "accident_name":acc['accident_name'],"accident_name_lower":acc['accident_name'].lower(),
                    "accident_slug":acc['slug'],"severity_score":acc.get('severity_score',7),
                    "average_settlement_range":acc.get('average_settlement_range','Varies'),
                    "legal_standard":acc.get('legal_standard','Negligence'),
                    "liability_notes":acc.get('liability_notes',''),"frequency_label":acc.get('frequency_label','Common'),
                    "sol_state":state_leg['sol'],
                    "notice_label":notice_label,"notice_value":notice_value,"notice_color":notice_color,
                    "nearby_courthouse":profile.get('nearby_courthouse','U.S. District Court'),
                    "h1_title":f"{acc['accident_name']} at {airport['airport_name']}",
                    "hero_intro":build_hero_intro(airport,acc,profile),
                    "airport_accident_context_para":build_context_para(airport,acc,profile),
                    "liable_intro_para":(f"At {airport['airport_name']}, responsibility for {acc['accident_name'].lower()} incidents "
                                         f"is divided among {op}, {profile.get('food_operator','food operators')}, "
                                         f"{profile.get('parking_operator','parking operators')}, and {profile.get('ground_handler_primary','ground handlers')}. "
                                         f"Identifying every liable party before the statute of limitations expires is critical."),
                    "what_to_do_intro":f"After a {acc['accident_name'].lower()} at {airport['airport_name']}, the next 72 hours are critical. Evidence disappears fast and legal deadlines begin immediately.",
                    "evidence_intro":f"The single most important action after any accident at {airport['airport_name']} is to initiate evidence preservation before records are overwritten or sealed.",
                    "construction_banner_html":const_b,"altitude_banner_html":alt_b,"pfas_banner_html":pfas_b,
                    "hazard_items_html":build_hazard_items(airport,acc,profile),
                    "liable_cards_html":build_liable_cards(airport,acc,profile),
                    "notice_box_html":build_notice_box(profile),
                    "steps_html":build_steps(airport,acc,profile,variations,seed+20),
                    "evidence_timeline_html":build_evidence_timeline(acc,variations,seed+30),
                    "location_options_html":build_location_options(acc),
                    "injuries_html":injuries_h,"damages_html":damages_h,"value_factors_html":value_h,
                    "ftca_alert_html":ftca_alert,"montreal_alert_html":montreal_alert,
                    "other_accidents_html":build_other_accidents(airport['slug'],acc['slug']),
                    "footer_airport_links_html":build_footer_airport_links(airport['slug'],acc['slug']),
                    "footer_accident_links_html":build_footer_accident_links(acc['slug'],airport['slug'],airports),
                    # ── Variation picks (deterministic per airport+accident) ────
                    "hero_eyebrow":      pick(variations,"hero_eyebrow_variants",  seed,
                                              city=airport['city'], state=airport['state'],
                                              iata=iata, airport=airport['airport_name']),
                    "liable_section_label": pick(variations,"liable_section_labels", seed,
                                              iata=iata, airport=airport['airport_name']),
                    "liable_section_title": pick(variations,"liable_section_titles", seed,
                                              iata=iata, airport=airport['airport_name'],
                                              accident=acc['accident_name']),
                    "liable_intro_para": pick(variations,"liable_intro_variants",   seed,
                                              airport=airport['airport_name'], iata=iata,
                                              accident_lower=acc['accident_name'].lower(),
                                              op=op, food_op=profile.get('food_operator','food operators'),
                                              park_op=profile.get('parking_operator','parking operators'),
                                              handler=profile.get('ground_handler_primary','ground handlers'),
                                              op_type=op_type_label),
                    "context_section_title": pick(variations,"context_section_titles", seed+1,
                                              airport=airport['airport_name'], iata=iata,
                                              accident=acc['accident_name']),
                    "steps_section_title":   pick(variations,"steps_section_titles",  seed+2,
                                              airport=airport['airport_name'], iata=iata,
                                              accident=acc['accident_name']),
                    "evidence_section_title":pick(variations,"evidence_section_titles",seed+3,
                                              airport=airport['airport_name'], iata=iata),
                    "legal_section_title":   pick(variations,"legal_section_titles",  seed+4,
                                              airport=airport['airport_name'], iata=iata,
                                              accident=acc['accident_name']),
                    "other_accidents_title": pick(variations,"other_accidents_titles", seed+5,
                                              airport=airport['airport_name'], iata=iata),
                    "cta_title":            pick(variations,"cta_titles",              seed+6,
                                              airport=airport['airport_name'], iata=iata,
                                              accident=acc['accident_name']),
                    "form_title":           pick(variations,"form_titles",             seed+7,
                                              airport=airport['airport_name'], iata=iata,
                                              accident=acc['accident_name']),
                    "context_section_label":pick(variations,"context_section_labels",  seed+8,
                                              iata=iata),
                    "steps_section_label":   pick(variations,"steps_section_labels",   seed+9),
                    "legal_section_label":   pick(variations,"legal_section_labels",   seed+10,
                                              state=airport['state']),
                    "other_accidents_sub":   pick(variations,"other_accidents_sub",    seed+11,
                                              airport=airport['airport_name'], iata=iata),
                    "cta_sub":              pick(variations,"cta_sub",                 seed+12,
                                              airport=airport['airport_name'], iata=iata,
                                              accident_lower=acc['accident_name'].lower(),
                                              state=airport['state'], sol=state_leg['sol']),
                    "what_to_do_intro":     pick(variations,"what_to_do_intro",        seed+13,
                                              airport=airport['airport_name'], iata=iata,
                                              accident_lower=acc['accident_name'].lower()),
                    "evidence_intro":       pick(variations,"evidence_intro",          seed+14,
                                              airport=airport['airport_name'], iata=iata,
                                              accident_lower=acc['accident_name'].lower()),
                    "sticky_bar_text":      pick(variations,"sticky_bar_text",         seed+15,
                                              airport=airport['airport_name'], iata=iata,
                                              accident_lower=acc['accident_name'].lower(),
                                              accident=acc['accident_name']),
                    "submit_label":         pick(variations,"form_submit_labels",      seed+16,
                                              airport=airport['airport_name'], iata=iata,
                                              accident=acc['accident_name']),
                    "nav_cta_label":        pick(variations,"nav_cta_labels",          seed+17),
                    "phone_label":          pick(variations,"phone_labels",            seed+18),
                    # ── Trust strip (5 items from pools) ───────────────────────
                    **dict(zip(
                        ["trust_item_1","trust_item_2","trust_item_3","trust_item_4","trust_item_5"],
                        build_trust_items(variations,seed,airport['state'],iata)
                    )),
                    "form_card_title":      pick(variations,"form_card_titles",       seed+19,
                                              airport=airport['airport_name'], iata=iata,
                                              accident=acc['accident_name']),
                    "form_card_sub":        pick(variations,"form_card_subs",         seed+19,
                                              airport=airport['airport_name'], iata=iata,
                                              accident_lower=acc['accident_name'].lower()),
                    "form_hint":            pick(variations,"form_hint",               seed+20,
                                              airport=airport['airport_name'], iata=iata),
                    # ── Static fields ──────────────────────────────────────────
                    "form_placeholder":f"Describe what happened at {airport['airport_name']}...",
                    "cta_btn_label":f"Start My Free {iata} Case Review \u2192",
                    "faq_liable_answer":faq_liable,"faq_deadline_answer":faq_deadline,"faq_evidence_answer":faq_evidence,
                    "form_webhook": FORM_WEBHOOK,
                    "gtm_id": GTM_ID,
                }
                html = render(tmpl, ctx)
                out_path = dist/airport['slug']/acc['slug']/"index.html"
                futures.append(pool.submit(_write, (out_path, html)))
                if len(futures) % 500 == 0:
                    print(f"    ... {len(futures):,} leaf pages queued")

        for fut in as_completed(futures):
            n += fut.result()

    return n


def pick_pool(variations, key, pool_key, seed, **fmt):
    """Pick from a nested pool (variations[key][pool_key])."""
    outer = variations.get(key, {})
    if isinstance(outer, dict):
        pool = outer.get(pool_key, [""])
    else:
        pool = outer
    idx  = seed % len(pool) if pool else 0
    text = pool[idx] if pool else ""
    for k, v in fmt.items():
        text = text.replace("{" + k + "}", str(v))
    text = re.sub(r'\{[a-z_]+\}', '', text).strip()
    return text


def build_trust_items(variations, seed, state, iata):
    """Build 5 trust strip items from variation pools."""
    pools = variations.get("trust_items", {})
    slots = [
        ("slot_1_fees",       {}),
        ("slot_2_review",     {}),
        ("slot_3_evidence",   {}),
        ("slot_4_jurisdiction",{"state": state}),
        ("slot_5_airport",    {"iata": iata}),
    ]
    items = []
    for i, (slot_key, fmt) in enumerate(slots):
        pool = pools.get(slot_key, ["No fees unless you win"])
        idx  = (seed + i * 7) % len(pool)
        text = pool[idx]
        for k, v in fmt.items():
            text = text.replace("{" + k + "}", str(v))
        items.append(text)
    return items


def copy_assets(dist):
    shutil.copy2(HOMEPAGE_SRC, dist/"index.html"); print("  \u2713 index.html")
    dest = dist/"assets"
    if dest.exists(): shutil.rmtree(dest)
    shutil.copytree(ASSETS_SRC, dest); print("  \u2713 assets/")


def generate_sitemap(airports, accidents, dist):
    """Generate dist/sitemap.xml with all page URLs, priorities, and lastmod."""
    from datetime import date
    today = date.today().isoformat()
    urls = []

    def add(loc, priority, changefreq):
        urls.append(f'  <url>\n    <loc>{BASE_URL}{loc}</loc>\n    <lastmod>{today}</lastmod>\n    <changefreq>{changefreq}</changefreq>\n    <priority>{priority}</priority>\n  </url>')

    # Homepage
    add("/", "1.0", "weekly")

    # State hubs
    by_state = defaultdict(list)
    for a in airports: by_state[a['state']].append(a)
    for state_name, sa in sorted(by_state.items()):
        sc = sa[0]['state_code'].lower()
        add(f"/state/{sc}/", "0.8", "monthly")

    # Accident hubs
    for acc in accidents:
        add(f"/{acc['slug']}/", "0.8", "monthly")

    # Airport hubs (large/medium hubs get higher priority)
    priority_map = {"large_hub": "0.8", "medium_hub": "0.7", "small_hub": "0.6", "non_hub": "0.5"}
    for airport in airports:
        pri = priority_map.get(airport['type'], "0.5")
        add(f"/{airport['slug']}/", pri, "monthly")

    # Leaf pages — prioritized by airport size
    leaf_priority = {"large_hub": "0.7", "medium_hub": "0.6", "small_hub": "0.5", "non_hub": "0.4"}
    for airport in sorted(airports, key=lambda x: ({"large_hub":0,"medium_hub":1,"small_hub":2,"non_hub":3}[x['type']], x['airport_name'])):
        pri = leaf_priority.get(airport['type'], "0.4")
        for acc in accidents:
            add(f"/{airport['slug']}/{acc['slug']}/", pri, "monthly")

    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    xml += "\n".join(urls)
    xml += '\n</urlset>'

    (dist / "sitemap.xml").write_text(xml, encoding="utf-8")
    print(f"  \u2713 sitemap.xml ({len(urls):,} URLs)")


def generate_robots(dist):
    """Generate dist/robots.txt."""
    robots = f'''User-agent: *
Allow: /

Sitemap: {BASE_URL}/sitemap.xml
'''
    (dist / "robots.txt").write_text(robots)
    print("  \u2713 robots.txt")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--states",    action="store_true")
    p.add_argument("--airports",  action="store_true")
    p.add_argument("--accidents", action="store_true")
    p.add_argument("--leaves",    action="store_true")
    p.add_argument("--state",     metavar="CODE")
    p.add_argument("--airport",   metavar="IATA")
    p.add_argument("--accident",  metavar="SLUG")
    p.add_argument("--phase",     type=int, choices=[1,2,3,4])
    p.add_argument("--dry-run",   action="store_true")
    p.add_argument("--clean",     action="store_true")
    args = p.parse_args()

    gen_all = not any([args.states,args.airports,args.accidents,args.leaves,
                       args.state,args.airport,args.accident,args.phase])

    start = datetime.now()
    print(f"\n{'='*60}\nAirportAccidents.com \u2014 Page Generator\n{'='*60}")
    print("\n[1/5] Loading data...")
    airports, accidents, crossref, profiles, variations = load_data()
    print(f"  \u2713 {len(airports)} airports | {len(accidents)} accidents | {len(profiles)} profiles")

    if args.dry_run:
        by_s = defaultdict(list)
        for a in airports: by_s[a['state']].append(a)
        print(f"\n[DRY RUN]")
        print(f"  State hubs:    {len(by_s)}")
        print(f"  Airport hubs:  {len(airports)}")
        print(f"  Accident hubs: {len(accidents)}")
        print(f"  Leaf pages:    {len(airports)*len(accidents):,}")
        print(f"  TOTAL:         {len(by_s)+len(airports)+len(accidents)+len(airports)*len(accidents):,}")
        return

    print("\n[2/5] Loading templates...")
    ts = TMPL_STATE.read_text(); ta = TMPL_AIRPORT.read_text()
    tac = TMPL_ACCIDENT.read_text(); tl = TMPL_LEAF.read_text()
    print(f"  \u2713 4 templates loaded")

    print("\n[3/5] Setting up dist/...")
    if args.clean and DIST.exists(): shutil.rmtree(DIST); print("  \u2713 Cleaned dist/")
    DIST.mkdir(exist_ok=True)

    if gen_all or not any([args.states,args.airports,args.accidents,args.leaves]):
        print("\n[4/5] Copying static assets...")
        copy_assets(DIST)

    print("\n[5/5] Generating pages...")
    total = 0

    if gen_all or args.states or args.state:
        print("\n  \u2014 State hub pages \u2014")
        total += generate_states(airports, ts, DIST, args.state)

    if gen_all or args.airports or (args.airport and not args.accident):
        print("\n  \u2014 Airport hub pages \u2014")
        total += generate_airport_hubs(airports, profiles, ta, DIST, args.airport)

    if gen_all or args.accidents or (args.accident and not args.airport):
        print("\n  \u2014 Accident hub pages \u2014")
        total += generate_accident_hubs(accidents, airports, tac, DIST, args.accident)

    if gen_all or args.leaves or args.phase or args.airport or args.accident:
        print("\n  \u2014 Leaf pages \u2014")
        total += generate_leaf_pages(airports, accidents, profiles, variations, tl, DIST,
                                     filter_airport=args.airport,
                                     filter_accident=args.accident,
                                     phase=args.phase)

    # Always generate sitemap + robots on full or leaf builds
    if gen_all or args.leaves:
        print("\n  \u2014 SEO files \u2014")
        generate_sitemap(airports, accidents, DIST)
        generate_robots(DIST)
        total += 2

    elapsed = (datetime.now()-start).total_seconds()
    rate = int(total / elapsed) if elapsed > 0 else 0
    print(f"\n{'='*60}\n\u2713 Generated {total:,} pages in {elapsed:.1f}s ({rate:,}/sec)\n  Output: {DIST}\n{'='*60}\n")


if __name__ == "__main__":
    main()
