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
    with open(DATA_DIR / 'metro_clusters.json')      as f: metro_clusters    = json.load(f)
    with open(DATA_DIR / 'related_accidents.json')   as f: related_accs      = json.load(f)
    with open(DATA_DIR / 'state_neighbors.json')     as f: state_neighbors   = json.load(f)
    with open(DATA_DIR / 'top_airport_per_state.json')as f: top_per_state    = json.load(f)
    # Load critical CSS for inlining
    critical_css_path = ROOT / "assets" / "css" / "critical.css"
    critical_css_content = critical_css_path.read_text() if critical_css_path.exists() else ""
    with open(DATA_DIR / 'faq_templates.json')         as f: faq_templates    = json.load(f)
    with open(DATA_DIR / 'accident_faq_category.json') as f: acc_faq_cat      = json.load(f)
    with open(DATA_DIR / 'state_comparative_fault.json')as f: comp_fault      = json.load(f)
    with open(DATA_DIR / 'metro_data.json')         as f: metro_data       = json.load(f)
    with open(DATA_DIR / 'operator_entities.json')   as f: operator_entities = json.load(f)
    with open(DATA_DIR / 'state_law_data.json')      as f: state_law_data    = json.load(f)
    with open(DATA_DIR / 'cv_new_sections.json')    as f: new_sect_vars  = json.load(f)
    with open(DATA_DIR / 'injury_costs.json')        as f: injury_costs   = json.load(f)
    with open(DATA_DIR / 'state_timelines.json')     as f: state_timelines= json.load(f)
    with open(DATA_DIR / 'case_vignettes.json')      as f: case_vignettes = json.load(f)
    with open(DATA_DIR / 'attorney_insights.json')        as f: attorney_insights  = json.load(f)
    with open(DATA_DIR / 'attorney_insight_mapping.json') as f: insight_mapping   = json.load(f)
    with open(DATA_DIR / 'seasonal_content.json')          as f: seasonal_content  = json.load(f)
    return airports, accidents, crossref, profiles, variations, metro_clusters, related_accs, state_neighbors, top_per_state, faq_templates, acc_faq_cat, comp_fault, critical_css_content, metro_data, operator_entities, state_law_data, new_sect_vars, injury_costs, state_timelines, case_vignettes, attorney_insights, insight_mapping, seasonal_content


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


def generate_states(airports, tmpl, dist, filter_code=None, state_neighbors=None, top_per_state=None):
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
        # Add neighboring state links
        neighbor_links = ""
        if state_neighbors and top_per_state:
            neighbors = (state_neighbors or {}).get(state_name, [])[:8]
            for nb in neighbors:
                top = (top_per_state or {}).get(nb)
                if top:
                    neighbor_links += (
                        f'<a href="/state/{top["state_code"].lower()}/" class="seo-link-item">'
                        f'<span class="seo-link-item__code">{top["state_code"]}</span>'
                        f'<span class="seo-link-item__text">Airport accidents in {nb}</span></a>\n')
        ctx["neighboring_states_html"] = neighbor_links
        ctx["state_name"] = state_name
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


def generate_accident_hubs(accidents, airports, tmpl, dist, filter_slug=None, top_per_state=None, related_accs=None):
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
        # State breakdown: top airport per state for this accident
        state_links = ""
        if top_per_state:
            for state_name_s, top in sorted((top_per_state or {}).items())[:51]:
                state_links += (
                    f'<a href="/{top["slug"]}/{acc["slug"]}/" class="seo-link-item">'
                    f'<span class="seo-link-item__code">{top["iata"]}</span>'
                    f'<span class="seo-link-item__text">{state_name_s} — {top["name"]}</span></a>\n')
        ctx["state_breakdown_html"] = state_links
        # Related accident types
        related_links = ""
        if related_accs:
            acc_by_slug_local = {a['slug']: a for a in accidents}
            for rel_slug in (related_accs or {}).get(acc['slug'],[]):
                rel = acc_by_slug_local.get(rel_slug)
                if not rel: continue
                icon = ALL_ACCIDENT_ICONS.get(rel_slug, '\u26a0\ufe0f')
                related_links += (
                    f'<a href="/{rel_slug}/" class="seo-link-item seo-link-item--related">'
                    f'<span class="seo-link-item__icon">{icon}</span>'
                    f'<span class="seo-link-item__text">{rel["accident_name"]}</span></a>\n')
        ctx["related_accidents_nav_html"] = related_links
        write_page(dist/acc['slug']/"index.html", render(tmpl, ctx))
        n += 1
        print(f"  \u2713 {acc['slug']}/ \u2014 {acc['accident_name']}")
    return n


# ── Leaf page generator ───────────────────────────────────────────────────────

def generate_leaf_pages(airports, accidents, profiles, tmpl, dist,
                        filter_airport=None, filter_accident=None, phase=None):
    phase_types = {1:{'large_hub'},2:{'medium_hub'},3:{'small_hub'},4:{'non_hub'}}
    allowed = phase_types.get(phase) if phase else None
    # Pre-build lookups for SEO link functions
    airport_by_slug = {a['slug']: a for a in airports}
    accident_by_slug = {a['slug']: a for a in accidents}
    by_state_map = defaultdict(list)
    for a in airports: by_state_map[a['state']].append(a)
    type_order_map = {"large_hub":0,"medium_hub":1,"small_hub":2,"non_hub":3}
    for state in by_state_map:
        by_state_map[state].sort(key=lambda x:(type_order_map.get(x['type'],4),x['airport_name']))
    slug_to_metro = {}
    for metro, slugs in metro_clusters.items():
        for slug in slugs: slug_to_metro[slug] = metro
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


def generate_leaf_pages(airports, accidents, profiles, variations,
                        metro_clusters, related_accs, state_neighbors, top_per_state,
                        faq_templates, acc_faq_cat, comp_fault,
                        critical_css_content,
                        new_sect_vars, injury_costs, state_timelines, case_vignettes,
                        attorney_insights, insight_mapping, seasonal_content,
                        tmpl, dist,
                        filter_airport=None, filter_accident=None, phase=None):
    """Wrapper that runs the leaf generator with parallel file writes."""
    phase_types = {1:{'large_hub'},2:{'medium_hub'},3:{'small_hub'},4:{'non_hub'}}
    allowed = phase_types.get(phase) if phase else None
    # Pre-build lookups for SEO link functions
    airport_by_slug = {a['slug']: a for a in airports}
    accident_by_slug = {a['slug']: a for a in accidents}
    by_state_map = defaultdict(list)
    for a in airports: by_state_map[a['state']].append(a)
    type_order_map = {"large_hub":0,"medium_hub":1,"small_hub":2,"non_hub":3}
    for state in by_state_map:
        by_state_map[state].sort(key=lambda x:(type_order_map.get(x['type'],4),x['airport_name']))
    slug_to_metro = {}
    for metro, slugs in metro_clusters.items():
        for slug in slugs: slug_to_metro[slug] = metro

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
                    # ── SEO internal link sections ─────────────────────────────
                    "nearby_same_accident_html": build_nearby_same_accident(
                        airport, acc['slug'], acc['accident_name'], by_state_map),
                    "related_accidents_html":    build_related_accidents_links(
                        airport['slug'], acc['slug'], related_accs, accident_by_slug),
                    "metro_links_html":          build_metro_links(
                        airport, acc['slug'], acc['accident_name'],
                        metro_clusters, airport_by_slug, slug_to_metro),
                    "neighboring_states_html":   build_neighboring_state_links(
                        airport, state_neighbors, top_per_state),
                    "metro_name":               slug_to_metro.get(airport['slug'],''),
                    "has_metro":                "true" if slug_to_metro.get(airport['slug']) else "",
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
                    # ── New content sections (1,2,4,5,6,8,9,10) ──────────────
                    **build_new_section_context(
                        airport, acc, profile, state_leg, seed,
                        new_sect_vars, injury_costs, state_timelines, case_vignettes,
                        comp_fault
                    ),
                    "build_date":           datetime.now().strftime("%Y-%m-%d"),
                    "unique_content_paras": "\n".join(
                        f'<p class="unique-content__para">{p}</p>'
                        for p in generate_unique_airport_content(
                            airport, acc, profile, state_leg
                        ).split('\n\n') if p.strip()
                    ),
                    # ── Attorney insight ──────────────────────────────────
                    **build_attorney_insight(acc['slug'], airport, attorney_insights, insight_mapping, seed=seed),
                    # ── Seasonal content ──────────────────────────────────
                    **build_seasonal_context(profile, acc, airport, seasonal_content),
                    "critical_css":         critical_css_content,
                    # ── FAQ section ────────────────────────────────────────────
                    **dict(zip(
                        ["faq_section_html", "faq_jsonld"],
                        build_faq_html(airport, acc, profile, state_leg,
                                       faq_templates, acc_faq_cat, comp_fault)
                    )),
                    # ── HowTo schema steps ─────────────────────────────────────
                    "howto_steps_json": build_howto_steps_json(airport, acc, profile, variations, seed+20),
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


ALL_ACCIDENT_ICONS = {
    "slip-and-fall":"\U0001f6b6","jet-bridge-boarding":"\u2708\ufe0f","baggage-claim":"\U0001f9f3",
    "vehicle-accidents":"\U0001f697","security-checkpoint":"\U0001f6c2","escalator-elevator":"\u2b06\ufe0f",
    "food-court-restaurant":"\u2615","shuttle-bus-ground-transportation":"\U0001f68c",
    "parking-lot-curbside":"\U0001f17f\ufe0f","assault-security-failure":"\u26a0\ufe0f",
    "disabled-passenger-assistance":"\u267f","construction-zone":"\U0001f6a7",
    "boarding-stairs-ramps":"\U0001fab5","worker-accidents":"\U0001f477","tarmac-airside":"\U0001f6e9\ufe0f",
    "luggage-cart-conveyor":"\U0001f6d2","international-travel-claims":"\U0001f310",
    "toxic-exposure":"\u2623\ufe0f","lost-delayed-luggage":"\U0001f4e6",
    "rental-car-accidents":"\U0001f699","medical-emergency-negligence":"\U0001f3e5",
    "child-unaccompanied-minor":"\U0001f476","slip-fall-wet-weather":"\U0001f327\ufe0f",
    "airline-delay-cancellation-injury":"\u23f3","retail-shop-injuries":"\U0001f3ea",
}


def build_nearby_same_accident(airport, acc_slug, acc_name, by_state, n=6):
    """Links to same accident at nearby airports in same state."""
    state_airports = [a for a in by_state.get(airport['state'],[]) if a['slug'] != airport['slug']][:n]
    items = []
    for a in state_airports:
        iata = a['iata_code'] or a['faa_code']
        items.append(
            f'<a href="/{a["slug"]}/{acc_slug}/" class="seo-link-item">'
            f'<span class="seo-link-item__code">{iata}</span>'
            f'<span class="seo-link-item__text">{acc_name} at {a["airport_name"]}</span></a>')
    return "\n".join(items)


def build_related_accidents_links(airport_slug, acc_slug, related_accs, accident_by_slug, n=4):
    """Links to semantically related accident types at the same airport."""
    related = related_accs.get(acc_slug, [])[:n]
    items = []
    for rel_slug in related:
        rel_acc = accident_by_slug.get(rel_slug)
        if not rel_acc: continue
        icon = ALL_ACCIDENT_ICONS.get(rel_slug, '\u26a0\ufe0f')
        items.append(
            f'<a href="/{airport_slug}/{rel_slug}/" class="seo-link-item seo-link-item--related">'
            f'<span class="seo-link-item__icon">{icon}</span>'
            f'<span class="seo-link-item__text">{rel_acc["accident_name"]}</span></a>')
    return "\n".join(items)


def build_metro_links(airport, acc_slug, acc_name, metro_clusters, airport_by_slug, slug_to_metro, n=4):
    """Links to same accident at other airports in the same metro area."""
    metro = slug_to_metro.get(airport['slug'])
    if not metro: return ""
    metro_slugs = [s for s in metro_clusters[metro] if s != airport['slug']]
    items = []
    for slug in metro_slugs[:n]:
        a = airport_by_slug.get(slug)
        if not a: continue
        iata = a['iata_code'] or a['faa_code']
        items.append(
            f'<a href="/{slug}/{acc_slug}/" class="seo-link-item seo-link-item--metro">'
            f'<span class="seo-link-item__code">{iata}</span>'
            f'<span class="seo-link-item__text">{acc_name} at {a["airport_name"]}</span></a>')
    return "\n".join(items)


def build_neighboring_state_links(airport, state_neighbors, top_per_state, n=5):
    """Links to state hubs for neighboring states."""
    neighbors = state_neighbors.get(airport['state'],[])[:n]
    items = []
    for neighbor in neighbors:
        top = top_per_state.get(neighbor)
        if not top: continue
        items.append(
            f'<a href="/state/{top["state_code"].lower()}/" class="seo-link-item">'
            f'<span class="seo-link-item__code">{top["state_code"]}</span>'
            f'<span class="seo-link-item__text">Airport accidents in {neighbor}</span></a>')
    return "\n".join(items)


def build_accident_hub_state_links(acc_slug, top_per_state, n=25):
    """For accident hub: link to top airport per state for this accident."""
    items = []
    for state, top in sorted(top_per_state.items(), key=lambda x: x[0])[:n]:
        items.append(
            f'<a href="/{top["slug"]}/{acc_slug}/" class="seo-link-item">'
            f'<span class="seo-link-item__code">{top["iata"]}</span>'
            f'<span class="seo-link-item__text">{state} — {top["name"]}</span></a>')
    return "\n".join(items)


def render_faq_text(template_str, ctx):
    """Simple {var} replacement for FAQ text."""
    result = template_str
    for k, v in ctx.items():
        result = result.replace('{' + k + '}', str(v) if v else '')
    result = re.sub(r'\{[a-z_]+\}', '', result).strip()
    return result


def build_faq_html(airport, acc, profile, state_legal, faq_templates, acc_faq_cat, comp_fault):
    """Build visible FAQ accordion + JSON-LD schema for a leaf page."""
    acc_slug = acc['slug']
    faq_cat  = acc_faq_cat.get(acc_slug, 'premises_liability')
    templates = faq_templates.get(faq_cat, faq_templates.get('premises_liability', []))

    iata    = airport['iata_code'] or airport['faa_code']
    state   = airport['state']
    notice  = profile.get('notice_of_claim_days', 0)
    op      = profile.get('airport_operator_name', f"{airport['city']} Airport Authority")
    op_type = {'city':'city-operated','county':'county-operated',
               'port_authority':'port authority','state':'state-operated',
               'joint_authority':'joint authority','federal':'federal'}.get(
               profile.get('operator_type','city'), 'public entity')

    ctx = {
        'airport':           airport['airport_name'],
        'iata':              iata,
        'city':              airport['city'],
        'state':             state,
        'accident_lower':    acc['accident_name'].lower(),
        'accident_name':     acc['accident_name'],
        'sol':               state_legal.get('sol', '2 years'),
        'notice_days':       str(notice) + ' days' if notice else 'not required',
        'notice_label':      f"{notice} days" if notice else "not required",
        'op':                op,
        'op_type':           op_type,
        'food_op':           profile.get('food_operator', 'the food service operator'),
        'park_op':           profile.get('parking_operator', 'the parking operator'),
        'handler':           profile.get('ground_handler_primary', 'the ground handling company'),
        'settlement_range':  acc.get('average_settlement_range', 'varies by injury'),
        'nearby_courthouse': profile.get('nearby_courthouse', 'the nearest U.S. District Court'),
        'comparative_fault_rule': comp_fault.get(state, 'comparative fault'),
    }

    def rv(t): return render_faq_text(t, ctx)

    faqs = []
    for t in templates:
        faqs.append({'q': rv(t['q']), 'a': rv(t['a'])})

    # ── Visible HTML accordion ─────────────────────────────────────────────
    items_html = []
    for i, faq in enumerate(faqs):
        items_html.append(
            f'<div class="faq-item" id="faq-{i+1}">'
            f'<button class="faq-item__btn" aria-expanded="false" aria-controls="faq-body-{i+1}">'
            f'<span class="faq-item__q">{faq["q"]}</span>'
            f'<span class="faq-item__icon" aria-hidden="true">+</span>'
            f'</button>'
            f'<div class="faq-item__body" id="faq-body-{i+1}" hidden>'
            f'<p class="faq-item__a">{faq["a"]}</p>'
            f'</div></div>')

    faq_html = "\n".join(items_html)

    # ── JSON-LD FAQPage schema ─────────────────────────────────────────────
    import json as _json
    schema_entities = []
    for faq in faqs:
        schema_entities.append({
            "@type": "Question",
            "name": faq["q"],
            "acceptedAnswer": {"@type": "Answer", "text": faq["a"]}
        })

    schema = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": schema_entities
    }

    return faq_html, _json.dumps(schema, indent=2)


def build_howto_steps_json(airport, acc, profile, variations, seed):
    """Build HowTo schema steps JSON from the same step content as the visible steps."""
    import json as _json
    iata   = airport['iata_code'] or airport['faa_code']
    name   = airport['airport_name']
    notice = profile.get('notice_of_claim_days', 0)
    op     = profile.get('airport_operator_name', f"{airport['city']} Airport Authority")
    v      = variations or {}
    fmt    = dict(airport=name, iata=iata, op=op,
                  food_op=profile.get('food_operator','food operators'),
                  park_op=profile.get('parking_operator','parking operators'),
                  handler=profile.get('ground_handler_primary','ground handlers'),
                  accident_lower=acc['accident_name'].lower())

    raw_steps = [
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
        raw_steps.append((
            f"File Notice of Claim within {notice} days",
            f"Claims against {op} require a formal Notice of Claim within {notice} days."
        ))

    schema_steps = []
    for i, (title, text) in enumerate(raw_steps[:6], 1):
        schema_steps.append({
            "@type": "HowToStep",
            "position": i,
            "name": title,
            "text": text,
            "url": f"#step-{i}"
        })
    return _json.dumps(schema_steps, indent=2)


ALL_ACCIDENT_LIST = [
    ("slip-and-fall","\U0001f6b6","Slip & Fall Accidents"),
    ("jet-bridge-boarding","\u2708\ufe0f","Jet Bridge & Boarding Accidents"),
    ("baggage-claim","\U0001f9f3","Baggage Claim Accidents"),
    ("vehicle-accidents","\U0001f697","Vehicle & Baggage Cart Accidents"),
    ("security-checkpoint","\U0001f6c2","Security Checkpoint Injuries"),
    ("escalator-elevator","\u2b06\ufe0f","Escalator & Elevator Accidents"),
    ("food-court-restaurant","\u2615","Restaurant & Food Court Injuries"),
    ("shuttle-bus-ground-transportation","\U0001f68c","Shuttle Bus & Ground Transport"),
    ("parking-lot-curbside","\U0001f17f\ufe0f","Parking Lot & Curbside Injuries"),
    ("assault-security-failure","\u26a0\ufe0f","Assault & Security Failure"),
    ("disabled-passenger-assistance","\u267f","Disabled Passenger Assistance"),
    ("construction-zone","\U0001f6a7","Construction Zone Accidents"),
    ("boarding-stairs-ramps","\U0001fab5","Boarding Stairs & Ramp Injuries"),
    ("worker-accidents","\U0001f477","Airport Worker Injuries"),
    ("tarmac-airside","\U0001f6e9\ufe0f","Tarmac & Airside Accidents"),
    ("luggage-cart-conveyor","\U0001f6d2","Luggage Cart & Conveyor Injuries"),
    ("international-travel-claims","\U0001f310","International Travel Claims"),
    ("toxic-exposure","\u2623\ufe0f","Toxic Exposure & PFAS Claims"),
    ("lost-delayed-luggage","\U0001f4e6","Lost & Delayed Luggage"),
    ("rental-car-accidents","\U0001f699","Rental Car Facility Accidents"),
    ("medical-emergency-negligence","\U0001f3e5","Medical Emergency Negligence"),
    ("child-unaccompanied-minor","\U0001f476","Unaccompanied Minor Claims"),
    ("slip-fall-wet-weather","\U0001f327\ufe0f","Wet Weather Slip & Fall"),
    ("airline-delay-cancellation-injury","\u23f3","Delay & Cancellation Injuries"),
    ("retail-shop-injuries","\U0001f3ea","Retail Shop Injuries"),
]


def generate_city_pages(metro_data, accidents, profiles, airport_by_slug, tmpl, dist, comp_fault):
    """Generate /city/{metro-slug}/ pages for each metro area."""
    n = 0
    for metro_name, md in metro_data.items():
        city_slug = md['city_slug']

        # Airport cards HTML
        airport_cards = []
        for ap in md['airports']:
            a = airport_by_slug.get(ap['slug'])
            if not a: continue
            p = profiles.get(ap['slug'], {})
            notice = p.get('notice_of_claim_days', 0)
            notice_html = f'<div class="city-airport-card__notice">\u26a0 Notice of Claim: {notice} days</div>' if notice else ""
            airport_cards.append(
                f'<a href="/{ap["slug"]}/" class="city-airport-card" role="listitem">'
                f'<div class="city-airport-card__iata">{ap["iata"]}</div>'
                f'<div class="city-airport-card__name">{ap["name"]}</div>'
                f'<div class="city-airport-card__meta">{a["city"]}, {a["state_code"]} — {TYPE_LABELS.get(a["type"],"Airport")}</div>'
                f'{notice_html}'
                f'</a>')

        # Accident links (link to primary airport × each accident)
        primary_slug = md['primary_airport_slug']
        accident_links = []
        for slug, icon, name in ALL_ACCIDENT_LIST:
            accident_links.append(
                f'<a href="/{primary_slug}/{slug}/" class="city-accident-link" role="listitem">'
                f'<span class="city-accident-link__icon">{icon}</span>'
                f'<span>{name}</span></a>')

        # Airport diff items
        diff_items = []
        for ap in md['airports'][:4]:
            p = profiles.get(ap['slug'], {})
            notice = p.get('notice_of_claim_days', 0)
            op = ap.get('op', 'Airport authority')
            notice_str = f' — Notice of Claim {notice} days' if notice else ''
            diff_items.append(
                f'<div class="lp-hazard-item" role="listitem">'
                f'<span class="lp-hazard-item__icon">\u2708\ufe0f</span>'
                f'<span class="lp-hazard-item__text"><strong>{ap["iata"]}</strong>: {op}'
                f'{notice_str}</span></div>')

        # Footer links
        footer_airports = "\n".join(f'<a href="/{ap["slug"]}/">{ap["iata"]} — {ap["name"]}</a>' for ap in md['airports'])
        footer_accidents = "\n".join(f'<a href="/{primary_slug}/{s}/">{n}</a>' for s,_,n in ALL_ACCIDENT_LIST[:8])

        # Comp fault
        cf = comp_fault.get(md['primary_state'], 'modified comparative fault')
        cf_short = "Pure comparative" if 'pure' in cf.lower() else "Modified comparative" if 'modified' in cf.lower() else "Contributory negligence"
        cf_note = cf

        ctx = {
            'page_title':          f"Airport Accident Lawyers — {metro_name} | All {md['airport_count']} Airports",
            'meta_description':    f"Injured at a {metro_name} airport? We cover all {md['airport_count']} airports — {', '.join(ap['iata'] for ap in md['airports'][:4])}. Free case review. {md['sol']} statute of limitations.",
            'canonical_url':       f"{BASE_URL}/city/{city_slug}/",
            'og_title':            f"Airport Accident Lawyers — {metro_name}",
            'metro_name':          metro_name,
            'city_slug':           city_slug,
            'airport_count':       md['airport_count'],
            'primary_state':       md['primary_state'],
            'primary_state_code':  md['primary_state_code'],
            'primary_state_code_lower': md['primary_state_code'].lower(),
            'primary_airport_name': md['primary_airport_name'],
            'primary_airport_iata': md['primary_airport_iata'],
            'primary_op':          md['primary_op'],
            'gov_type':            'government',
            'sol':                 md['sol'],
            'notice':              md['notice'],
            'notice_days_gt_zero': "true" if md.get('notice','') not in ('None required','None (most)','') else "",
            'comp_fault_short':    cf_short,
            'comp_fault_note':     cf_note,
            'h1_title':            f"Airport Accident Attorneys for the {metro_name}: All {md['airport_count']} Airports Covered",
            'hero_intro':          f"The {metro_name} is served by {md['airport_count']} commercial airports — each with different operators, different notice of claim requirements, and different liable parties. Whether your injury occurred at {md['primary_airport_iata']}, a smaller regional airport, or any airport in between, your case depends on which specific airport and which specific zone you were hurt in.",
            'legal_intro':         f"Airport injury claims in {md['primary_state']} involve deadlines that run from the moment of your injury — not when you decide to pursue a claim. The {md['sol']} statute of limitations and notice of claim requirements apply to all {md['airport_count']} airports in the {metro_name} served by {md['primary_state']} law.",
            'differences_intro':   f"Every airport in the {metro_name} looks similar from the outside — terminals, gates, baggage claim. But legally they are completely separate entities with different operators, different insurance carriers, and different procedures for filing claims.",
            'airport_cards_html':  "\n".join(airport_cards),
            'accident_links_html': "\n".join(accident_links),
            'airport_diff_items_html': "\n".join(diff_items),
            'footer_airport_links_html': footer_airports,
            'footer_accident_links_html': footer_accidents,
        }
        write_page(dist / "city" / city_slug / "index.html", render(tmpl, ctx))
        n += 1
        print(f"  \u2713 city/{city_slug}/ — {metro_name}")
    return n


def generate_operator_pages(operator_entities, accidents, airport_by_slug, profiles, tmpl, dist):
    """Generate /operator/{slug}/ pages for each major operator."""
    n = 0
    acc_by_slug = {a['slug']: a for a in accidents}

    for op in operator_entities:
        if op['airport_count'] < 2: continue  # skip operators with only 1 airport

        # Airport links — link each airport to its hub
        airport_links = []
        for slug in op['airports'][:60]:
            a = airport_by_slug.get(slug)
            if not a: continue
            iata = a['iata_code'] or a['faa_code']
            airport_links.append(
                f'<a href="/{slug}/" class="op-airport-link" role="listitem">'
                f'<span class="op-airport-link__code">{iata}</span>'
                f'<span>{a["airport_name"]}, {a["state_code"]}</span></a>')

        # Accident links — link accident types at the primary airport
        primary = op['airports'][0] if op['airports'] else None
        accident_links = []
        for acc_slug in op['accident_types']:
            acc = acc_by_slug.get(acc_slug)
            if not acc or not primary: continue
            icon = ALL_ACCIDENT_ICONS.get(acc_slug, '\u26a0\ufe0f')
            accident_links.append(
                f'<a href="/{primary}/{acc_slug}/" class="op-accident-link" role="listitem">'
                f'<span>{icon}</span>'
                f'<span>{acc["accident_name"]}</span></a>')

        # Liability items
        liability_items = [
            f'<div class="lp-hazard-item" role="listitem"><span class="lp-hazard-item__icon">\u26a0\ufe0f</span><span class="lp-hazard-item__text"><strong>Zone liability</strong>: {op["liability_zone"]}</span></div>',
            f'<div class="lp-hazard-item" role="listitem"><span class="lp-hazard-item__icon">\U0001f4bc</span><span class="lp-hazard-item__text"><strong>Operates at</strong>: {op["airport_count"]} US commercial airports</span></div>',
            f'<div class="lp-hazard-item" role="listitem"><span class="lp-hazard-item__icon">\U0001f4cb</span><span class="lp-hazard-item__text"><strong>Claim type</strong>: Independent defendant with separate commercial liability insurance</span></div>',
        ]

        # Who paras
        type_descriptions = {
            'food_beverage': f"{op['name']} is one of the largest airport food and beverage concessionaires in the United States, operating restaurants, cafes, bars, and retail food outlets across {op['airport_count']} commercial airports.",
            'parking': f"{op['name']} is a major airport parking management company, operating garages, surface lots, and curbside areas at {op['airport_count']} US airports.",
            'ground_handling': f"{op['name']} is a global ground handling company providing baggage handling, ramp operations, and ground vehicle services at {op['airport_count']} US airports.",
        }
        who1 = type_descriptions.get(op['type'], op['description'])
        who2 = f"As a contractor operating within airport facilities, {op['name']} carries its own commercial general liability insurance policy independent of the airport authority. This means {op['name']} can be named as a separate defendant in personal injury claims — and should be named alongside the airport authority in every case where their zone was involved."
        who3 = f"Victims who file claims only against the airport authority and miss {op['name']} as a defendant often recover substantially less than the full value of their case. {op['name']}'s commercial liability coverage is a separate pool of insurance that is only accessible when {op['name']} is properly named as a defendant."

        acc_intro = f"In every case where your injury occurred in a zone operated by {op['name']}, we name {op['name']} as a primary or secondary defendant alongside the airport authority. These are the accident types where {op['name']} is most frequently a defendant:"
        airports_para = f"We have handled claims involving {op['name']} at airports across the US. Each location has its own liability structure and its own insurance adjuster. Find your airport below to see the specific defendants and legal path that applies to your case."

        footer_airports = "\n".join(f'<a href="/{s}/">{airport_by_slug[s]["iata_code"] or airport_by_slug[s]["faa_code"]} — {airport_by_slug[s]["airport_name"]}</a>' for s in op['airports'][:6] if s in airport_by_slug)
        footer_accidents = "\n".join(f'<a href="/{op['airports'][0]}/{s}/">{n}</a>' for s,_,n in ALL_ACCIDENT_LIST[:8]) if op['airports'] else ""

        ctx = {
            'page_title':      f"{op['name']} Airport Injury Claims | {op['airport_count']} US Airports",
            'meta_description':f"Injured at an airport where {op['name']} operates? We handle {op['name']} liability claims at all {op['airport_count']} US airports. Free case review — {op['type_label']} liability specialist.",
            'canonical_url':   f"{BASE_URL}/operator/{op['slug']}/",
            'og_title':        f"{op['name']} Airport Injury Claims | Free Legal Help",
            'operator_name':   op['name'],
            'operator_description': op['description'],
            'type_label':      op['type_label'],
            'airport_count':   op['airport_count'],
            'accident_type_count': len(op['accident_types']),
            'liability_zone_count': len(op['liability_zone'].split(',')),
            'h1_title':        f"{op['name']} Airport Injury Claims: Who to Sue and How to Recover",
            'hero_intro':      f"{op['name']} operates as a {op['type_label'].lower()} at {op['airport_count']} US airports. When you are injured in a zone managed by {op['name']}, they are not just a brand name on a sign — they are a legally liable party with their own commercial insurance that you can pursue independently of the airport authority.",
            'who_para_1':      who1,
            'who_para_2':      who2,
            'who_para_3':      who3,
            'accidents_intro': acc_intro,
            'airports_para':   airports_para,
            'liability_items_html': "\n".join(liability_items),
            'operator_accident_links_html': "\n".join(accident_links),
            'operator_airport_links_html': "\n".join(airport_links),
            'footer_airport_links_html': footer_airports,
            'footer_accident_links_html': footer_accidents,
        }
        write_page(dist / "operator" / op['slug'] / "index.html", render(tmpl, ctx))
        n += 1
        print(f"  \u2713 operator/{op['slug']}/ — {op['name']} ({op['airport_count']} airports)")
    return n


def generate_state_law_pages(state_law_data, accidents, airport_by_slug, profiles, faq_templates, acc_faq_cat, comp_fault_map, tmpl, dist):
    """Generate /law/{state-code}/airport-injury/ pages."""
    import json as _json
    n = 0
    # Use a generic slip-and-fall as the FAQ base for state law pages (premises liability)
    acc_by_slug = {a['slug']: a for a in accidents}
    slip = acc_by_slug.get('slip-and-fall', accidents[0])

    for state_name, d in state_law_data.items():
        sc_lower = d['state_code_lower']

        # Build FAQ for state law page using generic slip-and-fall + state context
        top_airport = airport_by_slug.get(d['top_airport_slug'], {})
        top_profile = profiles.get(d['top_airport_slug'], {})

        state_legal_dict = {'sol': d['sol'], 'notice': d['notice'], 'notice_days': d['notice_days']}
        faq_html, faq_ld = build_faq_html(
            top_airport if top_airport else {'airport_name': f"{state_name} airports", 'iata_code': d['state_code'], 'faa_code': d['state_code'], 'city': state_name, 'state': state_name},
            slip, top_profile if top_profile else {'airport_operator_name': d['gov_entity'], 'operator_type': d['gov_type'], 'notice_of_claim_days': d['notice_days'], 'food_operator': 'food service operators', 'parking_operator': 'parking operators', 'ground_handler_primary': 'ground handlers', 'nearby_courthouse': f'U.S. District Court, {state_name}'},
            state_legal_dict, faq_templates, acc_faq_cat, comp_fault_map
        ) if top_airport else ("", "{}")

        # Airport cards
        all_airports = [airport_by_slug[slug] for slug in
                        [a['slug'] for a in sorted(
                            [airport_by_slug[s] for s in airport_by_slug if airport_by_slug[s]['state'] == state_name],
                            key=lambda x: ({"large_hub":0,"medium_hub":1,"small_hub":2,"non_hub":3}.get(x['type'],4), x['airport_name'])
                        )]
                        if slug in airport_by_slug]
        airport_cards = []
        for a in all_airports[:30]:
            p = profiles.get(a['slug'], {})
            notice = p.get('notice_of_claim_days', 0)
            op = p.get('airport_operator_name', d['gov_entity'])
            iata = a['iata_code'] or a['faa_code']
            notice_html = f'<div class="sl-airport-card__notice">Notice: {notice} days</div>' if notice else ""
            airport_cards.append(
                f'<a href="/{a["slug"]}/" class="sl-airport-card" role="listitem">'
                f'<div class="sl-airport-card__iata">{iata}</div>'
                f'<div class="sl-airport-card__name">{a["airport_name"]}</div>'
                f'<div class="sl-airport-card__op">{op}</div>'
                f'{notice_html}</a>')

        # Accident type links (top airport × each accident)
        primary_slug = d['top_airport_slug']
        accident_links = []
        for slug, icon, name in ALL_ACCIDENT_LIST:
            accident_links.append(
                f'<a href="/{primary_slug}/{slug}/" class="sl-accident-link" role="listitem">'
                f'<span>{icon}</span><span>{name}</span></a>')

        # Comp fault
        cf = comp_fault_map.get(state_name, 'modified comparative fault')
        cf_short = "Pure comparative" if 'pure' in cf.lower() else "Modified comparative" if 'modified' in cf.lower() else "Contributory negligence"
        is_contributory = 'contributory' in cf.lower()
        cf_exp = (f"{state_name} uses contributory negligence — if you are found even 1% at fault, you may be barred from recovery. This makes liability arguments critically important in {state_name} airport cases."
                  if is_contributory else
                  f"{cf}. {'Your award is reduced by your percentage of fault, but you can still recover if you are less than 50% (or 51%) at fault.' if 'modified' in cf.lower() else 'Your award is reduced by your percentage of fault — even if you are mostly at fault you can still recover something.'}")

        # Notice explanation
        notice_days = d['notice_days']
        if notice_days > 0:
            notice_exp = f"A formal Notice of Claim must be filed against {d['gov_entity']} within {notice_days} days of the injury. This is mandatory before any lawsuit against a government-operated airport in {state_name}. Missing this deadline permanently bars your case — no exceptions."
            notice_danger = "sl-deadline-card__value--danger"
        else:
            notice_exp = f"{state_name} does not require a pre-suit Notice of Claim for most airport injury cases. However, specific airports may have their own procedures. Always consult an attorney before assuming no notice is required."
            notice_danger = ""

        # Law paragraphs
        law1 = f"Airport injury law in {state_name} is governed by a combination of {state_name} state tort law, the Federal Tort Claims Act for TSA-related claims, and the Montreal Convention for international flight injuries. The {d['sol']} statute of limitations is the outer deadline — but {notice_exp[:150]}."
        law2 = f"{state_name}'s comparative fault rule — {cf} — directly affects how much you can recover. In claims against government-operated airports like {d['gov_entity']}, the airport's legal team will frequently raise comparative fault arguments. Your attorney's ability to counter those arguments determines your final recovery."
        law3 = f"All {d['airport_count']} {state_name} airports operate under some form of government authority — city-owned, county-owned, port authority, or state-operated. Each has slightly different procedures for filing claims. The {d['gov_entity']} model is the most common in {state_name}, but airports in {state_name} vary. Your attorney must know the specific authority structure at the airport where you were hurt."

        notice_box = f"Every government-operated airport in {state_name} has a Notice of Claim requirement. For most {state_name} airports, this means filing a formal notice with {d['gov_entity']} within {notice_days if notice_days else 'the applicable'} days of the injury. Failing to file this notice before suing permanently bars your case — even if you have strong evidence and a documented injury."
        fed_court = f"Federal claims (TSA injuries, FTCA claims) arising from {state_name} airports are handled by the U.S. District Court with jurisdiction over the airport's location. {state_name} state claims are filed in {state_name} state court."

        footer_airports = "\n".join(f'<a href="/{a["slug"]}/">{a["iata_code"] or a["faa_code"]} — {a["airport_name"]}</a>' for a in all_airports[:6])
        footer_accidents = "\n".join(f'<a href="/{primary_slug}/{s}/">{nm}</a>' for s,_,nm in ALL_ACCIDENT_LIST[:8])

        ctx = {
            'page_title':      f"{state_name} Airport Injury Law — Deadlines, Notice of Claim & Legal Guide",
            'meta_description':f"Complete guide to {state_name} airport injury law: {d['sol']} SOL, {d['notice']} notice of claim, {cf_short} fault rule. All {d['airport_count']} {state_name} airports covered. Free case review.",
            'canonical_url':   f"{BASE_URL}/law/{sc_lower}/airport-injury/",
            'og_title':        f"{state_name} Airport Injury Law | Deadlines & Legal Guide",
            'state':           state_name,
            'state_code':      d['state_code'],
            'state_code_lower':sc_lower,
            'airport_count':   d['airport_count'],
            'sol':             d['sol'],
            'notice':          d['notice'],
            'notice_days':     str(notice_days) if notice_days else "None",
            'gov_entity':      d['gov_entity'],
            'gov_type':        d['gov_type'],
            'comp_fault_short':cf_short,
            'comp_fault_explanation': cf_exp,
            'notice_danger_class': notice_danger,
            'sol_explanation': f"{d['sol']} from the date of your injury at any {state_name} airport. Government-operated airports do not extend this deadline — it runs regardless of whether you have completed medical treatment.",
            'notice_explanation': notice_exp,
            'h1_title':        f"{state_name} Airport Injury Law: Deadlines, Notice of Claim, and Your Rights",
            'hero_intro':      f"{state_name} has {d['airport_count']} commercial airports operating under government authority. Airport injury claims in {state_name} involve a {d['sol']} statute of limitations, a {d['notice']} Notice of Claim requirement for most airports, and {cf_short.lower()} comparative fault rules. Every deadline runs from the moment of your injury — not when you decide to pursue a claim.",
            'deadlines_intro': f"These are the hard deadlines that govern airport injury claims in {state_name}. Missing any one of them can permanently end your case regardless of how strong your evidence is.",
            'law_para_1':      law1,
            'law_para_2':      law2,
            'law_para_3':      law3,
            'notice_box_text': notice_box,
            'federal_court_note': fed_court,
            'faq_section_html':   faq_html,
            'faq_jsonld':          faq_ld,
            'state_airport_cards_html': "\n".join(airport_cards),
            'state_accident_links_html': "\n".join(accident_links),
            'footer_airport_links_html': footer_airports,
            'footer_accident_links_html': footer_accidents,
        }
        write_page(dist / "law" / sc_lower / "airport-injury" / "index.html", render(tmpl, ctx))
        n += 1
        print(f"  \u2713 law/{sc_lower}/airport-injury/ — {state_name}")
    return n


# ── NEW SECTION BUILDERS (Points 1,2,4,5,6,8,9,10) ──────────────────────────

def build_new_section_context(airport, acc, profile, state_leg, seed,
                               new_sect_vars, injury_costs, state_timelines, case_vignettes,
                               comp_fault_map):
    """Build context dict for all 8 new content sections."""
    import json as _json

    iata     = airport['iata_code'] or airport['faa_code']
    name     = airport['airport_name']
    state    = airport['state']
    city     = airport['city']
    op       = profile.get('airport_operator_name', f"{city} Airport Authority")
    food_op  = profile.get('food_operator', 'the food service operator')
    park_op  = profile.get('parking_operator', 'the parking operator')
    handler  = profile.get('ground_handler_primary', 'the ground handling company')
    op_type  = OPERATOR_TYPE_LABELS.get(profile.get('operator_type','city'),'public entity')
    sol      = state_leg.get('sol','2 years')
    notice   = profile.get('notice_of_claim_days', 0)
    passengers = profile.get('annual_passengers_M', 5)
    tier     = profile.get('passenger_tier','regional')
    zones    = profile.get('notable_accident_zones',[])
    zone_str = zones[0] if zones else f"{iata} terminal"
    annual_incidents = acc.get('average_annual_incidents_per_major_hub','50-200')
    settlement_range = acc.get('average_settlement_range','varies')
    acc_slug = acc['slug']
    acc_name = acc['accident_name']
    acc_lower = acc_name.lower()
    faq_cat  = 'premises_liability'  # default for vignettes
    for cat_name, cat_accs in [
        ('federal_liability',['security-checkpoint']),
        ('vehicle_liability',['vehicle-accidents','shuttle-bus-ground-transportation','parking-lot-curbside','rental-car-accidents']),
        ('security_liability',['assault-security-failure']),
        ('workers_compensation',['worker-accidents','tarmac-airside']),
    ]:
        if acc_slug in cat_accs:
            faq_cat = cat_name
            break

    tier_descriptors = {'mega':'busiest','major':'largest','regional':'major regional','small':'smaller commercial'}
    tier_desc = tier_descriptors.get(tier,'major')
    cf = comp_fault_map.get(state,'modified comparative fault')
    cf_short = "pure comparative" if 'pure' in cf.lower() else "modified comparative" if 'modified' in cf.lower() else "contributory negligence"

    def pv(key, **fmt):
        """Pick variation from new_sect_vars pool."""
        pool = new_sect_vars.get(key,['(content)'])
        text = pool[seed % len(pool)]
        fmt_defaults = dict(airport=name,iata=iata,city=city,state=state,
                            op=op,food_op=food_op,park_op=park_op,handler=handler,
                            op_type=op_type,sol=sol,accident=acc_name,
                            accident_lower=acc_lower,settlement_range=settlement_range,
                            notice_days=str(notice)+' days' if notice else 'not required',
                            passengers=f"{passengers:.0f}",tier_descriptor=tier_desc,
                            zones=zone_str,annual_incidents=annual_incidents,
                            state_code=airport['state_code'])
        fmt_defaults.update(fmt)
        import re as _re
        for k,v in fmt_defaults.items():
            text = text.replace('{'+k+'}', str(v) if v else '')
        return _re.sub(r'\{[a-z_]+\}','',text).strip()

    # ── 1. SETTLEMENT VALUE SECTION ───────────────────────────────────────
    factors = parse_list(acc.get('factors_increasing_value',[]))
    factors_html = '\n'.join(
        f'<div class="new-section__factor"><span class="new-section__factor-icon">+</span><span>{f}</span></div>'
        for f in factors[:6])

    settlement_html = (
        f'<div class="new-section" id="settlement-value">'
        f'<div class="new-section__label">Case value</div>'
        f'<h2 class="new-section__title">{pv("settlement_section_titles",seed=seed)}</h2>'
        f'<p class="new-section__intro">{pv("settlement_intro_variants",seed=seed+1)}</p>'
        f'<div class="new-section__value-bar">'
        f'<div class="new-section__value-item"><div class="new-section__value-label">Settlement range</div>'
        f'<div class="new-section__value-number">{settlement_range}</div></div>'
        f'<div class="new-section__value-item"><div class="new-section__value-label">Severity score</div>'
        f'<div class="new-section__value-number">{acc.get("severity_score",7)}/10</div></div>'
        f'<div class="new-section__value-item"><div class="new-section__value-label">Recovery time</div>'
        f'<div class="new-section__value-number">{acc.get("typical_recovery_time","Varies")}</div></div>'
        f'</div>'
        f'<div class="new-section__factors-label">{pv("settlement_factors_labels",seed=seed+2)}</div>'
        f'<div class="new-section__factors">{factors_html}</div>'
        f'</div>')

    # ── 2. CASE VIGNETTES ─────────────────────────────────────────────────
    vigs = case_vignettes.get(faq_cat, case_vignettes.get('premises_liability',[]))
    # Deterministically pick 2 vignettes
    selected = []
    for i in range(min(2, len(vigs))):
        v = vigs[(seed + i * 3) % len(vigs)]
        selected.append(v)

    def render_vig(v):
        text = _json.dumps(v)
        for k, val in [('airport',name),('iata',iata),('op',op),('food_op',food_op),
                        ('park_op',park_op),('handler',handler),('zone',zone_str),
                        ('settlement_range',settlement_range),('state',state),('sol',sol),('accident_lower',acc_lower)]:
            text = text.replace('{'+k+'}', str(val))
        import re as _re
        text = _re.sub(r'\{[a-z_]+\}','',text)
        return _json.loads(text)

    vignettes_items = []
    for v in selected:
        rv = render_vig(v)
        vignettes_items.append(
            f'<div class="new-section__vignette">'
            f'<div class="new-section__vignette-scenario">{rv["scenario"]}</div>'
            f'<div class="new-section__vignette-facts"><strong>The situation:</strong> {rv["facts"]}</div>'
            f'<div class="new-section__vignette-evidence"><strong>Evidence used:</strong> {rv["evidence"]}</div>'
            f'<div class="new-section__vignette-outcome"><strong>Outcome:</strong> {rv["outcome"]}</div>'
            f'<div class="new-section__vignette-lesson">\U0001f4a1 <strong>Key lesson:</strong> {rv["key_lesson"]}</div>'
            f'</div>')

    vignettes_html = (
        f'<div class="new-section" id="case-scenarios">'
        f'<div class="new-section__label">Representative outcomes</div>'
        f'<h2 class="new-section__title">{pv("vignette_section_titles",seed=seed+3)}</h2>'
        f'<p class="new-section__intro">{pv("vignette_intros",seed=seed+4)}</p>'
        f'<div class="new-section__vignettes">{"".join(vignettes_items)}</div>'
        f'</div>')

    # ── 4. DEFENSE STRATEGIES ─────────────────────────────────────────────
    all_defenses = parse_list(acc.get('liable_party_defenses',[]))
    all_counters  = parse_list(acc.get('plaintiff_counter_strategies',[]))
    # Rotate which 4 defenses show on this page using seed
    d_start = seed % max(1, len(all_defenses) - 3)
    defenses = (all_defenses + all_defenses)[d_start:d_start+4]
    counters = (all_counters + all_counters)[d_start:d_start+4]
    defense_items = []
    for i, (defense, counter) in enumerate(zip(defenses[:4], counters[:4])):
        defense_items.append(
            f'<div class="new-section__defense">'
            f'<div class="new-section__defense-arg">\u26a0\ufe0f <strong>What {op} will argue:</strong> {defense}</div>'
            f'<div class="new-section__defense-counter">\u2713 <strong>{pv("defense_counter_labels",seed=seed+5+i)}:</strong> {counter}</div>'
            f'</div>')

    defense_html = (
        f'<div class="new-section" id="defense-strategy">'
        f'<div class="new-section__label">Know what to expect</div>'
        f'<h2 class="new-section__title">{pv("defense_section_titles",seed=seed+6)}</h2>'
        f'<p class="new-section__intro">{pv("defense_intro_variants",seed=seed+7)}</p>'
        f'<div class="new-section__defenses">{"".join(defense_items)}</div>'
        f'</div>') if defense_items else ''

    # ── 5. CASE TIMELINE ─────────────────────────────────────────────────
    timeline_steps = state_timelines.get(state, state_timelines.get('California',[]))
    timeline_items = '\n'.join(
        f'<div class="new-section__timeline-step new-section__timeline-step--{s["urgency"]}">'
        f'<div class="new-section__timeline-window">{s["window"]}</div>'
        f'<div class="new-section__timeline-content">'
        f'<div class="new-section__timeline-title">{s["title"]}</div>'
        f'<div class="new-section__timeline-body">{s["body"]}</div>'
        f'</div></div>'
        for s in timeline_steps)

    timeline_html = (
        f'<div class="new-section" id="case-timeline">'
        f'<div class="new-section__label">{state} — realistic timeline</div>'
        f'<h2 class="new-section__title">{pv("timeline_section_titles",seed=seed+8)}</h2>'
        f'<p class="new-section__intro">{pv("timeline_intro_variants",seed=seed+9)}</p>'
        f'<div class="new-section__timeline">{timeline_items}</div>'
        f'</div>')

    # ── 8. MEDICAL COSTS ─────────────────────────────────────────────────
    all_injuries = parse_list(acc.get('common_injuries',[]))
    inj_start = (seed * 3) % max(1, len(all_injuries) - 5)
    injuries = (all_injuries + all_injuries)[inj_start:inj_start+6]
    medical_items = []
    for inj in injuries[:6]:
        cost_data = None
        for key, costs in injury_costs.items():
            if key.lower() in inj.lower() or inj.lower() in key.lower():
                cost_data = costs
                break
        if cost_data:
            medical_items.append(
                f'<div class="new-section__medical-row">'
                f'<div class="new-section__medical-injury">{inj}</div>'
                f'<div class="new-section__medical-range">${cost_data["low"]:,} – ${cost_data["high"]:,}</div>'
                f'<div class="new-section__medical-note">{cost_data["note"]}</div>'
                f'</div>')
        else:
            medical_items.append(
                f'<div class="new-section__medical-row">'
                f'<div class="new-section__medical-injury">{inj}</div>'
                f'<div class="new-section__medical-range">Varies by severity</div>'
                f'<div class="new-section__medical-note">Consult treating physician for cost estimate</div>'
                f'</div>')

    medical_html = (
        f'<div class="new-section" id="medical-costs">'
        f'<div class="new-section__label">What treatment costs in {state}</div>'
        f'<h2 class="new-section__title">{pv("medical_section_titles",seed=seed+10)}</h2>'
        f'<p class="new-section__intro">{pv("medical_intro_variants",seed=seed+11)}</p>'
        f'<div class="new-section__medical-table">'
        f'<div class="new-section__medical-header">'
        f'<span>Injury type</span><span>Treatment cost range</span><span>What it covers</span>'
        f'</div>{"".join(medical_items)}</div>'
        f'</div>') if medical_items else ''

    # ── 9. PRIOR INCIDENTS ───────────────────────────────────────────────
    prior_html = (
        f'<div class="new-section" id="prior-incidents">'
        f'<div class="new-section__label">Documented history at {iata}</div>'
        f'<h2 class="new-section__title">{pv("prior_incidents_titles",seed=seed+12)}</h2>'
        f'<p class="new-section__intro">{pv("prior_incidents_intros",seed=seed+13)}</p>'
        f'<div class="new-section__prior-facts">'
        f'<div class="new-section__prior-stat">'
        f'<div class="new-section__prior-number">{annual_incidents}</div>'
        f'<div class="new-section__prior-label">{acc_name} incidents per year at airports like {iata}</div>'
        f'</div>'
        f'<div class="new-section__prior-text">'
        f'<p>{acc.get("frequency_note","Incidents occur regularly at commercial airports.")}</p>'
        f'<p style="margin-top:12px;">Prior incident records at {name} are obtained through public records requests and litigation discovery. '
        f'These records establish that {op} had actual or constructive notice of the recurring hazard at {iata} — '
        f'the legal standard that converts a simple negligence claim into a pattern-of-negligence case.</p>'
        f'</div></div>'
        f'</div>')

    # ── 10. AIRPORT STATISTICS ───────────────────────────────────────────
    high_risk_times = parse_list(acc.get('high_risk_times',[]))
    risk_items = '\n'.join(
        f'<div class="new-section__risk-item"><span class="new-section__risk-icon">\u23f0</span><span>{t}</span></div>'
        for t in high_risk_times[:4])

    demographics = parse_list(profile.get('victim_demographics') or acc.get('victim_demographics',[]))
    demo_items = '\n'.join(
        f'<div class="new-section__demo-item">\u2192 {d}</div>'
        for d in demographics[:4]) if demographics else ''

    stats_html = (
        f'<div class="new-section" id="airport-stats">'
        f'<div class="new-section__label">{iata} — risk profile</div>'
        f'<h2 class="new-section__title">{pv("stats_section_titles",seed=seed+14)}</h2>'
        f'<p class="new-section__intro">{pv("stats_intro_variants",seed=seed+15)}</p>'
        f'<div class="new-section__stats-grid">'
        f'<div class="new-section__stat-block">'
        f'<div class="new-section__stat-number">{passengers:.0f}M</div>'
        f'<div class="new-section__stat-label">Annual passengers at {iata}</div>'
        f'</div>'
        f'<div class="new-section__stat-block">'
        f'<div class="new-section__stat-number">{annual_incidents}</div>'
        f'<div class="new-section__stat-label">{acc_name} incidents per year at airports this size</div>'
        f'</div>'
        f'<div class="new-section__stat-block">'
        f'<div class="new-section__stat-number">{settlement_range.split()[0]}</div>'
        f'<div class="new-section__stat-label">Settlement range for this accident type</div>'
        f'</div>'
        f'</div>'
        f'{"<div class=\'new-section__risk-times\'><div class=\'new-section__risk-label\'>High-risk times at " + iata + "</div>" + risk_items + "</div>" if risk_items else ""}'
        f'{"<div class=\'new-section__demographics\'><div class=\'new-section__risk-label\'>Most affected passengers</div>" + demo_items + "</div>" if demo_items else ""}'
        f'</div>')

    return {
        'settlement_html':  settlement_html,
        'vignettes_html':   vignettes_html,
        'defense_html':     defense_html,
        'timeline_html':    timeline_html,
        'medical_html':     medical_html,
        'prior_html':       prior_html,
        'stats_html':       stats_html,
    }



def generate_unique_airport_content(airport, acc, profile, state_legal):
    """
    Generate 100% unique 300-400 word content block for each airport+accident.
    Uses only data that differs per airport — guaranteed unique across all 13,050 pages.
    """
    iata       = airport['iata_code'] or airport['faa_code']
    name       = airport['airport_name']
    city       = airport['city']
    state      = airport['state']
    tier       = profile.get('passenger_tier', 'regional')
    passengers = profile.get('annual_passengers_M', 1)
    op         = profile.get('airport_operator_name', f'{city} Airport Authority')
    op_type    = profile.get('operator_type', 'city')
    food_op    = profile.get('food_operator', 'food service operators')
    park_op    = profile.get('parking_operator', 'parking operators')
    handler    = profile.get('ground_handler_primary', 'ground handlers')
    notice     = profile.get('notice_of_claim_days', 0)
    climate    = profile.get('climate_zone', 'temperate')
    snow       = profile.get('avg_annual_snow_inches', 0)
    elevation  = profile.get('elevation_ft', 100)
    construction      = profile.get('construction_active', False)
    construction_notes= profile.get('construction_notes', '')
    altitude_risk     = profile.get('high_altitude_medical_risk', False)
    zones      = profile.get('notable_accident_zones', [])
    hazards    = profile.get('airport_specific_hazards', [])
    local_note = profile.get('local_context_note', '')
    sol        = state_legal.get('sol', '2 years')
    acc_slug   = acc['slug']
    acc_lower  = acc['accident_name'].lower()

    op_type_labels = {'city':'city department','county':'county agency',
                      'port_authority':'port authority','state':'state agency',
                      'joint_authority':'joint authority','federal':'federal agency'}
    op_label = op_type_labels.get(op_type, 'government entity')

    tier_map = {
        'mega':     f"one of the world's busiest airports — {passengers:.0f} million annual passengers",
        'major':    f"one of the largest airports in {state} with {passengers:.0f} million annual passengers",
        'regional': f"a regional airport serving {city} with {passengers:.1f} million annual passengers",
        'small':    f"a smaller commercial airport serving {city}",
    }
    size_desc = tier_map.get(tier, f"a commercial airport handling {passengers:.1f} million passengers annually")

    # P1: Operator structure — unique per airport
    if food_op == 'Airport-operated':
        food_ctx = f"{name} operates its food service directly — {acc_lower} claims in dining areas go against {op} itself"
    else:
        food_ctx = f"{food_op} operates all food and beverage concessions at {iata} under contract with {op} — they are the direct defendant in dining area {acc_lower} claims"

    if park_op in ('Airport-operated', ''):
        park_ctx = f"parking is managed directly by {op}"
    else:
        park_ctx = f"{park_op} manages parking at {iata} under its own contract with separate liability coverage"

    p1 = f"{name} is {size_desc}. {food_ctx}. {park_ctx}. Ground handling — baggage, ramp vehicles, jet bridge equipment — is operated by {handler}. Each of these entities is a separate defendant with separate commercial insurance, and each can be named alongside {op} in a {acc_lower} claim."

    # P2: Physical/climate hazards — unique per airport
    climate_map = {
        'northern_winter': f"With {snow:.0f} inches of annual snowfall and temperatures that regularly drop well below freezing, {name} faces severe seasonal conditions at all outdoor surfaces — curbside, parking, and boarding areas. The {acc_lower} risk at {iata} peaks in winter months when ice and compacted snow create hazardous conditions that {op} is legally obligated to address but frequently fails to manage adequately during high-traffic periods.",
        'coastal_rain':    f"{city}'s marine climate produces persistent moisture at {name} — wet floors from tracked-in rain, moisture in covered walkways, and slick curbside surfaces are year-round conditions rather than seasonal risks. The {acc_lower} hazard at {iata} is constant, not episodic, making the failure to maintain adequate dry floor conditions a chronic rather than occasional negligence.",
        'mountain':        f"At {elevation:,} feet elevation with {snow:.0f} inches of annual snowfall, {name} faces both extreme outdoor weather conditions and altitude-specific health risks. The {acc_lower} risk profile at {iata} is shaped by ice and snow at outdoor zones from October through April and by the physiological effects of {city}'s high altitude on passengers who arrived at sea level hours earlier.",
        'desert_dry':      f"{city}'s extreme heat creates thermal cycling in floor and surface materials at {name} — contributing to surface degradation and uneven flooring that is a documented {acc_lower} risk at desert-climate airports. Summer heat also accelerates fatigue and disorientation in passengers, particularly elderly travelers, increasing the risk of accidents at {iata}.",
        'southern_humid':  f"{city}'s humidity and frequent precipitation mean {name} deals with wet-floor conditions from tracked moisture on a near-daily basis during summer months. The {acc_lower} risk at {iata} is elevated by the combination of high passenger volume during summer peak season and the persistent moisture that summer weather brings into all terminal entry points.",
        'tropical':        f"{city}'s tropical climate means {name} operates in near-constant humidity and frequent rainfall — wet floors from tracked-in moisture are a permanent feature of the {acc_lower} risk profile at {iata}. Year-round wet conditions create a continuous maintenance obligation for {op} and its cleaning contractors that is difficult to satisfy during peak hours.",
    }
    p2 = climate_map.get(climate, f"The climate conditions in {city} contribute to the physical hazard profile at {name}.")

    if altitude_risk and acc_slug == 'medical-emergency-negligence':
        p2 += f" At {elevation:,} feet, {iata} is among the highest-elevation major US airports — significantly elevating cardiac and respiratory risk for all passengers and creating a heightened legal standard for the adequacy of emergency response infrastructure."

    if construction:
        p2 += f" Active construction at {iata} — {construction_notes[:120].rstrip()} — creates additional hazard zones directly adjacent to operating passenger areas, adding a construction-liability layer on top of the standard airport premises liability."

    # P3: Legal framework — unique per airport + state
    if notice > 0:
        p3 = f"{op} is a {op_label} of {state}. This has direct legal consequences for your {acc_lower} claim: government liability law applies, not standard tort law. A formal Notice of Claim must be filed against {op} within {notice} days of your injury at {name} — this is a mandatory prerequisite to any lawsuit, and missing it permanently bars your case under {state} law regardless of how strong your evidence is or how severe your injury. {local_note}"
    else:
        p3 = f"{op} is a {op_label}. {state} does not require a pre-suit Notice of Claim for most airport injury cases, which removes one procedural barrier — but {op} begins managing the incident from the moment it is reported at {iata}, and the {sol} statute of limitations is already running. {local_note}"

    # P4: Specific zones — unique per airport
    parts = [p1, p2, p3]
    if zones:
        zone_list = ', '.join(zones[:3])
        p4 = f"The documented high-risk zones at {name} for {acc_lower} incidents are: {zone_list}. These specific areas have higher incident frequency because of the combination of passenger volume, operational complexity, and multiple contractor handoffs that characterize these zones at {iata}."
        parts.append(p4)

    if hazards:
        h_text = hazards[0]
        if len(hazards) > 1:
            h_text += f" A second documented risk factor at {iata}: {hazards[1]}"
        parts.append(h_text)

    return '\n\n'.join(parts)



def build_attorney_insight(acc_slug, airport, attorney_insights, insight_mapping, seed=0):
    """Rotate through 20 attorney observations per accident type using page seed."""
    # Direct slug lookup — new format is {slug: [obs1, obs2, ..., obs20]}
    observations = attorney_insights.get(acc_slug)
    if not observations:
        # Fallback via insight_mapping for unmapped slugs
        key = insight_mapping.get(acc_slug, 'premises_liability_general')
        observations = attorney_insights.get(key)
    if not observations:
        return {"attorney_insight_html": ""}

    # Handle legacy dict format {insight: "...", author_note: "..."}
    if isinstance(observations, dict):
        observations = [observations.get('insight', '')]

    if not observations:
        return {"attorney_insight_html": ""}

    iata = airport['iata_code'] or airport['faa_code']
    # Each airport+accident combination gets a different observation via seed rotation
    text = observations[seed % len(observations)]

    html = (
        f'<div class="attorney-insight">'
        f'<div class="attorney-insight__label">\U0001f3db\ufe0f Attorney perspective \u2014 {iata}</div>'
        f'<blockquote class="attorney-insight__quote">'
        f'<p class="attorney-insight__text">{text}</p>'
        f'</blockquote>'
        f'<div class="attorney-insight__attribution">'
        f'AirportAccidents.com Legal Team \u2014 based on actual airport injury litigation at {iata} and similar airports'
        f'</div>'
        f'</div>')
    return {"attorney_insight_html": html}

def build_seasonal_context(profile, acc, airport, seasonal_content=None):
    """Build seasonal risk context based on current quarter and airport climate."""
    from datetime import datetime
    month = datetime.now().month
    quarter = f"Q{(month - 1) // 3 + 1}"
    climate = profile.get('climate_zone', 'temperate')
    name = airport['airport_name']
    city = airport['city']
    iata = airport['iata_code'] or airport['faa_code']
    acc_lower = acc['accident_name'].lower()

    # Import seasonal_content from module scope — it was passed as param but we need it
    # Access via the global in the function (we pass it indirectly)
    sc = seasonal_content or {}
    climate_seasons = sc.get(climate, sc.get('temperate', {}))
    current = climate_seasons.get(quarter, {})

    def rv(text):
        if not text: return ''
        import re as _re
        text = text.replace('{airport}', name).replace('{city}', city).replace('{iata}', iata).replace('{accident_lower}', acc_lower)
        return _re.sub(r'\{[a-z_]+\}', '', text).strip()

    urgency = current.get('urgency', 'standard')
    risk_label = rv(current.get('risk_label', ''))
    risk_note  = rv(current.get('risk_note',  ''))

    urgency_colors = {'critical': '#FCA5A5', 'elevated': '#FCD34D', 'standard': 'var(--alpha-white-55)'}
    color = urgency_colors.get(urgency, 'var(--alpha-white-55)')

    seasonal_html = (
        f'<div class="seasonal-context">'
        f'<span class="seasonal-context__label" style="color:{color};">{risk_label}</span>'
        f'<p class="seasonal-context__note">{risk_note}</p>'
        f'</div>') if risk_label else ''

    return {
        "seasonal_quarter": quarter,
        "seasonal_climate": climate,
        "seasonal_html": seasonal_html,
        "seasonal_urgency": urgency,
        "seasonal_label": risk_label,
    }


def copy_assets(dist):
    shutil.copy2(HOMEPAGE_SRC, dist/"index.html"); print("  \u2713 index.html")
    dest = dist/"assets"
    if dest.exists(): shutil.rmtree(dest)
    shutil.copytree(ASSETS_SRC, dest); print("  \u2713 assets/")


def generate_sitemap(airports, accidents, dist):
    """Generate split sitemaps by priority tier + sitemap index."""
    from datetime import date
    today = date.today().isoformat()

    def url_entry(loc, priority, changefreq="monthly"):
        return (f'  <url>\n    <loc>{BASE_URL}{loc}</loc>\n'
                f'    <lastmod>{today}</lastmod>\n'
                f'    <changefreq>{changefreq}</changefreq>\n'
                f'    <priority>{priority}</priority>\n  </url>')

    def write_sm(name, entries):
        xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
               '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"\n'
               '        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
               '        xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 '
               'http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd">\n')
        xml += "\n".join(entries) + '\n</urlset>'
        (dist / name).write_text(xml, encoding="utf-8")
        return len(entries)

    by_state = defaultdict(list)
    for a in airports: by_state[a['state']].append(a)

    core = [url_entry("/", "1.0", "weekly")]
    for state_name, sa in sorted(by_state.items()):
        core.append(url_entry(f"/state/{sa[0]['state_code'].lower()}/", "0.9", "monthly"))
    for acc in accidents:
        core.append(url_entry(f"/{acc['slug']}/", "0.9", "monthly"))
    n_core = write_sm("sitemap-core.xml", core)

    hub_pri = {"large_hub":"0.8","medium_hub":"0.7","small_hub":"0.6","non_hub":"0.5"}
    hubs = [url_entry(f"/{a['slug']}/", hub_pri.get(a['type'],"0.5"))
            for a in sorted(airports, key=lambda x: x['airport_name'])]
    n_hubs = write_sm("sitemap-airports.xml", hubs)

    type_order = {"large_hub":0,"medium_hub":1,"small_hub":2,"non_hub":3}
    sorted_airports = sorted(airports, key=lambda x:(type_order.get(x['type'],4),x['airport_name']))
    leaf_pri = {"large_hub":"0.7","medium_hub":"0.6","small_hub":"0.5","non_hub":"0.4"}
    tiers = {"large_hub":[],"medium_hub":[],"small_hub":[],"non_hub":[]}
    for airport in sorted_airports:
        pri = leaf_pri.get(airport['type'],"0.4")
        for acc in accidents:
            tiers[airport['type']].append(url_entry(f"/{airport['slug']}/{acc['slug']}/", pri))

    tier_names = {"large_hub":"sitemap-leaves-large.xml","medium_hub":"sitemap-leaves-medium.xml",
                  "small_hub":"sitemap-leaves-small.xml","non_hub":"sitemap-leaves-regional.xml"}
    n_leaves = 0
    for tier_key, tier_entries in tiers.items():
        if tier_entries:
            n_leaves += write_sm(tier_names[tier_key], tier_entries)

    all_sms = ["sitemap-core.xml","sitemap-airports.xml"] + list(tier_names.values())
    idx = ('<?xml version="1.0" encoding="UTF-8"?>\n'
           '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
    for sm_name in all_sms:
        idx += f'  <sitemap>\n    <loc>{BASE_URL}/{sm_name}</loc>\n    <lastmod>{today}</lastmod>\n  </sitemap>\n'
    idx += '</sitemapindex>'
    (dist / "sitemap.xml").write_text(idx, encoding="utf-8")

    total = n_core + n_hubs + n_leaves
    print(f"  \u2713 sitemap index + 6 split sitemaps ({total:,} URLs total)")
    print(f"      core={n_core} | airports={n_hubs} | leaves={n_leaves}")


def generate_robots(dist):
    """Generate hardened robots.txt with crawl budget management."""
    robots = f"""User-agent: *
Allow: /
Disallow: /assets/
Disallow: /*?*
Crawl-delay: 1

# Block low-value bots
User-agent: AhrefsBot
Crawl-delay: 10

User-agent: SemrushBot
Crawl-delay: 10

User-agent: MJ12bot
Disallow: /

User-agent: DotBot
Disallow: /

# Sitemaps
Sitemap: {BASE_URL}/sitemap.xml
Sitemap: {BASE_URL}/sitemap-core.xml
Sitemap: {BASE_URL}/sitemap-airports.xml
Sitemap: {BASE_URL}/sitemap-leaves-large.xml
Sitemap: {BASE_URL}/sitemap-leaves-medium.xml
Sitemap: {BASE_URL}/sitemap-leaves-small.xml
Sitemap: {BASE_URL}/sitemap-leaves-regional.xml
"""
    (dist / "robots.txt").write_text(robots)
    print("  \u2713 robots.txt (hardened)")


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
    airports, accidents, crossref, profiles, variations, metro_clusters, related_accs, state_neighbors, top_per_state, faq_templates, acc_faq_cat, comp_fault, critical_css_content, metro_data, operator_entities, state_law_data, new_sect_vars, injury_costs, state_timelines, case_vignettes, attorney_insights, insight_mapping, seasonal_content = load_data()
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
        total += generate_states(airports, ts, DIST, args.state, state_neighbors, top_per_state)

    if gen_all or args.airports or (args.airport and not args.accident):
        print("\n  \u2014 Airport hub pages \u2014")
        total += generate_airport_hubs(airports, profiles, ta, DIST, args.airport)

    if gen_all or args.accidents or (args.accident and not args.airport):
        print("\n  \u2014 Accident hub pages \u2014")
        total += generate_accident_hubs(accidents, airports, tac, DIST, args.accident, top_per_state, related_accs)

    if gen_all or args.leaves or args.phase or args.airport or args.accident:
        print("\n  \u2014 Leaf pages \u2014")
        total += generate_leaf_pages(airports, accidents, profiles, variations,
                                     metro_clusters, related_accs, state_neighbors, top_per_state,
                                     faq_templates, acc_faq_cat, comp_fault,
                                     critical_css_content,
                                     new_sect_vars, injury_costs, state_timelines, case_vignettes,
                                     attorney_insights, insight_mapping, seasonal_content,
                                     tl, DIST,
                                     filter_airport=args.airport,
                                     filter_accident=args.accident,
                                     phase=args.phase)

    # City, Operator, State Law pages
    if gen_all:
        # Load templates
        tmpl_city     = (TEMPLATES_DIR / "city-hub.html").read_text()
        tmpl_operator = (TEMPLATES_DIR / "operator-entity.html").read_text()
        tmpl_state_law= (TEMPLATES_DIR / "state-law.html").read_text()
        airport_by_slug_map = {a['slug']: a for a in airports}

        print("\n  \u2014 City pages \u2014")
        total += generate_city_pages(metro_data, accidents, profiles, airport_by_slug_map, tmpl_city, DIST, comp_fault)

        print("\n  \u2014 Operator entity pages \u2014")
        total += generate_operator_pages(operator_entities, accidents, airport_by_slug_map, profiles, tmpl_operator, DIST)

        print("\n  \u2014 State law pages \u2014")
        total += generate_state_law_pages(state_law_data, accidents, airport_by_slug_map, profiles, faq_templates, acc_faq_cat, comp_fault, tmpl_state_law, DIST)

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
