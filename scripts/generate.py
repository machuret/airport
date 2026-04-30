#!/usr/bin/env python3
"""
AirportAccidents.com — Page Generator
======================================
Generates all static HTML pages from templates + data.

Output structure:
  dist/
    index.html                          ← homepage (copied as-is)
    state/
      ca/index.html                     ← state hub pages (54 total)
    {airport-slug}/
      index.html                        ← airport hub pages (522 total)
    {accident-slug}/
      index.html                        ← accident hub pages (25 total)

Usage:
  python3 scripts/generate.py              # generate everything
  python3 scripts/generate.py --states     # states only
  python3 scripts/generate.py --airports   # airport hubs only
  python3 scripts/generate.py --accidents  # accident hubs only
  python3 scripts/generate.py --state CA   # single state
  python3 scripts/generate.py --airport LAX
  python3 scripts/generate.py --dry-run    # count pages, don't write
"""

import csv
import json
import os
import re
import sys
import shutil
import argparse
import textwrap
from collections import defaultdict
from pathlib import Path
from datetime import datetime

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).parent.parent
DATA_DIR    = ROOT / "data"
TEMPLATES   = ROOT / "templates"
PUBLIC      = ROOT / "public"
DIST        = ROOT / "dist"
ASSETS_SRC  = ROOT / "assets"

AIRPORTS_CSV  = DATA_DIR / "us_airports_full.csv"
ACCIDENTS_JSON = DATA_DIR / "accidents_database_v2.json"
CROSSREF_CSV  = DATA_DIR / "airports_accidents_crossref.csv"

TMPL_STATE    = TEMPLATES / "state-hub.html"
TMPL_AIRPORT  = TEMPLATES / "airport-hub.html"
TMPL_ACCIDENT = TEMPLATES / "accident-hub.html"
HOMEPAGE_SRC  = PUBLIC / "index.html"

BASE_URL = "https://airportaccidents.com"

# ── State legal metadata ─────────────────────────────────────────────────────
STATE_LEGAL = {
    "Alabama":              {"sol": "2 years",   "notice": "6 months",         "gov": "Birmingham Airport Authority"},
    "Alaska":               {"sol": "2 years",   "notice": "120 days",         "gov": "Ted Stevens Anchorage Int'l Airport"},
    "Arizona":              {"sol": "2 years",   "notice": "180 days",         "gov": "City of Phoenix Aviation Dept"},
    "Arkansas":             {"sol": "3 years",   "notice": "1 year",           "gov": "Little Rock National Airport Auth"},
    "California":           {"sol": "2 years",   "notice": "6 months",         "gov": "Los Angeles World Airports (LAWA)"},
    "Colorado":             {"sol": "2 years",   "notice": "182 days",         "gov": "Denver International Airport"},
    "Connecticut":          {"sol": "2 years",   "notice": "6 months",         "gov": "CT Airport Authority"},
    "Florida":              {"sol": "2 years",   "notice": "3 years (sovereign immunity)", "gov": "Miami-Dade Aviation Dept"},
    "Georgia":              {"sol": "2 years",   "notice": "6 months",         "gov": "City of Atlanta Dept of Aviation"},
    "Guam":                 {"sol": "2 years",   "notice": "6 months",         "gov": "Antonio B. Won Pat Int'l Airport"},
    "Hawaii":               {"sol": "2 years",   "notice": "None required",    "gov": "Hawaii Dept of Transportation"},
    "Idaho":                {"sol": "2 years",   "notice": "180 days",         "gov": "Boise Airport Authority"},
    "Illinois":             {"sol": "2 years",   "notice": "1 year",           "gov": "Chicago Dept of Aviation"},
    "Indiana":              {"sol": "2 years",   "notice": "180 days",         "gov": "Indianapolis Airport Authority"},
    "Iowa":                 {"sol": "2 years",   "notice": "60 days",          "gov": "Des Moines Airport Authority"},
    "Kansas":               {"sol": "2 years",   "notice": "120 days",         "gov": "Wichita Airport Authority"},
    "Kentucky":             {"sol": "1 year",    "notice": "90 days",          "gov": "Louisville Regional Airport Auth"},
    "Louisiana":            {"sol": "1 year",    "notice": "None required",    "gov": "Louis Armstrong Int'l Airport"},
    "Maine":                {"sol": "6 years",   "notice": "None required",    "gov": "Portland Int'l Jetport"},
    "Maryland":             {"sol": "3 years",   "notice": "1 year",           "gov": "Maryland Aviation Administration"},
    "Massachusetts":        {"sol": "3 years",   "notice": "None required",    "gov": "Massport (Logan Airport)"},
    "Michigan":             {"sol": "3 years",   "notice": "None required",    "gov": "Wayne County Airport Authority"},
    "Minnesota":            {"sol": "3 years",   "notice": "180 days",         "gov": "Metropolitan Airports Commission"},
    "Mississippi":          {"sol": "3 years",   "notice": "90 days",          "gov": "Jackson Municipal Airport Auth"},
    "Missouri":             {"sol": "5 years",   "notice": "90 days",          "gov": "Kansas City Aviation Dept"},
    "Montana":              {"sol": "3 years",   "notice": "None required",    "gov": "Billings Logan Int'l Airport"},
    "Nebraska":             {"sol": "4 years",   "notice": "1 year",           "gov": "Omaha Airport Authority"},
    "Nevada":               {"sol": "2 years",   "notice": "6 months",         "gov": "Clark County Dept of Aviation (LAS)"},
    "New Hampshire":        {"sol": "3 years",   "notice": "None required",    "gov": "Manchester-Boston Regional Airport"},
    "New Jersey":           {"sol": "2 years",   "notice": "90 days",          "gov": "Port Authority of NY & NJ"},
    "New Mexico":           {"sol": "3 years",   "notice": "90 days",          "gov": "Albuquerque Int'l Sunport"},
    "New York":             {"sol": "1.5 years", "notice": "90 days",          "gov": "Port Authority of NY & NJ"},
    "North Carolina":       {"sol": "3 years",   "notice": "None required",    "gov": "Charlotte Douglas Int'l Airport"},
    "North Dakota":         {"sol": "6 years",   "notice": "None required",    "gov": "Hector Int'l Airport, Fargo"},
    "Ohio":                 {"sol": "2 years",   "notice": "None required",    "gov": "Columbus Regional Airport Auth"},
    "Oklahoma":             {"sol": "2 years",   "notice": "1 year",           "gov": "Will Rogers World Airport"},
    "Oregon":               {"sol": "2 years",   "notice": "180 days",         "gov": "Port of Portland"},
    "Pennsylvania":         {"sol": "2 years",   "notice": "6 months",         "gov": "Philadelphia Airport (city-owned)"},
    "Puerto Rico":          {"sol": "1 year",    "notice": "None required",    "gov": "Luis Munoz Marin Int'l Airport"},
    "Rhode Island":         {"sol": "3 years",   "notice": "None required",    "gov": "Rhode Island Airport Corp"},
    "South Carolina":       {"sol": "3 years",   "notice": "None required",    "gov": "Charleston Int'l Airport"},
    "South Dakota":         {"sol": "3 years",   "notice": "None required",    "gov": "Rapid City Regional Airport"},
    "Tennessee":            {"sol": "1 year",    "notice": "1 year",           "gov": "Metro Nashville Airport Auth"},
    "Texas":                {"sol": "2 years",   "notice": "None (most)",      "gov": "DFW Airport Board / City of Houston"},
    "US Virgin Islands":    {"sol": "2 years",   "notice": "6 months",         "gov": "Virgin Islands Port Authority"},
    "Utah":                 {"sol": "4 years",   "notice": "1 year",           "gov": "Salt Lake City Corp (SLC)"},
    "Vermont":              {"sol": "3 years",   "notice": "None required",    "gov": "Burlington Int'l Airport"},
    "Virginia":             {"sol": "2 years",   "notice": "None required",    "gov": "Metropolitan Washington Airports Auth"},
    "Washington":           {"sol": "3 years",   "notice": "None required",    "gov": "Port of Seattle"},
    "West Virginia":        {"sol": "2 years",   "notice": "None required",    "gov": "Yeager Airport, Charleston"},
    "Wisconsin":            {"sol": "3 years",   "notice": "120 days",         "gov": "Milwaukee County Airport"},
    "Wyoming":              {"sol": "4 years",   "notice": "None required",    "gov": "Jackson Hole Airport"},
}
DEFAULT_LEGAL = {"sol": "2-3 years", "notice": "60-180 days (varies)", "gov": "Local airport authority"}

# Airport type labels
TYPE_LABELS = {
    "large_hub":  "Large Hub",
    "medium_hub": "Medium Hub",
    "small_hub":  "Small Hub",
    "non_hub":    "Regional Airport",
}

# Accident category labels
CATEGORY_LABELS = {
    "premises_liability":   "Premises Liability",
    "vehicle_liability":    "Vehicle Liability",
    "federal_liability":    "Federal / Aviation Law",
    "workers_compensation": "Workers Compensation",
    "security_liability":   "Security Liability",
    "construction_liability":"Construction Liability",
    "environmental_liability":"Environmental Liability",
    "medical_negligence":   "Medical Negligence",
    "property_damage":      "Property Damage",
    "airline_liability":    "Airline Liability",
    "negligence":           "General Negligence",
    "civil_rights":         "Civil Rights",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_data():
    """Load all data files, return (airports, accidents, crossref)."""
    with open(AIRPORTS_CSV) as f:
        airports = list(csv.DictReader(f))

    with open(ACCIDENTS_JSON) as f:
        accidents = json.load(f)

    with open(CROSSREF_CSV) as f:
        crossref = list(csv.DictReader(f))

    return airports, accidents, crossref


def write_page(path: Path, html: str):
    """Write HTML to path, creating directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")


def render(template: str, context: dict) -> str:
    """
    Simple Mustache-style renderer.
    Replaces {{key}} with context[key].
    Handles {{#if key}} / {{/if}} blocks.
    """
    # Handle {{#if key}} ... {{/if}} blocks
    def replace_if_block(m):
        key   = m.group(1).strip()
        inner = m.group(2)
        return inner if context.get(key) else ""

    html = re.sub(
        r'\{\{#if\s+([\w_]+)\}\}(.*?)\{\{/if\}\}',
        replace_if_block,
        template,
        flags=re.DOTALL,
    )

    # Replace all {{key}} tokens
    for key, value in context.items():
        html = html.replace("{{" + key + "}}", str(value) if value is not None else "")

    # Remove any unreplaced tokens (optional vars not in context)
    html = re.sub(r'\{\{[\w_]+\}\}', '', html)

    return html


def truncate(text: str, max_len: int = 160) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len - 3].rsplit(' ', 1)[0] + '...'


def list_to_html_items(items) -> str:
    """Convert a list (or pipe-separated string) to <li> elements."""
    if not items:
        return ""
    if isinstance(items, str):
        items = [i.strip() for i in items.split('|') if i.strip()]
    return "\n".join(f"<li>{item}</li>" for item in items)


def airport_card_html(airport: dict, size_class: str = "") -> str:
    """Generate a single airport card for state hub pages."""
    slug       = airport['slug']
    name       = airport['airport_name']
    city       = airport['city']
    iata       = airport['iata_code'] or airport['faa_code']
    atype      = airport['type']
    type_label = TYPE_LABELS.get(atype, "Airport")

    type_css = {
        "large_hub":  "type--large",
        "medium_hub": "type--medium",
        "small_hub":  "type--small",
        "non_hub":    "type--non",
    }.get(atype, "type--non")

    large_class = "sh-airport-card--large" if atype == "large_hub" else ""

    return (
        f'<a href="/{slug}/" class="sh-airport-card {large_class}" role="listitem">'
        f'  <div class="sh-airport-card__code">{iata}</div>'
        f'  <div class="sh-airport-card__info">'
        f'    <div class="sh-airport-card__name">{name}</div>'
        f'    <div class="sh-airport-card__city">{city}</div>'
        f'  </div>'
        f'  <span class="sh-airport-card__type {type_css}">{type_label}</span>'
        f'  <span class="sh-airport-card__arrow" aria-hidden="true">→</span>'
        f'</a>'
    )


def nearby_airport_html(airport: dict) -> str:
    """Airport item for airport hub 'nearby airports' section."""
    return (
        f'<a href="/{airport["slug"]}/" class="airport-item" role="listitem">'
        f'  <div class="airport-item__code">{airport["iata_code"] or airport["faa_code"]}</div>'
        f'  <div>'
        f'    <div class="airport-item__city">{airport["city"]}</div>'
        f'    <div class="airport-item__name">{airport["airport_name"]}</div>'
        f'  </div>'
        f'</a>'
    )


def zone_card_html(zone: str, is_primary: bool = True) -> str:
    """Generate a zone card for accident hub pages."""
    tag = "Primary Zone" if is_primary else "Secondary Zone"
    return (
        f'<div class="card" role="listitem">'
        f'  <div class="aoh-zone__tag">{tag}</div>'
        f'  <div class="aoh-zone__title">{zone}</div>'
        f'</div>'
    )


def company_pill_html(company: str) -> str:
    return (
        f'<div class="aoh-company-pill" role="listitem">{company}</div>'
    )


def airport_link_html(airport: dict, accident_slug: str) -> str:
    """Link card for accident hub → airport leaf pages."""
    return (
        f'<a href="/{airport["slug"]}/{accident_slug}/" class="airport-item" role="listitem">'
        f'  <div class="airport-item__code">{airport["iata_code"] or airport["faa_code"]}</div>'
        f'  <div>'
        f'    <div class="airport-item__city">{airport["city"]}, {airport["state_code"]}</div>'
        f'    <div class="airport-item__name">{airport["airport_name"]}</div>'
        f'  </div>'
        f'</a>'
    )


def sol_short(sol_note: str) -> str:
    """Extract a short version of the SOL note."""
    match = re.search(r'\d[\d\-–]* years?', sol_note, re.I)
    return match.group(0) if match else "2-3 years"


def parse_list_field(value) -> list:
    """Parse a JSON list or pipe-separated string into a Python list."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass
        return [v.strip() for v in value.split('|') if v.strip()]
    return []


# ── State hub generator ──────────────────────────────────────────────────────

def generate_states(airports: list, template: str, dist: Path, filter_code: str = None):
    by_state = defaultdict(list)
    for a in airports:
        by_state[a['state']].append(a)

    generated = 0

    for state_name, state_airports in sorted(by_state.items()):
        state_code = state_airports[0]['state_code']

        if filter_code and state_code.upper() != filter_code.upper():
            continue

        legal = STATE_LEGAL.get(state_name, DEFAULT_LEGAL)

        large   = [a for a in state_airports if a['type'] == 'large_hub']
        medium  = [a for a in state_airports if a['type'] == 'medium_hub']
        small   = [a for a in state_airports if a['type'] == 'small_hub']
        regional = [a for a in state_airports if a['type'] == 'non_hub']

        # Airport cards per type
        large_html    = "\n".join(airport_card_html(a) for a in large)
        medium_html   = "\n".join(airport_card_html(a) for a in medium)
        small_html    = "\n".join(airport_card_html(a) for a in small)
        regional_html = "\n".join(airport_card_html(a) for a in regional)

        # Airport <option> list for form
        opts = []
        for a in sorted(state_airports, key=lambda x: (x['type'] != 'large_hub', x['airport_name'])):
            iata = a['iata_code'] or a['faa_code']
            opts.append(f'<option value="{a["slug"]}">{a["airport_name"]} ({iata})</option>')
        airport_options_html = "\n".join(opts)

        # Footer airports — show up to 6 largest
        footer_airports = sorted(state_airports, key=lambda x: (
            {'large_hub': 0, 'medium_hub': 1, 'small_hub': 2, 'non_hub': 3}[x['type']], x['airport_name']
        ))[:6]
        footer_html = "\n".join(
            f'<a href="/{a["slug"]}/">{a["airport_name"]} ({a["iata_code"] or a["faa_code"]})</a>'
            for a in footer_airports
        )

        context = {
            "state":                    state_name,
            "state_code":               state_code,
            "state_code_lower":         state_code.lower(),
            "airport_count":            len(state_airports),
            "large_hub_count":          len(large),
            "medium_hub_count":         len(medium),
            "small_hub_count":          len(small),
            "regional_count":           len(regional),
            "sol":                      legal["sol"],
            "notice_deadline":          legal["notice"],
            "gov_entity":               legal["gov"],
            "large_hubs_html":          large_html,
            "medium_hubs_html":         medium_html,
            "small_hubs_html":          small_html,
            "regional_hubs_html":       regional_html,
            "state_airport_options_html": airport_options_html,
            "footer_airports_html":     footer_html,
        }

        html = render(template, context)
        out  = dist / "state" / state_code.lower() / "index.html"
        write_page(out, html)
        generated += 1
        print(f"  ✓ state/{state_code.lower()}/ — {state_name} ({len(state_airports)} airports)")

    return generated


# ── Airport hub generator ────────────────────────────────────────────────────

def generate_airport_hubs(airports: list, template: str, dist: Path, filter_iata: str = None):
    by_state = defaultdict(list)
    for a in airports:
        by_state[a['state']].append(a)

    generated = 0

    for airport in airports:
        iata = airport['iata_code'] or airport['faa_code']

        if filter_iata and iata.upper() != filter_iata.upper():
            continue

        state_airports = by_state[airport['state']]
        nearby = [a for a in state_airports if a['slug'] != airport['slug']]
        nearby_html = "\n".join(nearby_airport_html(a) for a in nearby[:12])

        legal = STATE_LEGAL.get(airport['state'], DEFAULT_LEGAL)
        type_label = TYPE_LABELS.get(airport['type'], "Airport")

        # Airport operator — use gov entity from state legal data
        airport_operator = legal["gov"]

        context = {
            "airport_name":       airport['airport_name'],
            "airport_slug":       airport['slug'],
            "city":               airport['city'],
            "state":              airport['state'],
            "state_code":         airport['state_code'],
            "state_code_lower":   airport['state_code'].lower(),
            "iata_code":          iata,
            "faa_code":           airport['faa_code'],
            "airport_type_label": type_label,
            "airport_operator":   airport_operator,
            "nearby_airports_html": nearby_html,
        }

        html = render(template, context)
        out  = dist / airport['slug'] / "index.html"
        write_page(out, html)
        generated += 1

        if generated % 50 == 0:
            print(f"  ... {generated} airport hubs generated")

    print(f"  ✓ {generated} airport hub pages generated")
    return generated


# ── Accident hub generator ───────────────────────────────────────────────────

def generate_accident_hubs(accidents: list, airports: list, template: str, dist: Path, filter_slug: str = None):
    # Pre-sort airports by type for the airport links section
    sorted_airports = sorted(airports, key=lambda x: (
        {'large_hub': 0, 'medium_hub': 1, 'small_hub': 2, 'non_hub': 3}[x['type']],
        x['airport_name']
    ))
    # Show top 40 airports on accident hub pages
    featured_airports = [a for a in sorted_airports if a['type'] in ('large_hub', 'medium_hub')][:40]

    generated = 0

    for acc in accidents:
        if filter_slug and acc['slug'] != filter_slug:
            continue

        # Parse list fields
        primary_liable    = parse_list_field(acc.get('primary_liable_parties', []))
        secondary_liable  = parse_list_field(acc.get('secondary_liable_parties', []))
        damages           = parse_list_field(acc.get('compensable_damages', []))
        injuries          = parse_list_field(acc.get('common_injuries', []))
        evidence          = parse_list_field(acc.get('key_evidence', []))
        defenses          = parse_list_field(acc.get('liable_party_defenses', []))
        counters          = parse_list_field(acc.get('plaintiff_counter_strategies', []))
        primary_zones     = parse_list_field(acc.get('primary_airport_zones', []))
        secondary_zones   = parse_list_field(acc.get('secondary_airport_zones', []))
        companies         = parse_list_field(acc.get('airport_businesses_involved', []))
        categories        = parse_list_field(acc.get('business_categories', []))

        # Build HTML blocks
        primary_liable_html   = list_to_html_items(primary_liable)
        secondary_liable_html = list_to_html_items(secondary_liable)
        damages_html          = list_to_html_items(damages)
        injuries_html         = list_to_html_items(injuries)
        evidence_html         = list_to_html_items(evidence)
        defenses_html         = list_to_html_items(defenses)
        counter_html          = list_to_html_items(counters)

        primary_zones_html   = "\n".join(zone_card_html(z, True)  for z in primary_zones[:4])
        secondary_zones_html = "\n".join(zone_card_html(z, False) for z in secondary_zones[:4])

        companies_html  = "\n".join(company_pill_html(c) for c in companies[:24])
        categories_html = "\n".join(
            f'<span class="aoh-category-tag">{cat}</span>'
            for cat in categories
        )

        airport_links_html = "\n".join(
            airport_link_html(a, acc['slug']) for a in featured_airports
        )

        # FTCA / Montreal alerts
        ftca_alert = ""
        if acc.get('ftca_applies'):
            ftca_alert = (
                '<div class="alert-box" role="alert">'
                '<div class="alert-box__title">⚠ Federal Tort Claims Act (FTCA) Required</div>'
                '<p class="alert-box__text">Claims against TSA or other federal agencies must be filed '
                'under the FTCA — you must submit an administrative claim BEFORE filing a lawsuit. '
                'Missing this step permanently bars your case.</p></div>'
            )

        montreal_alert = ""
        if acc.get('montreal_convention'):
            montreal_alert = (
                '<div class="warning-box" role="note">'
                '<div class="warning-box__title">Montreal Convention — 2-Year Hard Deadline</div>'
                '<p class="warning-box__text">International flight claims are governed by the Montreal '
                'Convention. The 2-year limitation period is absolute with no exceptions. '
                'File immediately.</p></div>'
            )

        # Trust strip federal note
        if acc.get('ftca_applies'):
            federal_note = "FTCA Federal Filing Required"
        elif acc.get('montreal_convention'):
            federal_note = "Montreal Convention Applies"
        elif acc.get('federal_law_involved'):
            federal_note = "Federal aviation law applies"
        else:
            federal_note = "All liable parties pursued"

        category_label = CATEGORY_LABELS.get(acc.get('category', ''), 'Personal Injury')

        context = {
            "accident_name":          acc['accident_name'],
            "accident_slug":          acc['slug'],
            "category_label":         category_label,
            "description":            acc.get('description', ''),
            "frequency_label":        acc.get('frequency_label', 'Common'),
            "danger_label":           acc.get('danger_label', 'High'),
            "severity_score":         acc.get('severity_score', 7),
            "average_settlement_range": acc.get('average_settlement_range', 'Varies'),
            "sol_short":              sol_short(acc.get('statute_of_limitations_note', '2-3 years')),
            "evidence_preservation_urgency_upper": str(acc.get('evidence_preservation_urgency', 'HIGH')).upper(),
            "evidence_notes":         acc.get('evidence_notes', ''),
            "severity_class":         f"pill--{'danger' if acc.get('danger_level') in ('very_high','extreme') else 'warning'}",
            "freq_class":             f"pill--{'danger' if acc.get('frequency') == 'very_common' else 'warning' if acc.get('frequency') == 'common' else 'neutral'}",
            "federal_note":           federal_note,
            "primary_liable_html":    primary_liable_html,
            "secondary_liable_html":  secondary_liable_html,
            "compensable_damages_html": damages_html,
            "common_injuries_html":   injuries_html,
            "key_evidence_html":      evidence_html,
            "defenses_html":          defenses_html,
            "counter_html":           counter_html,
            "primary_zones_html":     primary_zones_html,
            "secondary_zones_html":   secondary_zones_html,
            "companies_html":         companies_html,
            "categories_html":        categories_html,
            "airport_links_html":     airport_links_html,
            "ftca_alert":             ftca_alert,
            "montreal_alert":         montreal_alert,
        }

        html = render(template, context)
        out  = dist / acc['slug'] / "index.html"
        write_page(out, html)
        generated += 1
        print(f"  ✓ {acc['slug']}/ — {acc['accident_name']}")

    return generated


# ── Copy static assets ───────────────────────────────────────────────────────

def copy_assets(dist: Path):
    """Copy /assets/ and homepage into dist/."""
    # Homepage
    dest_home = dist / "index.html"
    shutil.copy2(HOMEPAGE_SRC, dest_home)
    print(f"  ✓ index.html (homepage)")

    # Assets folder
    dest_assets = dist / "assets"
    if dest_assets.exists():
        shutil.rmtree(dest_assets)
    shutil.copytree(ASSETS_SRC, dest_assets)
    print(f"  ✓ assets/ (css, js)")


# ── Stats helper ─────────────────────────────────────────────────────────────

def count_pages(dist: Path) -> dict:
    total = sum(1 for _ in dist.rglob("index.html"))
    states   = sum(1 for _ in (dist / "state").rglob("index.html")) if (dist / "state").exists() else 0
    airports = sum(1 for p in dist.iterdir() if p.is_dir() and p.name not in ("state", "assets"))
    return {"total": total, "states": states, "airports": airports}


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="AirportAccidents.com page generator")
    parser.add_argument("--states",    action="store_true", help="Generate state hub pages only")
    parser.add_argument("--airports",  action="store_true", help="Generate airport hub pages only")
    parser.add_argument("--accidents", action="store_true", help="Generate accident hub pages only")
    parser.add_argument("--state",     metavar="CODE",  help="Generate single state, e.g. --state CA")
    parser.add_argument("--airport",   metavar="IATA",  help="Generate single airport, e.g. --airport LAX")
    parser.add_argument("--accident",  metavar="SLUG",  help="Generate single accident, e.g. --accident slip-and-fall")
    parser.add_argument("--dry-run",   action="store_true", help="Count pages without writing")
    parser.add_argument("--clean",     action="store_true", help="Delete dist/ before generating")
    args = parser.parse_args()

    # Determine what to generate
    generate_all = not any([args.states, args.airports, args.accidents, args.state, args.airport, args.accident])

    start = datetime.now()
    print(f"\n{'='*60}")
    print(f"AirportAccidents.com — Page Generator")
    print(f"{'='*60}")

    # Load data
    print("\n[1/5] Loading data...")
    airports, accidents, crossref = load_data()
    print(f"  ✓ {len(airports)} airports")
    print(f"  ✓ {len(accidents)} accident types")
    print(f"  ✓ {len(crossref)} crossref rows")

    if args.dry_run:
        by_state = defaultdict(list)
        for a in airports: by_state[a['state']].append(a)
        print(f"\n[DRY RUN] Page counts:")
        print(f"  State hubs:    {len(by_state)}")
        print(f"  Airport hubs:  {len(airports)}")
        print(f"  Accident hubs: {len(accidents)}")
        print(f"  Leaf pages:    {len(crossref)}")
        print(f"  TOTAL:         {len(by_state) + len(airports) + len(accidents) + len(crossref)}")
        return

    # Load templates
    print("\n[2/5] Loading templates...")
    tmpl_state    = TMPL_STATE.read_text(encoding="utf-8")
    tmpl_airport  = TMPL_AIRPORT.read_text(encoding="utf-8")
    tmpl_accident = TMPL_ACCIDENT.read_text(encoding="utf-8")
    print(f"  ✓ state-hub.html    ({TMPL_STATE.stat().st_size // 1024}KB)")
    print(f"  ✓ airport-hub.html  ({TMPL_AIRPORT.stat().st_size // 1024}KB)")
    print(f"  ✓ accident-hub.html ({TMPL_ACCIDENT.stat().st_size // 1024}KB)")

    # Set up dist directory
    print("\n[3/5] Setting up dist/...")
    if args.clean and DIST.exists():
        shutil.rmtree(DIST)
        print(f"  ✓ Cleaned dist/")
    DIST.mkdir(exist_ok=True)

    # Copy assets + homepage
    if generate_all or not any([args.states, args.airports, args.accidents]):
        print("\n[4/5] Copying static assets...")
        copy_assets(DIST)

    # Generate pages
    print("\n[5/5] Generating pages...")
    total = 0

    if generate_all or args.states or args.state:
        print("\n  — State hub pages —")
        total += generate_states(airports, tmpl_state, DIST, filter_code=args.state)

    if generate_all or args.airports or args.airport:
        print("\n  — Airport hub pages —")
        total += generate_airport_hubs(airports, tmpl_airport, DIST, filter_iata=args.airport)

    if generate_all or args.accidents or args.accident:
        print("\n  — Accident hub pages —")
        total += generate_accident_hubs(accidents, airports, tmpl_accident, DIST, filter_slug=args.accident)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'='*60}")
    print(f"✓ Generated {total} pages in {elapsed:.1f}s")
    print(f"  Output: {DIST}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
