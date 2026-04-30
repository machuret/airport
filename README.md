# AirportAccidents.com

Programmatic SEO site targeting legal lead generation for airport injury victims.

## Structure

```
airport/
├── data/
│   ├── us_airports_full.csv          # 522 US airports with slugs, FAA codes, types
│   ├── accidents_database_v2.json    # 25 accident types × 60 columns
│   ├── accidents_database_v2.csv     # Same, CSV format
│   └── airports_accidents_crossref.csv  # 13,050 page URLs with SEO metadata
├── public/
│   └── index.html                    # Homepage
├── scripts/
│   └── build_enhanced_accident_db.py # Accident database generator
└── README.md
```

## Scale

- **522 airports** across all 50 states + DC, Puerto Rico, Guam, USVI
- **25 accident types** with 60 data columns each (liable parties, evidence, legal standards, companies involved, settlement ranges)
- **13,050 total pages** to generate

## Build Order

1. Phase 1: 53 large hubs × 25 = 1,325 pages
2. Phase 2: 39 medium hubs × 25 = 975 pages
3. Phase 3: 71 small hubs × 25 = 1,775 pages
4. Phase 4: 359 non-hub × 25 = 8,975 pages
