"""
DuckDB storage layer.
Tables: listings, availability, price_snapshots
"""

import duckdb
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from rich.console import Console

console = Console()
DB_PATH = Path("rental_market.db")


# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS listings (
    id              VARCHAR PRIMARY KEY,
    source          VARCHAR NOT NULL,
    ville           VARCHAR NOT NULL,
    -- Identité
    titre           VARCHAR,
    type_bien       VARCHAR,
    superhost       BOOLEAN,
    instant_book    BOOLEAN,
    -- Capacité
    nb_voyageurs    INTEGER,
    nb_chambres     INTEGER,
    nb_lits         INTEGER,
    nb_sdb          INTEGER,
    -- Localisation
    lat             DOUBLE,
    lng             DOUBLE,
    zone_geo        VARCHAR,
    code_postal     VARCHAR,
    neighbourhood   VARCHAR,
    -- Tarifs
    prix_nuit       DOUBLE,
    prix_semaine    DOUBLE,
    prix_weekend    DOUBLE,
    cleaning_fee    DOUBLE,
    minimum_nights  INTEGER,
    -- Qualité
    note            DOUBLE,
    nb_avis         INTEGER,
    photos_count    INTEGER,
    -- Logement
    amenities       VARCHAR,
    -- Meta
    url             VARCHAR,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scanned_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS availability (
    listing_id  VARCHAR NOT NULL,
    date        DATE NOT NULL,
    is_available BOOLEAN NOT NULL,
    scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (listing_id, date)
);

CREATE SEQUENCE IF NOT EXISTS seq_price_snapshots;
CREATE TABLE IF NOT EXISTS price_snapshots (
    id          INTEGER PRIMARY KEY DEFAULT nextval('seq_price_snapshots'),
    listing_id  VARCHAR NOT NULL,
    prix_nuit   DOUBLE NOT NULL,
    scraped_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE SEQUENCE IF NOT EXISTS seq_scan_log;
CREATE TABLE IF NOT EXISTS scan_log (
    id              INTEGER PRIMARY KEY DEFAULT nextval('seq_scan_log'),
    ville           VARCHAR NOT NULL,
    source          VARCHAR,
    started_at      TIMESTAMP NOT NULL,
    ended_at        TIMESTAMP,
    duration_s      DOUBLE,
    status          VARCHAR NOT NULL,
    nb_listings     INTEGER,
    nb_inserted     INTEGER,
    nb_updated      INTEGER,
    nb_errors       INTEGER,
    dry_run         BOOLEAN DEFAULT FALSE,
    message         VARCHAR
);
"""


# ── Connection ────────────────────────────────────────────────────────────────

def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection, creating the DB file if needed."""
    conn = duckdb.connect(str(DB_PATH))
    conn.execute(SCHEMA_SQL)
    _migrate_schema(conn)
    return conn


def _migrate_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Add new columns to existing DB if they don't exist yet."""
    try:
        existing = {row[0] for row in conn.execute("DESCRIBE listings").fetchall()}
        new_cols = {
            "zone_geo":       "VARCHAR",
            "last_scanned_at":"TIMESTAMP",
            "type_bien":      "VARCHAR",
            "nb_voyageurs":   "INTEGER",
            "nb_chambres":    "INTEGER",
            "nb_lits":        "INTEGER",
            "nb_sdb":         "INTEGER",
            # v2 enriched fields
            "superhost":      "BOOLEAN",
            "instant_book":   "BOOLEAN",
            "minimum_nights": "INTEGER",
            "cleaning_fee":   "DOUBLE",
            "amenities":      "VARCHAR",
            "photos_count":   "INTEGER",
            "prix_semaine":   "DOUBLE",
            "prix_weekend":   "DOUBLE",
            "code_postal":    "VARCHAR",
            "neighbourhood":  "VARCHAR",
        }
        for col_name, col_type in new_cols.items():
            if col_name not in existing:
                conn.execute(f"ALTER TABLE listings ADD COLUMN {col_name} {col_type}")
                console.log(f"[dim]Migration: colonne {col_name} ajoutée[/dim]")
    except Exception as e:
        console.log(f"[yellow]Migration schema (non bloquant): {e}[/yellow]")


# ── Upsert listing ────────────────────────────────────────────────────────────

def _make_listing_id(source: str, id_externe: Optional[str], url: Optional[str]) -> str:
    """Build a stable unique ID for a listing."""
    if id_externe:
        return f"{source}_{id_externe}"
    if url:
        import hashlib
        return f"{source}_{hashlib.md5(url.encode()).hexdigest()[:10]}"
    raise ValueError("Cannot build listing ID: no id_externe or url provided")


def upsert_listing(conn: duckdb.DuckDBPyConnection, listing: dict) -> tuple[str, str]:
    """
    Insert or update a listing.
    Returns (listing_id, "inserted" | "updated").
    """
    listing_id = _make_listing_id(
        listing["source"],
        listing.get("id_externe"),
        listing.get("url"),
    )
    last_scanned_at = listing.get("last_scanned_at")

    existing = conn.execute(
        "SELECT id FROM listings WHERE id = ?", [listing_id]
    ).fetchone()

    fields = [
        # always overwrite
        ("titre",          listing.get("titre")),
        ("updated_at",     None),                      # handled by SQL CURRENT_TIMESTAMP
        # COALESCE: keep existing value if new is NULL
        ("zone_geo",       listing.get("zone_geo")),
        ("type_bien",      listing.get("type_bien")),
        ("superhost",      listing.get("superhost")),
        ("instant_book",   listing.get("instant_book")),
        ("nb_voyageurs",   listing.get("nb_voyageurs")),
        ("nb_chambres",    listing.get("nb_chambres")),
        ("nb_lits",        listing.get("nb_lits")),
        ("nb_sdb",         listing.get("nb_sdb")),
        ("lat",            listing.get("lat")),
        ("lng",            listing.get("lng")),
        ("code_postal",    listing.get("code_postal")),
        ("neighbourhood",  listing.get("neighbourhood")),
        ("prix_nuit",      listing.get("prix_nuit")),
        ("prix_semaine",   listing.get("prix_semaine")),
        ("prix_weekend",   listing.get("prix_weekend")),
        ("cleaning_fee",   listing.get("cleaning_fee")),
        ("minimum_nights", listing.get("minimum_nights")),
        ("note",           listing.get("note")),
        ("nb_avis",        listing.get("nb_avis")),
        ("photos_count",   listing.get("photos_count")),
        ("amenities",      listing.get("amenities")),
        ("last_scanned_at", last_scanned_at),
    ]
    # Columns that use COALESCE (keep existing if new is NULL)
    coalesce_cols = {
        "zone_geo", "type_bien", "superhost", "instant_book",
        "nb_voyageurs", "nb_chambres", "nb_lits", "nb_sdb",
        "lat", "lng", "code_postal", "neighbourhood",
        "prix_nuit", "prix_semaine", "prix_weekend", "cleaning_fee",
        "minimum_nights", "note", "nb_avis", "photos_count",
        "amenities", "last_scanned_at",
    }

    if existing:
        set_clauses = ["titre = ?", "updated_at = CURRENT_TIMESTAMP"]
        params = [listing.get("titre")]
        for col, val in fields:
            if col in ("titre", "updated_at"):
                continue
            if col in coalesce_cols:
                set_clauses.append(f"{col} = COALESCE(?, {col})")
            else:
                set_clauses.append(f"{col} = ?")
            params.append(val)
        params.append(listing_id)
        conn.execute(
            f"UPDATE listings SET {', '.join(set_clauses)} WHERE id = ?",
            params,
        )
        console.log(f"[dim]Listing MàJ : {listing_id}[/dim]")
        return listing_id, "updated"

    conn.execute(
        """
        INSERT INTO listings (
            id, source, ville,
            titre, type_bien, superhost, instant_book,
            nb_voyageurs, nb_chambres, nb_lits, nb_sdb,
            lat, lng, zone_geo, code_postal, neighbourhood,
            prix_nuit, prix_semaine, prix_weekend, cleaning_fee, minimum_nights,
            note, nb_avis, photos_count, amenities,
            url, last_scanned_at
        ) VALUES (
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?
        )
        """,
        [
            listing_id, listing["source"], listing["ville"],
            listing.get("titre"), listing.get("type_bien"),
            listing.get("superhost"), listing.get("instant_book"),
            listing.get("nb_voyageurs"), listing.get("nb_chambres"),
            listing.get("nb_lits"), listing.get("nb_sdb"),
            listing.get("lat"), listing.get("lng"),
            listing.get("zone_geo"), listing.get("code_postal"), listing.get("neighbourhood"),
            listing.get("prix_nuit"), listing.get("prix_semaine"), listing.get("prix_weekend"),
            listing.get("cleaning_fee"), listing.get("minimum_nights"),
            listing.get("note"), listing.get("nb_avis"), listing.get("photos_count"),
            listing.get("amenities"),
            listing.get("url"), last_scanned_at,
        ],
    )
    console.log(f"[dim]Listing inséré : {listing_id}[/dim]")
    return listing_id, "inserted"


def save_availability(
    conn: duckdb.DuckDBPyConnection,
    listing_id: str,
    availability: dict[str, bool],
) -> None:
    """
    Save or update availability records for a listing.
    availability: {date_str (ISO): is_available}
    """
    if not availability:
        return

    rows = [
        (listing_id, date_str, is_avail)
        for date_str, is_avail in availability.items()
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO availability (listing_id, date, is_available)
        VALUES (?, ?, ?)
        """,
        rows,
    )
    console.log(f"[dim]{len(rows)} entrées availability sauvegardées pour {listing_id}[/dim]")


def save_price_snapshot(
    conn: duckdb.DuckDBPyConnection,
    listing_id: str,
    prix_nuit: float,
) -> None:
    """Append a price snapshot for historical tracking."""
    if prix_nuit is None:
        return

    conn.execute(
        """
        INSERT INTO price_snapshots (listing_id, prix_nuit)
        VALUES (?, ?)
        """,
        [listing_id, prix_nuit],
    )


# ── Batch save ────────────────────────────────────────────────────────────────

def save_listings_batch(
    conn: duckdb.DuckDBPyConnection,
    listings: list[dict],
) -> dict:
    """
    Save a batch of listings with their availability and price snapshots.
    Returns counts: {"inserted": int, "updated": int, "errors": int}.
    """
    counts = {"inserted": 0, "updated": 0, "errors": 0}
    for listing in listings:
        try:
            listing_id, status = upsert_listing(conn, listing)
            counts[status] += 1

            if listing.get("availability"):
                save_availability(conn, listing_id, listing["availability"])

            if listing.get("prix_nuit") is not None:
                save_price_snapshot(conn, listing_id, listing["prix_nuit"])

        except Exception as e:
            console.log(f"[red]Erreur sauvegarde listing '{listing.get('titre')}': {e}[/red]")
            counts["errors"] += 1
            continue

    console.log(f"[green]{len(listings)} listings traités en base[/green]")
    return counts


# ── Scan log ──────────────────────────────────────────────────────────────────

def log_scan_start(
    conn: duckdb.DuckDBPyConnection,
    ville: str,
    source: Optional[str],
    dry_run: bool = False,
) -> int:
    """Insert a scan_log row with status='running'. Returns the scan id."""
    row = conn.execute(
        """
        INSERT INTO scan_log (ville, source, started_at, status, dry_run)
        VALUES (?, ?, CURRENT_TIMESTAMP, 'running', ?)
        RETURNING id
        """,
        [ville, source, dry_run],
    ).fetchone()
    return row[0]


def log_scan_end(
    conn: duckdb.DuckDBPyConnection,
    scan_id: int,
    status: str,
    nb_listings: int = 0,
    nb_inserted: int = 0,
    nb_updated: int = 0,
    nb_errors: int = 0,
    message: Optional[str] = None,
) -> None:
    """Finalize a scan_log row (status: 'success'|'error'|'partial')."""
    conn.execute(
        """
        UPDATE scan_log
        SET ended_at    = CURRENT_TIMESTAMP,
            duration_s  = EPOCH(CURRENT_TIMESTAMP) - EPOCH(started_at),
            status      = ?,
            nb_listings = ?,
            nb_inserted = ?,
            nb_updated  = ?,
            nb_errors   = ?,
            message     = ?
        WHERE id = ?
        """,
        [status, nb_listings, nb_inserted, nb_updated, nb_errors, message, scan_id],
    )


def get_recent_scans(
    conn: duckdb.DuckDBPyConnection,
    limit: int = 50,
) -> pd.DataFrame:
    """Return the last `limit` scan_log rows, newest first."""
    return conn.execute(
        """
        SELECT id, ville, source, started_at, ended_at,
               ROUND(duration_s, 1) AS duration_s,
               status, nb_listings, nb_inserted, nb_updated, nb_errors,
               dry_run, message
        FROM scan_log
        ORDER BY started_at DESC
        LIMIT ?
        """,
        [limit],
    ).df()


# ── Skip-list (avoid rescan) ──────────────────────────────────────────────────

def get_recently_scanned_ids(
    conn: duckdb.DuckDBPyConnection,
    ville: str,
    max_age_days: int = 7,
) -> set[str]:
    """
    Return internal listing IDs that were fully scanned within the last max_age_days.
    Used to skip listing-page scrapes for recently processed listings.
    """
    rows = conn.execute(
        """
        SELECT id FROM listings
        WHERE ville = ?
          AND last_scanned_at IS NOT NULL
          AND last_scanned_at >= CURRENT_TIMESTAMP - INTERVAL (? || ' days')
        """,
        [ville, str(max_age_days)],
    ).fetchall()
    return {row[0] for row in rows}


# ── Queries ───────────────────────────────────────────────────────────────────

def build_scope_where(
    ville: Optional[str] = None,
    source: Optional[str] = None,
    bbox: Optional[dict] = None,
    alias: str = "l",
) -> tuple[str, list]:
    """
    Build a shared WHERE clause for ville/source/bbox filters.
    If bbox is given, listings are filtered geographically (ville filter ignored).
    Returns (sql_fragment_starting_with_WHERE_or_empty, params_list).
    """
    conditions: list[str] = []
    params: list = []

    if bbox:
        conditions.append(
            f"{alias}.lat BETWEEN ? AND ? AND {alias}.lng BETWEEN ? AND ?"
        )
        params.extend([bbox["south"], bbox["north"], bbox["west"], bbox["east"]])
    elif ville:
        conditions.append(f"{alias}.ville = ?")
        params.append(ville)

    if source:
        conditions.append(f"{alias}.source = ?")
        params.append(source)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where, params


def get_stats_by_city(
    conn: duckdb.DuckDBPyConnection,
    ville: Optional[str] = None,
    source: Optional[str] = None,
    bbox: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Return aggregated stats for a city (by ville name or bbox).
    Columns: source, nb_listings, prix_moyen, prix_median, note_moyenne
    """
    where, params = build_scope_where(ville, source, bbox)

    query = f"""
    SELECT
        l.source,
        COUNT(DISTINCT l.id)            AS nb_listings,
        ROUND(AVG(l.prix_nuit), 2)      AS prix_moyen,
        ROUND(MEDIAN(l.prix_nuit), 2)   AS prix_median,
        ROUND(AVG(l.note), 2)           AS note_moyenne
    FROM listings l
    {where}
    GROUP BY l.source
    """
    return conn.execute(query, params).df()


def get_all_listings(
    conn: duckdb.DuckDBPyConnection,
    ville: Optional[str] = None,
    source: Optional[str] = None,
    bbox: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Return all listings with computed taux_remplissage.
    If bbox is given, filters geographically instead of by ville name.
    """
    where, params = build_scope_where(ville, source, bbox)

    query = f"""
    SELECT
        l.id,
        l.source,
        l.ville,
        -- Identité
        l.titre,
        l.type_bien,
        l.superhost,
        l.instant_book,
        -- Capacité
        l.nb_voyageurs,
        l.nb_chambres,
        l.nb_lits,
        l.nb_sdb,
        -- Localisation
        l.lat,
        l.lng,
        l.zone_geo,
        l.code_postal,
        l.neighbourhood,
        -- Tarifs
        l.prix_nuit,
        l.prix_semaine,
        l.prix_weekend,
        l.cleaning_fee,
        l.minimum_nights,
        -- Qualité
        l.note,
        l.nb_avis,
        l.photos_count,
        -- Logement
        l.amenities,
        -- Meta
        l.url,
        l.created_at,
        l.last_scanned_at,
        -- Calculés
        COALESCE(a90.jours_indispo, 0)                           AS jours_indispo_90,
        ROUND(COALESCE(a90.jours_indispo, 0) * 100.0 / 90, 1)   AS taux_remplissage_90,
        COALESCE(a365.jours_indispo, 0)                          AS jours_indispo_365,
        ROUND(COALESCE(a365.jours_indispo, 0) * 100.0 / 365, 1) AS taux_remplissage_365,
        ROUND(l.prix_nuit * COALESCE(a90.jours_indispo, 0) / 90, 2)    AS revpar,
        ROUND(l.prix_nuit * COALESCE(a365.jours_indispo, 0) / 12, 2)   AS revenu_mensuel_estime
    FROM listings l
    LEFT JOIN (
        SELECT
            listing_id,
            SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END) AS jours_indispo
        FROM availability
        WHERE date >= CURRENT_DATE AND date < CURRENT_DATE + INTERVAL '90 days'
        GROUP BY listing_id
    ) a90 ON a90.listing_id = l.id
    LEFT JOIN (
        SELECT
            listing_id,
            SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END) AS jours_indispo
        FROM availability
        WHERE date >= CURRENT_DATE AND date < CURRENT_DATE + INTERVAL '365 days'
        GROUP BY listing_id
    ) a365 ON a365.listing_id = l.id
    {where}
    ORDER BY revpar DESC NULLS LAST
    """
    return conn.execute(query, params).df()
