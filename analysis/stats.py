"""
Statistical analysis module.
Computes taux_remplissage, prix moyen/médian, RevPAR, top listings, price evolution.
"""

from typing import Optional

import duckdb
import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table

from db.storage import build_scope_where

console = Console()


# ── Core metrics ──────────────────────────────────────────────────────────────

def taux_remplissage(
    conn: duckdb.DuckDBPyConnection,
    listing_id: str,
    days: int = 90,
) -> float:
    """
    Occupancy rate for a listing over the next `days` days.
    Returns percentage (0–100).
    """
    result = conn.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END), 0) AS indispo
        FROM availability
        WHERE listing_id = ?
          AND date >= CURRENT_DATE
          AND date < CURRENT_DATE + INTERVAL '90 days'
        """,
        [listing_id],
    ).fetchone()

    indispo = result[0] if result else 0
    return round(indispo / days * 100, 1)


def prix_moyen_ville(
    conn: duckdb.DuckDBPyConnection,
    ville: Optional[str] = None,
    source: Optional[str] = None,
    bbox: Optional[dict] = None,
) -> dict:
    """
    Return mean and median price per night for a city (by name or bbox).
    """
    where, params = build_scope_where(ville, source, bbox)
    where = (where + " AND l.prix_nuit IS NOT NULL") if where else "WHERE l.prix_nuit IS NOT NULL"

    row = conn.execute(
        f"""
        SELECT
            ROUND(AVG(l.prix_nuit), 2)    AS moyenne,
            ROUND(MEDIAN(l.prix_nuit), 2) AS mediane,
            COUNT(*)                      AS nb
        FROM listings l
        {where}
        """,
        params,
    ).fetchone()

    return {
        "moyenne": row[0] if row else None,
        "mediane": row[1] if row else None,
        "nb_listings": row[2] if row else 0,
    }


def revpar(
    conn: duckdb.DuckDBPyConnection,
    ville: Optional[str] = None,
    source: Optional[str] = None,
    bbox: Optional[dict] = None,
) -> Optional[float]:
    """
    Revenue Per Available Room (night) for a city (by name or bbox).
    RevPAR = prix_moyen * taux_remplissage_moyen / 100
    """
    where, params = build_scope_where(ville, source, bbox)
    where = (where + " AND l.prix_nuit IS NOT NULL") if where else "WHERE l.prix_nuit IS NOT NULL"

    row = conn.execute(
        f"""
        SELECT
            ROUND(
                AVG(l.prix_nuit) *
                AVG(
                    COALESCE(a.jours_indispo, 0) * 1.0 / 90
                ),
                2
            ) AS revpar
        FROM listings l
        LEFT JOIN (
            SELECT
                listing_id,
                SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END) AS jours_indispo
            FROM availability
            WHERE date >= CURRENT_DATE AND date < CURRENT_DATE + INTERVAL '90 days'
            GROUP BY listing_id
        ) a ON a.listing_id = l.id
        {where}
        """,
        params,
    ).fetchone()

    return row[0] if row else None


def top_listings(
    conn: duckdb.DuckDBPyConnection,
    ville: Optional[str] = None,
    n: int = 10,
    source: Optional[str] = None,
    bbox: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Return the top N listings by estimated RevPAR for a city (by name or bbox).
    """
    where, params = build_scope_where(ville, source, bbox)
    where = (where + " AND l.prix_nuit IS NOT NULL") if where else "WHERE l.prix_nuit IS NOT NULL"
    params.append(n)

    df = conn.execute(
        f"""
        SELECT
            l.id,
            l.titre,
            l.source,
            l.prix_nuit,
            l.note,
            l.nb_avis,
            ROUND(COALESCE(a.jours_indispo, 0) * 100.0 / 90, 1) AS taux_remplissage,
            ROUND(l.prix_nuit * COALESCE(a.jours_indispo, 0) / 90, 2) AS revpar_estime,
            l.url
        FROM listings l
        LEFT JOIN (
            SELECT
                listing_id,
                SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END) AS jours_indispo
            FROM availability
            WHERE date >= CURRENT_DATE AND date < CURRENT_DATE + INTERVAL '90 days'
            GROUP BY listing_id
        ) a ON a.listing_id = l.id
        {where}
        ORDER BY revpar_estime DESC NULLS LAST
        LIMIT ?
        """,
        params,
    ).df()

    return df


def evolution_prix(
    conn: duckdb.DuckDBPyConnection,
    ville: Optional[str] = None,
    source: Optional[str] = None,
    bbox: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Price evolution over time for a city (by name or bbox), aggregated by week.
    """
    where, params = build_scope_where(ville, source, bbox)
    where = (where + " AND ps.prix_nuit IS NOT NULL") if where else "WHERE ps.prix_nuit IS NOT NULL"

    df = conn.execute(
        f"""
        SELECT
            DATE_TRUNC('week', ps.scraped_at)   AS semaine,
            ROUND(AVG(ps.prix_nuit), 2)          AS prix_moyen,
            ROUND(MEDIAN(ps.prix_nuit), 2)       AS prix_median,
            COUNT(*)                             AS nb_snapshots
        FROM price_snapshots ps
        JOIN listings l ON l.id = ps.listing_id
        {where}
        GROUP BY semaine
        ORDER BY semaine ASC
        """,
        params,
    ).df()

    return df


def city_summary(
    conn: duckdb.DuckDBPyConnection,
    ville: Optional[str] = None,
    source: Optional[str] = None,
    bbox: Optional[dict] = None,
) -> dict:
    """
    Full summary dict for a city (by name or bbox).
    """
    pm = prix_moyen_ville(conn, ville, source, bbox=bbox)
    rv = revpar(conn, ville, source, bbox=bbox)

    where, params = build_scope_where(ville, source, bbox)

    taux_row = conn.execute(
        f"""
        SELECT ROUND(AVG(COALESCE(a.jours_indispo, 0) * 100.0 / 90), 1)
        FROM listings l
        LEFT JOIN (
            SELECT listing_id, SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END) AS jours_indispo
            FROM availability
            WHERE date >= CURRENT_DATE AND date < CURRENT_DATE + INTERVAL '90 days'
            GROUP BY listing_id
        ) a ON a.listing_id = l.id
        {where}
        """,
        params,
    ).fetchone()

    return {
        "ville": ville,
        "source": source or "all",
        "nb_listings": pm["nb_listings"],
        "prix_moyen": pm["moyenne"],
        "prix_median": pm["mediane"],
        "taux_remplissage_moyen": taux_row[0] if taux_row else None,
        "revpar": rv,
    }


def print_city_summary(
    conn: duckdb.DuckDBPyConnection,
    ville: str,
    bbox: Optional[dict] = None,
) -> None:
    """Pretty-print city summary using Rich."""
    summary = city_summary(conn, ville, bbox=bbox)
    tops = top_listings(conn, ville, n=5, bbox=bbox)

    table = Table(title=f"Résumé — {ville}", show_header=True, header_style="bold cyan")
    table.add_column("Métrique")
    table.add_column("Valeur", justify="right")

    table.add_row("Nb listings",         str(summary["nb_listings"]))
    table.add_row("Prix moyen / nuit",   f"{summary['prix_moyen']} €" if summary["prix_moyen"] else "—")
    table.add_row("Prix médian / nuit",  f"{summary['prix_median']} €" if summary["prix_median"] else "—")
    table.add_row("Taux remplissage moy", f"{summary['taux_remplissage_moyen']} %" if summary["taux_remplissage_moyen"] else "—")
    table.add_row("RevPAR estimé",       f"{summary['revpar']} €" if summary["revpar"] else "—")

    console.print(table)

    if not tops.empty:
        top_table = Table(title="Top 5 listings (RevPAR)", header_style="bold magenta")
        top_table.add_column("Titre")
        top_table.add_column("Source")
        top_table.add_column("Prix/nuit", justify="right")
        top_table.add_column("Taux rempl.", justify="right")
        top_table.add_column("RevPAR", justify="right")

        for _, row in tops.head(5).iterrows():
            top_table.add_row(
                str(row["titre"])[:40] if row["titre"] else "—",
                str(row["source"]),
                f"{row['prix_nuit']} €" if row["prix_nuit"] else "—",
                f"{row['taux_remplissage']} %" if row["taux_remplissage"] else "—",
                f"{row['revpar_estime']} €" if row["revpar_estime"] else "—",
            )

        console.print(top_table)
