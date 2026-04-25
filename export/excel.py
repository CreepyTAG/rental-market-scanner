"""
Export listings to Excel (.xlsx) with summary stats and seasonality.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
from rich.console import Console

from db.storage import get_all_listings, get_stats_by_city, build_scope_where

console = Console()
EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Month labels in French
MONTH_LABELS = {
    "01": "Janvier", "02": "Février", "03": "Mars", "04": "Avril",
    "05": "Mai", "06": "Juin", "07": "Juillet", "08": "Août",
    "09": "Septembre", "10": "Octobre", "11": "Novembre", "12": "Décembre",
}


def _build_seasonality_df(
    conn: duckdb.DuckDBPyConnection,
    ville: Optional[str] = None,
    bbox: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Build a per-listing, per-month occupancy DataFrame from availability data.
    Columns: ID, Titre, Zone Géo, Jan, Fév, ..., Déc (occupancy %)
    """
    where, params = build_scope_where(ville, None, bbox)
    where = (where + " AND a.date >= CURRENT_DATE") if where else "WHERE a.date >= CURRENT_DATE"

    df = conn.execute(
        f"""
        SELECT
            a.listing_id,
            l.titre,
            l.zone_geo,
            STRFTIME(a.date, '%m') AS mois,
            COUNT(*) AS total_jours,
            SUM(CASE WHEN NOT a.is_available THEN 1 ELSE 0 END) AS jours_occupes
        FROM availability a
        JOIN listings l ON l.id = a.listing_id
        {where}
        GROUP BY a.listing_id, l.titre, l.zone_geo, mois
        ORDER BY a.listing_id, mois
        """,
        params,
    ).df()

    if df.empty:
        return pd.DataFrame()

    # Compute occupancy percentage
    df["taux"] = (df["jours_occupes"] / df["total_jours"] * 100).round(1)

    # Pivot: one row per listing, one column per month
    pivot = df.pivot_table(
        index=["listing_id", "titre", "zone_geo"],
        columns="mois",
        values="taux",
        aggfunc="first",
    ).reset_index()

    # Rename month columns to French labels
    pivot.columns = [
        MONTH_LABELS.get(c, c) if c not in ("listing_id", "titre", "zone_geo") else c
        for c in pivot.columns
    ]
    pivot = pivot.rename(columns={
        "listing_id": "ID",
        "titre": "Titre",
        "zone_geo": "Zone Géo",
    })

    return pivot


def export_to_excel(
    conn: duckdb.DuckDBPyConnection,
    ville: str,
    source: Optional[str] = None,
    output_path: Optional[Path] = None,
    bbox: Optional[dict] = None,
) -> Path:
    """
    Export listings for a city to a single rolling Excel file (overwritten each scan).
    Three sheets: Listings, Saisonnalité, Résumé.
    If bbox is given, filters geographically (includes listings tagged with other villes).
    """
    # ── Build output path (one file per ville, rolling) ───────────────────────
    if output_path is None:
        import unicodedata
        slug = unicodedata.normalize("NFKD", ville).encode("ascii", "ignore").decode()
        slug = slug.lower().replace(" ", "_").replace("'", "").replace("-", "_")
        output_path = EXPORT_DIR / f"scan_{slug}.xlsx"

    # ── Fetch data ────────────────────────────────────────────────────────────
    df_listings = get_all_listings(conn, ville=ville, source=source, bbox=bbox)
    df_stats = get_stats_by_city(conn, ville=ville, source=source, bbox=bbox)
    df_season = _build_seasonality_df(conn, ville=ville, bbox=bbox)

    if df_listings.empty:
        console.print(f"[yellow]Aucun listing trouvé pour {ville}, export annulé.[/yellow]")
        return output_path

    # ── Rename columns for readability ────────────────────────────────────────
    col_rename = {
        "id": "ID",
        "source": "Source",
        "ville": "Ville",
        # Identité
        "titre": "Titre",
        "type_bien": "Type de bien",
        "superhost": "Superhôte",
        "instant_book": "Résa instantanée",
        # Capacité
        "nb_voyageurs": "Voyageurs max",
        "nb_chambres": "Chambres",
        "nb_lits": "Lits",
        "nb_sdb": "Salles de bain",
        # Localisation
        "lat": "Latitude",
        "lng": "Longitude",
        "zone_geo": "Zone Géo",
        "code_postal": "Code postal",
        "neighbourhood": "Quartier",
        # Tarifs
        "prix_nuit": "Prix/nuit (€)",
        "prix_semaine": "Prix semaine (€)",
        "prix_weekend": "Prix week-end (€)",
        "cleaning_fee": "Frais ménage (€)",
        "minimum_nights": "Nuits min.",
        # Qualité
        "note": "Note",
        "nb_avis": "Nb avis",
        "photos_count": "Nb photos",
        # Logement
        "amenities": "Équipements",
        # Meta
        "url": "URL Airbnb",
        "created_at": "Première détection",
        "last_scanned_at": "Dernier scan",
        # Calculés
        "jours_indispo_90": "Jours réservés (90j)",
        "taux_remplissage_90": "Taux rempl. 90j (%)",
        "jours_indispo_365": "Jours réservés (365j)",
        "taux_remplissage_365": "Taux rempl. 365j (%)",
        "revpar": "RevPAR (€)",
        "revenu_mensuel_estime": "Revenu mensuel est. (€)",
    }
    df_export = df_listings.rename(columns={k: v for k, v in col_rename.items() if k in df_listings.columns})

    stats_rename = {
        "source": "Source",
        "nb_listings": "Nb listings",
        "prix_moyen": "Prix moyen (€)",
        "prix_median": "Prix médian (€)",
        "note_moyenne": "Note moyenne",
    }
    df_stats_export = df_stats.rename(columns={k: v for k, v in stats_rename.items() if k in df_stats.columns})

    # ── Write Excel ───────────────────────────────────────────────────────────
    sheets = ["Listings", "Saisonnalité", "Résumé"]
    sheet_data = {
        "Listings": df_export,
        "Résumé": df_stats_export,
    }
    if not df_season.empty:
        sheet_data["Saisonnalité"] = df_season

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name in sheets:
            if sheet_name in sheet_data:
                sheet_data[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)

        # Auto-adjust column widths
        for sheet_name, df_sheet in sheet_data.items():
            if sheet_name not in writer.sheets:
                continue
            ws = writer.sheets[sheet_name]
            for i, col in enumerate(df_sheet.columns, 1):
                max_len = max(
                    len(str(col)),
                    df_sheet[col].astype(str).str.len().max() if not df_sheet[col].empty else 0,
                )
                ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(max_len + 3, 50)

    console.print(f"[bold green]Export Excel : {output_path}[/bold green]")
    console.print(f"  {len(df_export)} listings, {len(df_season)} avec saisonnalité")
    return output_path
