"""
Folium map with toggleable heatmap layers (price / occupancy).
"""

import folium
from folium.plugins import HeatMap
import pandas as pd
import streamlit as st
import streamlit as _st_iframe


SOURCE_COLORS = {
    "airbnb": "red",
    "booking": "blue",
}


def _popup_html(row: pd.Series) -> str:
    taux = f"{row['taux_remplissage_90']:.1f} %" if pd.notna(row.get("taux_remplissage_90")) else "—"
    prix = f"{row['prix_nuit']:.0f} €/nuit" if pd.notna(row.get("prix_nuit")) else "—"
    note = f"{row['note']:.1f} ⭐" if pd.notna(row.get("note")) else "—"
    revpar = f"{row['revpar']:.0f} €" if pd.notna(row.get("revpar")) else "—"
    url = row.get("url", "")

    source_label = str(row.get("source", "")).capitalize()
    title = str(row.get("titre", ""))[:50] or "Sans titre"

    link = f'<a href="{url}" target="_blank">Voir l\'annonce</a>' if url else ""

    return f"""
    <div style="font-family:sans-serif; font-size:13px; min-width:180px;">
        <b style="font-size:14px;">{title}</b><br>
        <span style="color:gray;">{source_label}</span><br><br>
        <b>Prix :</b> {prix}<br>
        <b>Note :</b> {note}<br>
        <b>Taux rempl. :</b> {taux}<br>
        <b>RevPAR est. :</b> {revpar}<br><br>
        {link}
    </div>
    """


def build_map(
    df: pd.DataFrame,
    center_lat: float,
    center_lng: float,
    zoom: int = 13,
) -> folium.Map:
    """
    Build and return a Folium map with:
    - Colored markers per source (Airbnb=red, Booking=blue)
    - Toggleable HeatMap layer by price
    - Toggleable HeatMap layer by occupancy rate
    - LayerControl
    """
    m = folium.Map(
        location=[center_lat, center_lng],
        zoom_start=zoom,
        tiles="CartoDB positron",
        control_scale=True,
    )

    # ── Marker feature groups ─────────────────────────────────────────────────
    fg_airbnb = folium.FeatureGroup(name="Airbnb", show=True)
    fg_booking = folium.FeatureGroup(name="Booking", show=True)

    valid = df.dropna(subset=["lat", "lng"])

    for _, row in valid.iterrows():
        source = str(row.get("source", "")).lower()
        color = SOURCE_COLORS.get(source, "gray")
        fg = fg_airbnb if source == "airbnb" else fg_booking

        folium.CircleMarker(
            location=[row["lat"], row["lng"]],
            radius=6,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(
                folium.IFrame(_popup_html(row), width=220, height=180),
                max_width=240,
            ),
            tooltip=f"{str(row.get('titre', ''))[:30]} — {row.get('prix_nuit', '?')} €",
        ).add_to(fg)

    fg_airbnb.add_to(m)
    fg_booking.add_to(m)

    # ── HeatMap: Price ────────────────────────────────────────────────────────
    price_data = valid.dropna(subset=["prix_nuit"])
    if not price_data.empty:
        max_price = price_data["prix_nuit"].max()
        heat_price = [
            [row["lat"], row["lng"], row["prix_nuit"] / max_price]
            for _, row in price_data.iterrows()
        ]
        HeatMap(
            heat_price,
            name="Heatmap — Prix/nuit",
            min_opacity=0.3,
            max_zoom=16,
            radius=20,
            blur=15,
            gradient={0.2: "blue", 0.5: "yellow", 0.8: "orange", 1.0: "red"},
            show=False,
        ).add_to(m)

    # ── HeatMap: Occupancy ────────────────────────────────────────────────────
    occ_data = valid.dropna(subset=["taux_remplissage_90"])
    if not occ_data.empty:
        heat_occ = [
            [row["lat"], row["lng"], row["taux_remplissage_90"] / 100]
            for _, row in occ_data.iterrows()
        ]
        HeatMap(
            heat_occ,
            name="Heatmap — Taux remplissage",
            min_opacity=0.3,
            max_zoom=16,
            radius=20,
            blur=15,
            gradient={0.2: "green", 0.5: "yellow", 1.0: "red"},
            show=False,
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    return m


def render_map(
    df: pd.DataFrame,
    center_lat: float,
    center_lng: float,
    zoom: int = 13,
    height: int = 550,
) -> None:
    """Render the Folium map inside a Streamlit component."""
    if df.empty:
        st.info("Aucun listing avec coordonnées disponibles pour la carte.")
        return

    m = build_map(df, center_lat, center_lng, zoom)
    map_html = m._repr_html_()
    _st_iframe.iframe(map_html, height=height)
