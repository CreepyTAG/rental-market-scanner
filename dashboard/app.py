"""
Streamlit dashboard — Rental Market Scanner.
Run: streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import yaml

from db.storage import get_connection, get_all_listings
from analysis.stats import city_summary, top_listings
from dashboard.map_view import render_map

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Rental Market Scanner",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
[data-testid="metric-container"] { background:#1e1e2e; border-radius:8px; padding:12px; }
</style>
""", unsafe_allow_html=True)

COLORS = {"airbnb": "#FF5A5F", "booking": "#003580", "vrbo": "#F9C33C", "abritel": "#F9C33C"}

# ── Config ────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=0)
def load_cities() -> dict:
    with open(Path(__file__).parent.parent / "config" / "cities.yaml") as f:
        return yaml.safe_load(f)["cities"]

cities_config = load_cities()
city_names = {v["name"]: k for k, v in cities_config.items()}

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🏠 Rental Scanner")
    st.markdown("---")

    selected_city_name = st.selectbox("Ville", options=list(city_names.keys()))
    city_key  = city_names[selected_city_name]
    city_cfg  = cities_config[city_key]
    bbox      = city_cfg["bbox"]

    source_filter = st.radio("Source", ["Toutes", "Airbnb", "Booking", "VRBO"], horizontal=True)
    source_param  = None if source_filter == "Toutes" else source_filter.lower()

    st.markdown("---")
    st.subheader("Filtres")

    prix_range = st.slider("Prix / nuit (€)", 0, 1000, (0, 1000), step=10)
    chambres_min = st.selectbox("Chambres min.", [0, 1, 2, 3, 4, 5], index=0)
    note_min = st.slider("Note minimum", 0.0, 5.0, 0.0, step=0.1)
    superhost_only = st.checkbox("Superhôtes uniquement")

    st.markdown("---")
    if st.button("🔄 Rafraîchir"):
        st.cache_data.clear()
        st.rerun()

# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_df(city_key: str, source: str | None, bbox: dict) -> pd.DataFrame:
    conn = get_connection()
    df = get_all_listings(conn, source=source, bbox=bbox)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_summary(city_key: str, source: str | None, bbox: dict) -> dict:
    conn = get_connection()
    s = city_summary(conn, source=source, bbox=bbox)
    conn.close()
    return s

@st.cache_data(ttl=300)
def load_top(city_key: str, source: str | None, bbox: dict) -> pd.DataFrame:
    conn = get_connection()
    df = top_listings(conn, n=20, source=source, bbox=bbox)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_availability(city_key: str, bbox: dict) -> pd.DataFrame:
    conn = get_connection()
    df = conn.execute("""
        SELECT a.listing_id, a.date, a.is_available,
               l.prix_nuit, l.neighbourhood, l.source
        FROM availability a
        JOIN listings l ON l.id = a.listing_id
        WHERE l.lat BETWEEN ? AND ?
          AND l.lng BETWEEN ? AND ?
          AND a.date >= CURRENT_DATE
          AND a.date < CURRENT_DATE + INTERVAL '365 days'
    """, [bbox["south"], bbox["north"], bbox["west"], bbox["east"]]).df()
    conn.close()
    return df

raw_df   = load_df(city_key, source_param, bbox)
summary  = load_summary(city_key, source_param, bbox)
top_df   = load_top(city_key, source_param, bbox)
avail_df = load_availability(city_key, bbox)

# ── Apply sidebar filters ─────────────────────────────────────────────────────

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    if "prix_nuit" in d.columns:
        mask = d["prix_nuit"].isna() | (
            (d["prix_nuit"] >= prix_range[0]) & (d["prix_nuit"] <= prix_range[1])
        )
        d = d[mask]
    if chambres_min > 0 and "nb_chambres" in d.columns:
        d = d[d["nb_chambres"].isna() | (d["nb_chambres"] >= chambres_min)]
    if note_min > 0 and "note" in d.columns:
        d = d[d["note"].isna() | (d["note"] >= note_min)]
    if superhost_only and "superhost" in d.columns:
        d = d[d["superhost"] == True]
    return d

df = apply_filters(raw_df)

# ── Header ────────────────────────────────────────────────────────────────────

st.title(f"🏠 {selected_city_name}")
st.caption(f"{source_filter} · {date.today().strftime('%d/%m/%Y')} · {len(df)} annonces affichées")

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Vue marché", "Prix & Revenus", "Occupation", "Données", "Dataset complet"])

# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — VUE MARCHÉ
# ════════════════════════════════════════════════════════════════════════════

with tab1:

    # KPI row 1
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1:
        st.metric("Annonces", summary.get("nb_listings", "—"))
    with k2:
        p = summary.get("prix_moyen")
        st.metric("Prix moy./nuit", f"{p:.0f} €" if p else "—")
    with k3:
        pm = summary.get("prix_median")
        st.metric("Prix médian", f"{pm:.0f} €" if pm else "—")
    with k4:
        t = summary.get("taux_remplissage_moyen")
        st.metric("Taux rempl. moy.", f"{t:.1f} %" if t else "—")
    with k5:
        rv = summary.get("revpar")
        st.metric("RevPAR", f"{rv:.0f} €" if rv else "—", help="prix_moyen × taux_remplissage_90")
    with k6:
        if not df.empty and "superhost" in df.columns:
            pct_sh = df["superhost"].sum() / len(df) * 100
            st.metric("Superhôtes", f"{pct_sh:.0f} %")
        else:
            st.metric("Superhôtes", "—")

    st.markdown("---")

    # Map + source breakdown side by side
    col_map, col_right = st.columns([2, 1])

    with col_map:
        st.subheader("Carte")
        render_map(
            df=df,
            center_lat=city_cfg["center"]["lat"],
            center_lng=city_cfg["center"]["lng"],
            zoom=city_cfg.get("zoom", 13),
            height=480,
        )

    with col_right:
        st.subheader("Répartition par source")
        if not df.empty and "source" in df.columns:
            src_counts = df["source"].value_counts().reset_index()
            src_counts.columns = ["source", "count"]
            fig_pie = px.pie(
                src_counts, values="count", names="source",
                color="source", color_discrete_map=COLORS,
                template="plotly_dark", hole=0.4,
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig_pie, width="stretch")

        st.markdown("---")
        st.subheader("Par commune (top 8)")
        if not df.empty and "neighbourhood" in df.columns:
            comm = (
                df.dropna(subset=["neighbourhood"])
                .groupby("neighbourhood")
                .agg(n=("id", "count"), prix=("prix_nuit", "mean"))
                .sort_values("n", ascending=False)
                .head(8)
                .reset_index()
            )
            comm["prix"] = comm["prix"].round(0)
            fig_comm = px.bar(
                comm, x="n", y="neighbourhood", orientation="h",
                color="prix", color_continuous_scale="RdYlGn",
                labels={"n": "Annonces", "neighbourhood": "", "prix": "Prix moy."},
                template="plotly_dark",
            )
            fig_comm.update_layout(yaxis={"categoryorder": "total ascending"}, margin=dict(l=0))
            st.plotly_chart(fig_comm, width="stretch")

# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — PRIX & REVENUS
# ════════════════════════════════════════════════════════════════════════════

with tab2:

    # Row A: price distribution + superhost premium
    colA, colB = st.columns(2)

    with colA:
        st.subheader("Distribution des prix / nuit")
        price_df = df.dropna(subset=["prix_nuit"]) if "prix_nuit" in df.columns else pd.DataFrame()
        if not price_df.empty:
            fig_hist = px.histogram(
                price_df, x="prix_nuit", color="source",
                nbins=50, barmode="overlay",
                color_discrete_map=COLORS,
                labels={"prix_nuit": "Prix/nuit (€)", "count": "Nb"},
                template="plotly_dark",
            )
            median_p = price_df["prix_nuit"].median()
            fig_hist.add_vline(x=median_p, line_dash="dash", line_color="white",
                               annotation_text=f"Médiane {median_p:.0f}€")
            st.plotly_chart(fig_hist, width="stretch")
        else:
            st.info("Pas de données prix.")

    with colB:
        st.subheader("Superhôte vs Standard")
        if not df.empty and "superhost" in df.columns and "prix_nuit" in df.columns:
            sh_df = df.dropna(subset=["prix_nuit", "superhost"]).copy()
            sh_df["Statut"] = sh_df["superhost"].map({True: "Superhôte", False: "Standard"})
            fig_box = px.box(
                sh_df, x="Statut", y="prix_nuit", color="Statut",
                color_discrete_map={"Superhôte": "#FFD700", "Standard": "#888"},
                labels={"prix_nuit": "Prix/nuit (€)"},
                template="plotly_dark",
            )
            # Show premium
            sh_prix = sh_df[sh_df["superhost"] == True]["prix_nuit"].median()
            std_prix = sh_df[sh_df["superhost"] == False]["prix_nuit"].median()
            if sh_prix and std_prix and std_prix > 0:
                premium = (sh_prix - std_prix) / std_prix * 100
                st.caption(f"Premium superhôte : **+{premium:.0f}%** (médiane {sh_prix:.0f}€ vs {std_prix:.0f}€)")
            st.plotly_chart(fig_box, width="stretch")
        else:
            st.info("Pas assez de données.")

    # Row B: price by commune + price by bedrooms
    colC, colD = st.columns(2)

    with colC:
        st.subheader("Prix moyen par commune")
        if not df.empty and "neighbourhood" in df.columns and "prix_nuit" in df.columns:
            comm_prix = (
                df.dropna(subset=["neighbourhood", "prix_nuit"])
                .groupby("neighbourhood")
                .agg(prix_moy=("prix_nuit", "mean"), n=("id", "count"))
                .query("n >= 2")
                .sort_values("prix_moy", ascending=True)
                .tail(15)
                .reset_index()
            )
            comm_prix["prix_moy"] = comm_prix["prix_moy"].round(0)
            fig_cprix = px.bar(
                comm_prix, x="prix_moy", y="neighbourhood", orientation="h",
                color="prix_moy", color_continuous_scale="RdYlGn",
                text="prix_moy",
                labels={"prix_moy": "Prix moy. (€)", "neighbourhood": ""},
                template="plotly_dark",
            )
            fig_cprix.update_traces(texttemplate="%{text:.0f} €", textposition="outside")
            fig_cprix.update_layout(margin=dict(l=0), showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig_cprix, width="stretch")
        else:
            st.info("Pas assez de données.")

    with colD:
        st.subheader("Prix moyen par nb de chambres")
        if not df.empty and "nb_chambres" in df.columns and "prix_nuit" in df.columns:
            ch_df = (
                df.dropna(subset=["nb_chambres", "prix_nuit"])
                .query("nb_chambres <= 8")
                .groupby("nb_chambres")
                .agg(prix_moy=("prix_nuit", "mean"), n=("id", "count"))
                .reset_index()
            )
            ch_df["label"] = ch_df["nb_chambres"].astype(int).astype(str) + " ch. (" + ch_df["n"].astype(str) + ")"
            fig_ch = px.bar(
                ch_df, x="nb_chambres", y="prix_moy",
                text="prix_moy", color="prix_moy",
                color_continuous_scale="Blues",
                labels={"nb_chambres": "Chambres", "prix_moy": "Prix moy. (€)"},
                template="plotly_dark",
            )
            fig_ch.update_traces(texttemplate="%{text:.0f} €", textposition="outside")
            fig_ch.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_ch, width="stretch")
        else:
            st.info("Pas assez de données.")

    # Row C: équipements premium
    st.subheader("Impact des équipements sur le prix")
    if not df.empty and "amenities" in df.columns and "prix_nuit" in df.columns:
        keywords = ["Piscine", "Jacuzzi", "Sauna", "Parking", "Terrasse", "Jardin",
                    "Lave-linge", "Climatisation", "Cheminée", "Barbecue", "Vue"]
        prix_med_global = df["prix_nuit"].median()
        rows_amen = []
        for kw in keywords:
            has = df[df["amenities"].str.contains(kw, case=False, na=False)]
            hasnt = df[~df["amenities"].str.contains(kw, case=False, na=False) & df["prix_nuit"].notna()]
            if len(has) >= 3 and has["prix_nuit"].notna().sum() >= 3:
                rows_amen.append({
                    "Équipement": kw,
                    "Avec": has["prix_nuit"].median(),
                    "Sans": hasnt["prix_nuit"].median() if len(hasnt) >= 3 else None,
                    "n": len(has),
                })
        if rows_amen:
            amen_df = pd.DataFrame(rows_amen).dropna()
            amen_df["Premium (€)"] = (amen_df["Avec"] - amen_df["Sans"]).round(0)
            amen_df["Premium (%)"] = ((amen_df["Avec"] - amen_df["Sans"]) / amen_df["Sans"] * 100).round(1)
            amen_df = amen_df.sort_values("Premium (%)", ascending=True)
            fig_amen = px.bar(
                amen_df, x="Premium (%)", y="Équipement", orientation="h",
                color="Premium (%)", color_continuous_scale="RdYlGn",
                text="Premium (%)",
                template="plotly_dark",
            )
            fig_amen.update_traces(texttemplate="%{text:.0f}%", textposition="outside")
            fig_amen.update_layout(coloraxis_showscale=False, margin=dict(l=0))
            st.plotly_chart(fig_amen, width="stretch")
        else:
            st.info("Pas assez de données équipements.")
    else:
        st.info("Pas de données équipements.")

    # Row D: scatter prix vs taux + revenu estimé
    colE, colF = st.columns(2)

    with colE:
        st.subheader("Prix vs Taux de remplissage")
        scat = df.dropna(subset=["prix_nuit", "taux_remplissage_90"]).copy() if "taux_remplissage_90" in df.columns else pd.DataFrame()
        if not scat.empty:
            scat["nb_avis"] = scat["nb_avis"].fillna(1) if "nb_avis" in scat.columns else 1
            fig_sc = px.scatter(
                scat, x="taux_remplissage_90", y="prix_nuit",
                color="source", size="nb_avis" if scat["nb_avis"].max() > 0 else None,
                hover_name="titre",
                color_discrete_map=COLORS,
                labels={"taux_remplissage_90": "Taux rempl. (%)", "prix_nuit": "Prix/nuit (€)"},
                template="plotly_dark",
                opacity=0.7,
            )
            st.plotly_chart(fig_sc, width="stretch")
        else:
            st.info("Pas assez de données.")

    with colF:
        st.subheader("Distribution revenu mensuel estimé")
        if not df.empty and "revenu_mensuel_estime" in df.columns:
            rev_df = df.dropna(subset=["revenu_mensuel_estime"])
            rev_df = rev_df[rev_df["revenu_mensuel_estime"] > 0]
            if not rev_df.empty:
                fig_rev = px.histogram(
                    rev_df, x="revenu_mensuel_estime", nbins=40,
                    color="source", color_discrete_map=COLORS,
                    labels={"revenu_mensuel_estime": "Revenu mensuel estimé (€)"},
                    template="plotly_dark",
                )
                med_rev = rev_df["revenu_mensuel_estime"].median()
                fig_rev.add_vline(x=med_rev, line_dash="dash", line_color="white",
                                  annotation_text=f"Médiane {med_rev:.0f}€")
                st.plotly_chart(fig_rev, width="stretch")
            else:
                st.info("Pas assez de données.")
        else:
            st.info("Pas de données revenu.")

# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — OCCUPATION
# ════════════════════════════════════════════════════════════════════════════

with tab3:

    colG, colH = st.columns(2)

    with colG:
        st.subheader("Taux remplissage moyen par commune")
        if not df.empty and "neighbourhood" in df.columns and "taux_remplissage_90" in df.columns:
            occ_comm = (
                df.dropna(subset=["neighbourhood", "taux_remplissage_90"])
                .groupby("neighbourhood")
                .agg(taux=("taux_remplissage_90", "mean"), n=("id", "count"))
                .query("n >= 2")
                .sort_values("taux", ascending=True)
                .tail(15)
                .reset_index()
            )
            fig_occ = px.bar(
                occ_comm, x="taux", y="neighbourhood", orientation="h",
                color="taux", color_continuous_scale="RdYlGn",
                text="taux",
                labels={"taux": "Taux rempl. (%)", "neighbourhood": ""},
                template="plotly_dark",
            )
            fig_occ.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_occ.update_layout(coloraxis_showscale=False, margin=dict(l=0))
            st.plotly_chart(fig_occ, width="stretch")
        else:
            st.info("Pas assez de données.")

    with colH:
        st.subheader("Occupation par nb de chambres")
        if not df.empty and "nb_chambres" in df.columns and "taux_remplissage_90" in df.columns:
            occ_ch = (
                df.dropna(subset=["nb_chambres", "taux_remplissage_90"])
                .query("nb_chambres <= 8")
                .groupby("nb_chambres")
                .agg(taux=("taux_remplissage_90", "mean"), n=("id", "count"))
                .reset_index()
            )
            fig_och = px.bar(
                occ_ch, x="nb_chambres", y="taux", color="taux",
                text="taux", color_continuous_scale="Blues",
                labels={"nb_chambres": "Chambres", "taux": "Taux rempl. (%)"},
                template="plotly_dark",
            )
            fig_och.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_och.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig_och, width="stretch")
        else:
            st.info("Pas assez de données.")

    # Monthly occupancy heatmap
    st.subheader("Saisonnalité — taux d'occupation mensuel")
    if not avail_df.empty:
        avail_df["month"] = pd.to_datetime(avail_df["date"]).dt.to_period("M").astype(str)
        monthly = (
            avail_df.groupby("month")
            .apply(lambda g: (1 - g["is_available"].mean()) * 100)
            .reset_index()
        )
        monthly.columns = ["Mois", "Taux occupation (%)"]
        monthly = monthly.sort_values("Mois")
        fig_month = px.bar(
            monthly, x="Mois", y="Taux occupation (%)",
            color="Taux occupation (%)", color_continuous_scale="RdYlGn",
            text="Taux occupation (%)",
            template="plotly_dark",
        )
        fig_month.update_traces(texttemplate="%{text:.0f}%", textposition="outside")
        fig_month.update_layout(coloraxis_showscale=False, xaxis_tickangle=-45)
        st.plotly_chart(fig_month, width="stretch")

        # By source
        if "source" in avail_df.columns and avail_df["source"].nunique() > 1:
            st.subheader("Saisonnalité par source")
            monthly_src = (
                avail_df.groupby(["month", "source"])
                .apply(lambda g: (1 - g["is_available"].mean()) * 100)
                .reset_index()
            )
            monthly_src.columns = ["Mois", "source", "Taux (%)"]
            fig_ms = px.line(
                monthly_src, x="Mois", y="Taux (%)", color="source",
                color_discrete_map=COLORS, markers=True,
                template="plotly_dark",
            )
            st.plotly_chart(fig_ms, width="stretch")
    else:
        st.info("Pas de données de disponibilité.")

    # Opportunities: high occupancy + below median price
    st.markdown("---")
    st.subheader("💡 Opportunités — logements sous-évalués")
    st.caption("Taux de remplissage élevé (>60%) et prix sous la médiane : candidats à revaloriser.")
    if not df.empty and "taux_remplissage_90" in df.columns and "prix_nuit" in df.columns:
        opp = df.dropna(subset=["prix_nuit", "taux_remplissage_90"]).copy()
        med = opp["prix_nuit"].median()
        opp = opp[(opp["taux_remplissage_90"] > 60) & (opp["prix_nuit"] < med)]
        opp = opp.sort_values("taux_remplissage_90", ascending=False)
        if not opp.empty:
            show_cols = [c for c in ["titre", "source", "neighbourhood", "nb_chambres",
                                     "prix_nuit", "taux_remplissage_90", "note", "url"] if c in opp.columns]
            st.dataframe(
                opp[show_cols].head(20).style.format({
                    "prix_nuit": "{:.0f} €",
                    "taux_remplissage_90": "{:.1f} %",
                    "note": "{:.2f}",
                }),
                width="stretch", hide_index=True,
            )
        else:
            st.success("Aucun logement manifestement sous-évalué détecté.")
    else:
        st.info("Pas assez de données.")

# ════════════════════════════════════════════════════════════════════════════
# TAB 4 — DONNÉES BRUTES
# ════════════════════════════════════════════════════════════════════════════

with tab4:

    st.subheader(f"Top 20 listings — RevPAR estimé")
    if not top_df.empty:
        show = [c for c in ["titre", "source", "neighbourhood", "nb_chambres",
                             "prix_nuit", "taux_remplissage", "revpar_estime", "note", "url"]
                if c in top_df.columns]
        st.dataframe(
            top_df[show].style.format({
                "prix_nuit": "{:.0f} €",
                "taux_remplissage": "{:.1f} %",
                "revpar_estime": "{:.0f} €",
                "note": "{:.2f}",
            }),
            width="stretch", hide_index=True,
        )

    st.markdown("---")
    st.subheader("Toutes les annonces")

    if not df.empty:
        display_cols = [c for c in [
            "titre", "source", "neighbourhood", "type_bien",
            "nb_chambres", "nb_sdb", "nb_voyageurs",
            "prix_nuit", "taux_remplissage_90", "revpar", "revenu_mensuel_estime",
            "note", "nb_avis", "superhost", "instant_book",
            "minimum_nights", "cleaning_fee", "amenities",
            "lat", "lng", "url",
        ] if c in df.columns]

        st.caption(f"{len(df)} annonces — filtres appliqués")
        st.dataframe(
            df[display_cols].style.format({
                "prix_nuit": lambda x: f"{x:.0f} €" if pd.notna(x) else "—",
                "taux_remplissage_90": lambda x: f"{x:.1f} %" if pd.notna(x) else "—",
                "revpar": lambda x: f"{x:.0f} €" if pd.notna(x) else "—",
                "revenu_mensuel_estime": lambda x: f"{x:.0f} €" if pd.notna(x) else "—",
                "note": lambda x: f"{x:.2f}" if pd.notna(x) else "—",
                "lat": lambda x: f"{x:.4f}" if pd.notna(x) else "—",
                "lng": lambda x: f"{x:.4f}" if pd.notna(x) else "—",
            }),
            width="stretch", hide_index=True, height=500,
        )

        # CSV export
        csv = df[display_cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Exporter en CSV",
            data=csv,
            file_name=f"listings_{selected_city_name.replace(' ', '_').lower()}_{date.today()}.csv",
            mime="text/csv",
        )
    else:
        st.info("Aucune annonce pour ces filtres.")

# ════════════════════════════════════════════════════════════════════════════
# TAB 5 — DATASET COMPLET
# ════════════════════════════════════════════════════════════════════════════

with tab5:
    st.subheader("Dataset complet — toutes les colonnes")

    if not df.empty:
        # Ordered columns: identity first, then all others
        priority = [
            "id", "source", "ville", "titre", "type_bien",
            "code_postal", "code_dept", "neighbourhood", "zone_geo", "lat", "lng",
            "nb_voyageurs", "nb_chambres", "nb_lits", "nb_sdb",
            "prix_nuit", "prix_semaine", "prix_weekend", "cleaning_fee", "minimum_nights",
            "note", "nb_avis",
            "note_proprete", "note_precision", "note_arrivee",
            "note_communication", "note_emplacement", "note_qualite_prix",
            "superhost", "instant_book", "photos_count",
            "taux_remplissage_90", "taux_remplissage_365",
            "jours_indispo_90", "jours_indispo_365",
            "revpar", "revenu_mensuel_estime",
            "amenities", "url", "created_at", "last_scanned_at",
        ]
        all_cols = priority + [c for c in df.columns if c not in priority]
        show_cols = [c for c in all_cols if c in df.columns]

        st.caption(f"{len(df)} annonces · {len(show_cols)} colonnes · filtres sidebar appliqués")

        fmt = {
            "prix_nuit":             lambda x: f"{x:.0f} €" if pd.notna(x) else "—",
            "prix_semaine":          lambda x: f"{x:.0f} €" if pd.notna(x) else "—",
            "prix_weekend":          lambda x: f"{x:.0f} €" if pd.notna(x) else "—",
            "cleaning_fee":          lambda x: f"{x:.0f} €" if pd.notna(x) else "—",
            "revpar":                lambda x: f"{x:.0f} €" if pd.notna(x) else "—",
            "revenu_mensuel_estime": lambda x: f"{x:.0f} €" if pd.notna(x) else "—",
            "note":                  lambda x: f"{x:.2f}" if pd.notna(x) else "—",
            "note_proprete":         lambda x: f"{x:.1f}" if pd.notna(x) else "—",
            "note_precision":        lambda x: f"{x:.1f}" if pd.notna(x) else "—",
            "note_arrivee":          lambda x: f"{x:.1f}" if pd.notna(x) else "—",
            "note_communication":    lambda x: f"{x:.1f}" if pd.notna(x) else "—",
            "note_emplacement":      lambda x: f"{x:.1f}" if pd.notna(x) else "—",
            "note_qualite_prix":     lambda x: f"{x:.1f}" if pd.notna(x) else "—",
            "taux_remplissage_90":   lambda x: f"{x:.1f} %" if pd.notna(x) else "—",
            "taux_remplissage_365":  lambda x: f"{x:.1f} %" if pd.notna(x) else "—",
            "lat":                   lambda x: f"{x:.5f}" if pd.notna(x) else "—",
            "lng":                   lambda x: f"{x:.5f}" if pd.notna(x) else "—",
        }
        active_fmt = {k: v for k, v in fmt.items() if k in show_cols}

        st.dataframe(
            df[show_cols].style.format(active_fmt),
            width="stretch", hide_index=True, height=600,
        )

        csv = df[show_cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Exporter tout en CSV",
            data=csv,
            file_name=f"dataset_complet_{selected_city_name.replace(' ', '_').lower()}_{date.today()}.csv",
            mime="text/csv",
        )
    else:
        st.info("Aucune annonce pour ces filtres.")

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption("Rental Market Scanner · Données scrappées à titre informatif")
