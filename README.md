# Rental Market Scanner

Analyse du marché locatif court terme (Airbnb) — extensible à toute la France.

Scraping en deux phases via Playwright + Brave/Chromium, stockage DuckDB, dashboard Streamlit, export Excel automatique.

---

## Données collectées par annonce

### Table `listings` — fiche complète

| Champ | Type | Description | Source |
|---|---|---|---|
| `id` | string | Identifiant interne (`airbnb_XXXXX`) | — |
| `source` | string | Plateforme (`airbnb`, `booking`, `vrbo`) | — |
| `ville` | string | Ville / zone configurée | config |
| **Identité** ||||
| `titre` | string | Titre de l'annonce | DOM `<h1>` |
| `type_bien` | string | Appartement, maison, studio… | body text |
| `superhost` | bool | Hôte certifié Superhôte | body text |
| `instant_book` | bool | Réservation instantanée | body text |
| `url` | string | URL de l'annonce | — |
| `photos_count` | int | Nombre de photos | DOM |
| **Capacité** ||||
| `nb_voyageurs` | int | Capacité max en voyageurs | body text |
| `nb_chambres` | int | Nombre de chambres | body text |
| `nb_lits` | int | Nombre de lits | body text |
| `nb_sdb` | int | Nombre de salles de bain | body text |
| `minimum_nights` | int | Durée de séjour minimum | body text |
| **Localisation** ||||
| `lat` / `lng` | float | Coordonnées GPS | JSON-LD `geo` / `__NEXT_DATA__` / géocodage commune |
| `neighbourhood` | string | Commune / quartier | JSON-LD `addressLocality` |
| `code_postal` | string | Code postal | JSON-LD / `__NEXT_DATA__` |
| `zone_geo` | string | Zone calculée (Nord, Sud-Est…) | calcul bbox |
| **Tarifs** ||||
| `prix_nuit` | float | Prix par nuit (€) | API `StaysPdpSections` / body text / DOM |
| `prix_semaine` | float | Prix moyen lun–jeu (€) | calendrier API (jours avec prix) |
| `prix_weekend` | float | Prix moyen ven–dim (€) | calendrier API (jours avec prix) |
| `cleaning_fee` | float | Frais de ménage (€) | API `StaysPdpSections` |
| **Qualité** ||||
| `note` | float | Note globale (ex : 4.87) | JSON-LD `aggregateRating` |
| `nb_avis` | int | Nombre d'avis | JSON-LD `reviewCount` |
| **Équipements** ||||
| `amenities` | string | Liste des équipements (CSV) | DOM `[data-section-id="AMENITIES"]` + scan keywords |
| **Timestamps** ||||
| `created_at` | timestamp | Premier scan | — |
| `updated_at` | timestamp | Dernière modification | — |
| `last_scanned_at` | timestamp | Dernier passage phase 2 | — |

**Équipements détectés** : Cuisine, Four, Micro-ondes, Réfrigérateur, Lave-vaisselle, Cafetière, Grille-pain, Lave-linge, Sèche-linge, Climatisation, Chauffage, Wifi, TV, Parking, Piscine, Jacuzzi, Terrasse, Jardin, Balcon, Barbecue, Cheminée, Sauna, Animaux acceptés, Bureau, Lit bébé, Alarme incendie…

### Table `availability` — calendrier jour par jour

| Champ | Type | Description |
|---|---|---|
| `listing_id` | string | Référence vers `listings.id` |
| `date` | date | Date (365 jours depuis aujourd'hui) |
| `is_available` | bool | Disponible (`true`) ou réservé/bloqué (`false`) |
| `scraped_at` | timestamp | Date du scraping |

### Table `price_snapshots` — historique des prix

| Champ | Type | Description |
|---|---|---|
| `listing_id` | string | Référence vers `listings.id` |
| `prix_nuit` | float | Prix constaté lors du scan |
| `scraped_at` | timestamp | Date du scan |

### Table `scan_log` — journal des scans

| Champ | Type | Description |
|---|---|---|
| `ville` | string | Zone scannée |
| `source` | string | Plateforme |
| `started_at` / `ended_at` | timestamp | Durée du scan |
| `status` | string | `success` / `error` |
| `nb_listings` | int | Listings trouvés |
| `nb_inserted` / `nb_updated` / `nb_errors` | int | Détail des opérations DB |

### Métriques calculées (à la lecture)

| Métrique | Formule |
|---|---|
| `taux_remplissage_90` | `jours_réservés_90j / 90 × 100` |
| `taux_remplissage_365` | `jours_réservés_365j / 365 × 100` |
| `revpar` | `prix_nuit × taux_remplissage_90 / 100` |
| `revenu_mensuel_estime` | `prix_nuit × jours_réservés_365j / 12` |

> Le taux de remplissage est estimé depuis le calendrier public Airbnb : les jours indisponibles sont présumés réservés.

---

## Architecture — deux phases

```
Phase 1 — Collecte des IDs (rapide, DOM uniquement)
  bbox → tuiles (si tile_lat/tile_lng configurés)
    └── chaque tuile → N pages de résultats → extraction des IDs
  → déduplication globale → liste unique d'IDs à scraper

Phase 2 — Scraping des annonces (détaillé)
  pour chaque ID :
    ├── DOM : titre, capacité, équipements, superhost, instant_book
    ├── JSON-LD : coords, adresse, note, nb_avis
    ├── __NEXT_DATA__ : coords (fallback si JSON-LD vide)
    ├── API PdpAvailabilityCalendar : disponibilité 365j + prix journaliers
    └── API StaysPdpSections : prix/nuit, frais ménage, coords
```

---

## Installation

### macOS / Linux local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

Brave Browser est utilisé en priorité s'il est installé. Sinon Chromium est utilisé automatiquement.

### VPS Ubuntu/Debian

```bash
# Dépendances système pour Playwright headless
apt-get update && apt-get install -y \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxfixes3 libxrandr2 libgbm1 libasound2

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
playwright install-deps chromium   # installe les libs manquantes auto

# Lancement headless (par défaut)
python run_scan.py --all --source airbnb
```

Brave n'est pas nécessaire sur VPS — Playwright utilise son Chromium bundled.

### Automatisation sur VPS (cron)

```bash
# Scan quotidien à 2h du matin
0 2 * * * cd /opt/rental-market-scanner && .venv/bin/python run_scan.py --all --source airbnb >> logs/scan.log 2>&1

# Ou via le scheduler intégré
python scheduler.py
```

---

## Lancement

```bash
# Une ville
python run_scan.py --city "Angers Agglomération" --source airbnb

# Toutes les villes configurées
python run_scan.py --all --source airbnb

# Options
--max-pages 15       # pages de résultats par tuile (défaut: 15)
--skip-days 7        # re-scanner après N jours (défaut: 7, 0 = toujours)
--dry-run            # simule sans sauvegarder
--resume             # reprend un scan interrompu (checkpoint)
--no-headless        # affiche le navigateur (debug)
```

---

## Dashboard

```bash
streamlit run dashboard/app.py
# → http://localhost:8501
```

Carte interactive, heatmaps prix/occupation, scatter prix vs taux, top listings RevPAR.

---

## Villes configurées — `config/cities.yaml`

22 zones préconfigurées :

| Zone | Type | Tuiles |
|---|---|---|
| **Angers Agglomération** | Vue globale ALM | 16 tuiles |
| Angers Centre, Avrillé, Beaucouzé, Bouchemaine | Communes ALM | — |
| Cantenay-Épinard, Écouflant, Les Ponts-de-Cé | Communes ALM | — |
| Mûrs-Erigné, Saint-Barthélemy-d'Anjou | Communes ALM | — |
| Sainte-Gemmes-sur-Loire, Trélazé, Verrières-en-Anjou | Communes ALM | — |
| **Maine-et-Loire** | Département entier | ~90 tuiles |
| Saumur, Saumur Agglo | Saumur + agglo | — / 12 tuiles |
| Cholet, Cholet Agglo | Cholet + agglo | — / 12 tuiles |
| Loire-Layon, Nord Anjou, Baugeois | Zones rurales | 8 tuiles chacune |
| Puerto Iguazú | Test international | — |

---

## Ajouter une ville

```yaml
# config/cities.yaml
ma_ville:
  name: "Ma Ville"
  center:
    lat: 47.xxxx
    lng: -0.xxxx
  bbox:
    south: 47.xx
    west: -0.xx
    north: 47.xx
    east: -0.xx
  zoom: 13
  tile_lat: 0.06   # optionnel : tiling pour grandes zones (~6.7 km/tuile)
  tile_lng: 0.08   # optionnel : (~5.8 km/tuile)
```

---

## Anti-ban

- Délais adaptatifs aléatoires : 2–4s (<10 req), 3–6s (<30), 5–9s (<60), 8–14s (60+)
- Rotation de User-Agent (pool de 6 UA : Chrome, Firefox, Safari, Linux/Mac/Windows)
- Détection captcha/blocage avec log automatique
- `--no-sandbox` + `--disable-dev-shm-usage` pour stabilité VPS

---

## Checkpoint / Resume

```bash
python run_scan.py --city "Angers Agglomération" --resume
```

Sauvegarde les IDs collectés (phase 1) et traités (phase 2) dans `checkpoints/<city_key>.json`. Reprend exactement là où le scan s'est arrêté.
