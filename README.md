# Rental Market Scanner

Analyse du marché locatif court terme (Airbnb, Booking.com, VRBO) à l'échelle nationale.
Scraping via Playwright + Brave, stockage DuckDB, dashboard Streamlit.

---

## Prérequis

- Python 3.11+
- [Brave Browser](https://brave.com/) installé
- Être déjà connecté à Airbnb et Booking.com dans Brave

---

## Installation

```bash
cd rental-market-scanner

python -m venv .venv
source .venv/bin/activate          # macOS / Linux
# .venv\Scripts\activate           # Windows

pip install -r requirements.txt
playwright install chromium
```

---

## Lancement rapide

### 1. Scan manuel d'un département

```bash
# Scanner la Vendée (Airbnb uniquement)
python run_scan.py --city "Vendée" --db-path vendee.db --source airbnb

# Reprendre un scan interrompu
python run_scan.py --city "Vendée" --db-path vendee.db --source airbnb --resume

# Mode dry-run : voir ce qui serait scrapé sans sauvegarder
python run_scan.py --city "Vendée" --dry-run
```

> Les données sont sauvegardées **toutes les 20 annonces** pendant le scan.
> Si le process est tué, les données déjà récupérées sont conservées.

### 2. Scan parallèle de plusieurs départements (orchestrateur)

```bash
# Tous les départements, 3 workers parallèles
python orchestrator.py

# Départements spécifiques
python orchestrator.py --depts 85 44 22 29 35 56

# Reprendre après une interruption
python orchestrator.py --resume

# 5 workers, Airbnb uniquement
python orchestrator.py --workers 5 --source airbnb
```

Chaque département génère :
- une DB : `data/dept_{code}.db`
- un export Excel : `exports/scan_{departement}.xlsx`

### 3. Fusionner les exports départementaux en un fichier France

```bash
python merge_exports.py
# → exports/france_complet_YYYYMMDD.xlsx
```

Le fichier contient 3 onglets :
- **Tous les listings** — toutes les annonces fusionnées et dédupliquées
- **Résumé par dépt** — stats par département
- **Résumé France** — vue globale par source

### 4. Dashboard Streamlit

```bash
streamlit run dashboard/app.py
```

Ouvre `http://localhost:8501`.

---

## Workflow VPS recommandé

```bash
# Connexion
ssh user@ton-vps

# Lancer dans tmux pour survivre à la déconnexion
tmux new -s scan
cd ~/rental-market-scanner && git pull

# Scan d'un département
.venv/bin/python run_scan.py --city "Vendée" --db-path vendee.db --source airbnb

# Détacher tmux : Ctrl+B puis D
# Revenir plus tard : tmux attach -t scan
```

Récupérer les exports sur ton Mac :
```bash
scp user@ton-vps:~/rental-market-scanner/exports/scan_vendee.xlsx exports/
python merge_exports.py
```

---

## Structure des données

### Base DuckDB

| Table             | Description                                       |
|-------------------|---------------------------------------------------|
| `listings`        | Fiche de chaque logement (prix, note, coords…)   |
| `availability`    | Disponibilité jour par jour sur 365 jours         |
| `price_snapshots` | Historique des prix (une entrée par scan)         |
| `scan_log`        | Journal de chaque scan (statut, durée, nb annonces) |

### Export Excel (par département)

| Onglet          | Contenu                                     |
|-----------------|---------------------------------------------|
| Listings        | Toutes les annonces avec métriques calculées |
| Saisonnalité    | Taux d'occupation mensuel par listing        |
| Résumé          | Stats agrégées par source                    |

---

## Métriques calculées

| Métrique              | Formule                                              |
|-----------------------|------------------------------------------------------|
| Taux de remplissage   | `jours_indisponibles / 90 × 100`                    |
| RevPAR estimé         | `prix_moyen × taux_remplissage / 100`               |
| Revenu mensuel estimé | `prix_nuit × jours_indispo_365 / 12`                |
| Prix semaine          | Moyenne lundi–jeudi depuis le calendrier            |
| Prix week-end         | Moyenne vendredi–dimanche depuis le calendrier       |

> Le taux de remplissage est estimé à partir des jours bloqués dans le calendrier
> public Airbnb (jours non disponibles = présumés réservés).

---

## Départements configurés (96)

| Code | Département | Code | Département | Code | Département |
|------|-------------|------|-------------|------|-------------|
| 01 | Ain | 34 | Hérault | 67 | Bas-Rhin |
| 02 | Aisne | 35 | Ille-et-Vilaine | 68 | Haut-Rhin |
| 03 | Allier | 36 | Indre | 69 | Rhône |
| 04 | Alpes-de-Haute-Provence | 37 | Indre-et-Loire | 70 | Haute-Saône |
| 05 | Hautes-Alpes | 38 | Isère | 71 | Saône-et-Loire |
| 06 | Alpes-Maritimes | 39 | Jura | 72 | Sarthe |
| 07 | Ardèche | 40 | Landes | 73 | Savoie |
| 08 | Ardennes | 41 | Loir-et-Cher | 74 | Haute-Savoie |
| 09 | Ariège | 42 | Loire | 75 | Paris |
| 10 | Aube | 43 | Haute-Loire | 76 | Seine-Maritime |
| 11 | Aude | 44 | Loire-Atlantique | 77 | Seine-et-Marne |
| 12 | Aveyron | 45 | Loiret | 78 | Yvelines |
| 13 | Bouches-du-Rhône | 46 | Lot | 79 | Deux-Sèvres |
| 14 | Calvados | 47 | Lot-et-Garonne | 80 | Somme |
| 15 | Cantal | 48 | Lozère | 81 | Tarn |
| 16 | Charente | 49 | Maine-et-Loire | 82 | Tarn-et-Garonne |
| 17 | Charente-Maritime | 50 | Manche | 83 | Var |
| 18 | Cher | 51 | Marne | 84 | Vaucluse |
| 19 | Corrèze | 52 | Haute-Marne | 85 | Vendée |
| 2A | Corse-du-Sud | 53 | Mayenne | 86 | Vienne |
| 2B | Haute-Corse | 54 | Meurthe-et-Moselle | 87 | Haute-Vienne |
| 21 | Côte-d'Or | 55 | Meuse | 88 | Vosges |
| 22 | Côtes-d'Armor | 56 | Morbihan | 89 | Yonne |
| 23 | Creuse | 57 | Moselle | 90 | Territoire de Belfort |
| 24 | Dordogne | 58 | Nièvre | 91 | Essonne |
| 25 | Doubs | 59 | Nord | 92 | Hauts-de-Seine |
| 26 | Drôme | 60 | Oise | 93 | Seine-Saint-Denis |
| 27 | Eure | 61 | Orne | 94 | Val-de-Marne |
| 28 | Eure-et-Loir | 62 | Pas-de-Calais | 95 | Val-d'Oise |
| 29 | Finistère | 63 | Puy-de-Dôme | | |
| 30 | Gard | 64 | Pyrénées-Atlantiques | | |
| 31 | Haute-Garonne | 65 | Hautes-Pyrénées | | |
| 32 | Gers | 66 | Pyrénées-Orientales | | |
| 33 | Gironde | | | | |

---

## Ajouter une ville personnalisée

Editer `config/cities.yaml` :

```yaml
cities:
  ma_ville:
    name: "Ma Ville"
    center:
      lat: 47.xxxx
      lng: -0.xxxx
    bbox:
      south: 47.xx
      west:  -0.xx
      north: 47.xx
      east:  -0.xx
    zoom: 13
    tile_lat: 0.06  # optionnel — découpe en tuiles pour les grandes zones
    tile_lng: 0.08
```

---

## Notes techniques

- Scrapers 100% async (`asyncio` + Playwright)
- Sauvegarde progressive toutes les 20 annonces (résistant aux crashes)
- Checkpoint par ville : reprise possible avec `--resume`
- Délais aléatoires 2–5 s entre requêtes pour éviter le rate-limiting
- Cache JSON brut dans `raw_cache/` (airbnb / booking / vrbo)
- Mode `--dry-run` sans aucune écriture en base ni cache
