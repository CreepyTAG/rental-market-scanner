# LogReadme — Rapport de tests & corrections

**Date** : 2026-04-15
**Cible** : Saint-Barthélemy-d'Anjou (Airbnb)

---

## Phase 1 — Bugs corrigés

### BUG-1 : `random_delay` exécuté en mode dry-run (CRITIQUE)
**Symptôme** : Le dry-run prenait ~4 minutes au lieu de ~15 secondes. Chaque listing (18 par page x 5 pages) attendait 2-4s entre chaque passage, même sans scraper la page de détail.
**Cause** : `await random_delay(2, 4)` était appelé inconditionnellement dans la boucle par listing.
**Correction** : Conditionné à `if not should_skip_detail` — la delay ne s'applique que quand on scrape réellement une page listing.
**Fichier** : `scrapers/airbnb.py`

### BUG-2 : Pas de déduplication cross-pages (CRITIQUE)
**Symptôme** : Pour un petit périmètre comme Saint-Barthélemy-d'Anjou, Airbnb renvoie les mêmes 18 listings sur chaque page de recherche (offset 0, 20, 40, 60, 80). Résultat : 90 entrées traitées au lieu de 18.
**Cause** : La dédup `seen.add(m[1])` dans `_extract_listings_from_page` ne fonctionnait qu'intra-page.
**Correction** : Ajout d'un `set seen_ids` au niveau du scraper principal. Si tous les listings d'une page sont des doublons, la pagination s'arrête.
**Fichier** : `scrapers/airbnb.py`

### BUG-3 : Dry-run pagine inutilement (MOYEN)
**Symptôme** : En mode `--dry-run`, le scraper parcourait 5 pages de recherche alors qu'une seule suffit.
**Correction** : `max_pages = 1` forcé en dry-run.
**Fichier** : `scrapers/airbnb.py`

### BUG-4 : Extraction de titre capturait les badges Airbnb (MOYEN)
**Symptôme** : Les titres étaient "Superhôte", "Coup de coeur voyageurs" au lieu du vrai titre.
**Correction** : Ajout d'une liste `BADGE_WORDS` pour exclure ces lignes.
**Fichier** : `scrapers/airbnb.py`

### BUG-5 : `datetime.utcnow()` deprecated (MINEUR)
**Correction** : Remplacé par `datetime.now(UTC)`.
**Fichier** : `scrapers/airbnb.py`

### BUG-6 : Nom de fichier Excel avec caractères accentués (MINEUR)
**Correction** : Normalisation unicode NFKD + encodage ASCII pour le slug.
**Fichier** : `export/excel.py`

---

## Phase 2 — Améliorations implémentées

### AMELO-1 : Nettoyage des URLs
**Avant** : `https://www.airbnb.fr/rooms/12345?search_mode=regular_search&adults=1&check_in=...`
**Après** : `https://www.airbnb.fr/rooms/12345`
L'URL avec params est conservée en interne (`url_nav`) pour la navigation, l'URL propre est stockée en DB.
**Fichier** : `scrapers/airbnb.py`

### AMELO-2 : Taux de remplissage 365 jours + saisonnalité mensuelle
- `jours_indispo_365` et `taux_remplissage_365` ajoutés dans la DB et l'Excel
- Nouvel onglet **Saisonnalité** dans l'Excel : taux d'occupation par mois (Janvier-Décembre) par listing
- Fonction `_monthly_occupancy()` pour calculer le taux par mois
**Fichiers** : `scrapers/airbnb.py`, `db/storage.py`, `export/excel.py`

### AMELO-3 : Scraping du type de bien et capacité
Nouvelles colonnes extraites depuis la page listing :
- `type_bien` : appartement, maison, tiny house, etc.
- `nb_voyageurs` : capacité d'accueil
- `nb_chambres` : nombre de chambres
- `nb_lits` : nombre de lits
- `nb_sdb` : nombre de salles de bain
Extraction via regex sur le texte de la page (pattern "X voyageurs · Y chambres · Z lits · W salle de bain").
**Fichiers** : `scrapers/airbnb.py`, `db/storage.py`

### AMELO-4 : Anti-ban (user-agent rotation + détection captcha)
- **Rotation de User-Agent** : pool de 6 UAs réalistes, rotation à chaque nouvelle page
- **Détection de blocage** : `detect_block()` cherche les mots-clés captcha/vérification dans la page. Si détecté, pause de 60s puis retry
- **Délais adaptatifs** : `adaptive_delay()` augmente progressivement les pauses (2-4s pour les premières requêtes, jusqu'à 15-30s après 30+ requêtes)
**Fichiers** : `browser/brave.py`, `scrapers/airbnb.py`

### AMELO-5 : Estimation du revenu mensuel
Nouvelle colonne calculée dans la requête SQL :
`revenu_mensuel_estime = prix_nuit * jours_indispo_365 / 12`
Visible dans l'onglet "Listings" de l'Excel.
**Fichier** : `db/storage.py`

---

## Fonctionnalités testées et validées

| Fonctionnalité | Statut | Détail |
|---|---|---|
| Scan dry-run | OK | 18 listings, ~12s, 1 page uniquement |
| Scan réel (1 page) | OK | 18 listings avec calendrier 365j, prix, coords, type de bien |
| Déduplication cross-pages | OK | Stop si 100% doublons |
| Export Excel (.xlsx) | OK | 3 onglets (Listings, Saisonnalité, Résumé) |
| Zone géographique | OK | 8 zones (Centre, Nord-Est, Sud-Ouest, etc.) |
| Type de bien + capacité | OK | Extraction depuis texte page listing |
| Taux remplissage 90j + 365j | OK | Deux métriques distinctes dans l'Excel |
| Saisonnalité mensuelle | OK | Onglet dédié avec taux par mois |
| Skip rescan (< 7j) | OK | 18/18 skippés au 2e scan |
| Checkpoint + Resume | OK | JSON dans `checkpoints/`, supprimé à la fin |
| Migration DB | OK | Ajout auto de 7 nouvelles colonnes |
| Anti-ban | OK | UA rotation + detect_block + adaptive_delay |
| URLs propres | OK | `/rooms/{id}` sans query params |
| Revenu mensuel estimé | OK | Colonne calculée dans l'Excel |

---

## Propositions restantes (par ordre d'importance)

### 1. Support multi-villes en parallèle (MOYENNE)
`--all` scanne les villes séquentiellement. Un scan concurrent (pool de pages limité) réduirait le temps total pour les grandes zones.

### 2. Historisation et comparaison temporelle (MOYENNE)
Comparer les données entre deux scans : variation de prix, taux de remplissage, nouveaux/disparus. Nécessite un système de snapshots datés et un diff.

### 3. Proxy rotatif configurable (MOYENNE)
Pour les gros volumes (>100 listings), ajouter le support de proxies HTTP rotatifs via config YAML. Réduirait fortement le risque de ban.

### 4. Dashboard web Streamlit (BASSE)
Le module `dashboard/` existe mais n'est pas connecté aux nouvelles données. Le mettre à jour : carte Folium avec zones géo, stats par zone, filtres interactifs.

### 5. Alertes sur nouveaux listings (BASSE)
Notification quand un nouveau listing apparaît dans la zone (nouveau = pas en DB). Utile pour du monitoring continu du marché.
