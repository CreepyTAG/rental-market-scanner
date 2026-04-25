#!/usr/bin/env bash
# install.sh — Setup rapide sur VPS Ubuntu/Debian
# Usage : bash install.sh

set -e
cd "$(dirname "$0")"

echo "=== Rental Market Scanner — Installation VPS ==="

# ── Dépendances système ───────────────────────────────────────────────────────
echo "[1/5] Dépendances système..."
apt-get update -qq && apt-get install -y -qq \
    python3 python3-pip python3-venv \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxfixes3 libxrandr2 libgbm1 libasound2 \
    unzip curl git

# ── Virtualenv ────────────────────────────────────────────────────────────────
echo "[2/5] Virtualenv Python..."
python3 -m venv .venv
source .venv/bin/activate

# ── Pip ───────────────────────────────────────────────────────────────────────
echo "[3/5] Dépendances Python..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

# ── Playwright ────────────────────────────────────────────────────────────────
echo "[4/5] Playwright Chromium..."
playwright install chromium
playwright install-deps chromium

# ── Dossiers ─────────────────────────────────────────────────────────────────
echo "[5/5] Création des dossiers..."
mkdir -p logs data exports checkpoints

# ── .env ─────────────────────────────────────────────────────────────────────
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo ">>> Fichier .env créé. Remplis-le avant de lancer :"
    echo "    nano .env"
else
    echo ".env déjà présent."
fi

echo ""
echo "=== Installation terminée ==="
echo ""
echo "Prochaines étapes :"
echo "  1. nano .env                    # remplir TELEGRAM_TOKEN et TELEGRAM_CHAT_ID"
echo "  2. source .venv/bin/activate"
echo "  3. set -a && source .env && set +a"
echo ""
echo "  # Test rapide (dry-run)"
echo "  python run_scan.py --city 'Maine-et-Loire' --source airbnb --dry-run"
echo ""
echo "  # Bot Telegram"
echo "  nohup python telegram_bot.py >> logs/telegram.log 2>&1 &"
echo ""
echo "  # Orchestrateur (3 workers, toute la France)"
echo "  nohup python orchestrator.py --workers 3 --source airbnb >> logs/orchestrator.log 2>&1 &"
