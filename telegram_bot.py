"""
Telegram bot — monitoring du scraping par département.

Prérequis :
    pip install python-telegram-bot>=21.0

Configuration :
    export TELEGRAM_TOKEN="<token BotFather>"
    export TELEGRAM_CHAT_ID="<votre chat_id>"   # optionnel : restreint l'accès

Commandes disponibles :
    /status      — état global de l'orchestrateur (complétés / en cours / echecs)
    /dept XX     — stats rapides d'un département (XX = code ex: 49)
    /running     — liste des workers actifs
    /failed      — liste des échecs
    /logs XX     — dernières 20 lignes du log d'un département
    /summary     — totaux toutes DBs (listings, revpar moyen…)
    /help        — liste des commandes

Usage :
    python telegram_bot.py
"""

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import duckdb
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ── Config ────────────────────────────────────────────────────────────────────

TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
ALLOWED_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")  # "" = pas de restriction

STATE_FILE = Path("orchestrator_state.json")
DATA_DIR = Path("data")
LOG_DIR = Path("logs")

# ── Auth guard ────────────────────────────────────────────────────────────────

def _allowed(update: Update) -> bool:
    if not ALLOWED_CHAT_ID:
        return True
    return str(update.effective_chat.id) == ALLOWED_CHAT_ID


def _deny(update: Update) -> str:
    return f"Chat {update.effective_chat.id} non autorisé."

# ── State helpers ─────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"completed": {}, "failed": {}, "started_at": None}


def _fmt_dur(seconds: float) -> str:
    s = int(seconds)
    return f"{s//3600}h{(s%3600)//60:02d}m" if s >= 3600 else f"{s//60}m{s%60:02d}s"


# ── DB query helper ───────────────────────────────────────────────────────────

def _query_dept_db(code: str, sql: str):
    db_path = DATA_DIR / f"dept_{code}.db"
    if not db_path.exists():
        return None
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return conn.execute(sql).fetchall()
    except Exception:
        return None
    finally:
        conn.close()


# ── Commands ──────────────────────────────────────────────────────────────────

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await update.message.reply_text(_deny(update))
        return
    text = (
        "🤖 *Rental Market Scanner — Commandes*\n\n"
        "/status — état global de l'orchestrateur\n"
        "/running — workers actifs\n"
        "/failed — départements en échec\n"
        "/dept XX — stats d'un département (ex: /dept 49)\n"
        "/logs XX — dernières lignes du log\n"
        "/summary — totaux toutes DBs\n"
        "/help — cette aide"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await update.message.reply_text(_deny(update))
        return
    state = load_state()
    completed = len(state["completed"])
    failed = len(state["failed"])
    started = state.get("started_at", "—")

    lines = [
        "📊 *État de l'orchestrateur*",
        "",
        f"✅ Complétés : *{completed}*",
        f"❌ Échecs    : *{failed}*",
        f"🕒 Démarré   : {started}",
    ]

    # Estimate running by scanning log mtime
    running_codes = []
    for log in LOG_DIR.glob("dept_*.log"):
        code = log.stem.replace("dept_", "")
        if code not in state["completed"] and code not in state["failed"]:
            age = time.time() - log.stat().st_mtime
            if age < 300:  # log modifié il y a < 5 min → considéré actif
                running_codes.append(code)

    if running_codes:
        lines.append(f"⟳ En cours   : *{len(running_codes)}* ({', '.join(running_codes)})")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_running(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await update.message.reply_text(_deny(update))
        return
    state = load_state()
    done = set(state["completed"]) | set(state["failed"])

    active = []
    for log in sorted(LOG_DIR.glob("dept_*.log")):
        code = log.stem.replace("dept_", "")
        if code in done:
            continue
        age = time.time() - log.stat().st_mtime
        if age < 300:
            active.append((code, age))

    if not active:
        await update.message.reply_text("Aucun worker actif détecté.")
        return

    lines = ["⟳ *Workers actifs*\n"]
    for code, age in active:
        lines.append(f"  [{code}] log mis à jour il y a {int(age)}s")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_failed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await update.message.reply_text(_deny(update))
        return
    state = load_state()
    if not state["failed"]:
        await update.message.reply_text("✅ Aucun échec enregistré.")
        return

    lines = ["❌ *Départements en échec*\n"]
    for code, info in state["failed"].items():
        err = info.get("error", "—")[:80]
        lines.append(f"  [{code}] {info['name']} — {err}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_dept(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await update.message.reply_text(_deny(update))
        return

    if not context.args:
        await update.message.reply_text("Usage : /dept XX  (ex: /dept 49)")
        return

    code = context.args[0].zfill(2) if context.args[0].isdigit() else context.args[0]
    db_path = DATA_DIR / f"dept_{code}.db"

    if not db_path.exists():
        await update.message.reply_text(f"Aucune DB trouvée pour le département {code}.")
        return

    rows = _query_dept_db(code, """
        SELECT
            COUNT(*) AS nb,
            ROUND(AVG(prix_nuit), 0) AS avg_prix,
            ROUND(AVG(note), 2) AS avg_note,
            ROUND(AVG(nb_avis), 0) AS avg_avis,
            COUNT(DISTINCT ville) AS nb_zones
        FROM listings
    """)
    if not rows or not rows[0][0]:
        await update.message.reply_text(f"DB {code} vide ou sans données listings.")
        return

    nb, avg_prix, avg_note, avg_avis, nb_zones = rows[0]

    # occupancy
    occ = _query_dept_db(code, """
        SELECT ROUND(
            100.0 * SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0), 1
        )
        FROM availability
        WHERE date >= current_date AND date < current_date + INTERVAL 90 DAYS
    """)
    taux = occ[0][0] if occ and occ[0][0] is not None else "—"

    state = load_state()
    status_icon = "✅" if code in state["completed"] else ("❌" if code in state["failed"] else "⟳")

    text = (
        f"🏘 *Département {code}* {status_icon}\n\n"
        f"📋 Listings       : *{nb}*\n"
        f"🏙 Zones          : {nb_zones}\n"
        f"💶 Prix moyen/nuit: *{avg_prix} €*\n"
        f"⭐ Note moyenne   : {avg_note}\n"
        f"💬 Avis moyens    : {avg_avis}\n"
        f"📅 Taux remplissage 90j : *{taux}%*"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await update.message.reply_text(_deny(update))
        return

    if not context.args:
        await update.message.reply_text("Usage : /logs XX  (ex: /logs 49)")
        return

    code = context.args[0].zfill(2) if context.args[0].isdigit() else context.args[0]
    log_path = LOG_DIR / f"dept_{code}.log"

    if not log_path.exists():
        await update.message.reply_text(f"Aucun log trouvé pour le département {code}.")
        return

    lines = log_path.read_text(errors="replace").splitlines()
    # Strip ANSI codes
    ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    clean = [ansi_escape.sub("", l) for l in lines if l.strip()]
    tail = "\n".join(clean[-20:])

    await update.message.reply_text(
        f"📄 *Log dept {code}* (20 dernières lignes)\n```\n{tail[:3800]}\n```",
        parse_mode="Markdown",
    )


async def cmd_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await update.message.reply_text(_deny(update))
        return

    db_files = sorted(DATA_DIR.glob("dept_*.db"))
    if not db_files:
        await update.message.reply_text("Aucune DB département trouvée dans data/.")
        return

    total_listings = 0
    total_dbs = 0
    prices = []

    for db_path in db_files:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            r = conn.execute("SELECT COUNT(*), AVG(prix_nuit) FROM listings").fetchone()
            if r and r[0]:
                total_listings += r[0]
                total_dbs += 1
                if r[1]:
                    prices.append(r[1])
        except Exception:
            pass
        finally:
            conn.close()

    avg_price = round(sum(prices) / len(prices), 0) if prices else "—"
    state = load_state()

    text = (
        f"🇫🇷 *Résumé global*\n\n"
        f"💾 DBs département : {total_dbs}\n"
        f"🏘 Listings total  : *{total_listings:,}*\n"
        f"💶 Prix moyen/nuit : *{avg_price} €*\n"
        f"✅ Depts complétés : {len(state['completed'])}\n"
        f"❌ Depts en erreur : {len(state['failed'])}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not TOKEN:
        print(
            "TELEGRAM_TOKEN non défini.\n"
            "Obtenez un token via @BotFather sur Telegram, puis :\n"
            "  export TELEGRAM_TOKEN='votre_token'\n"
            "  python telegram_bot.py"
        )
        raise SystemExit(1)

    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("start", cmd_help))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("running", cmd_running))
    app.add_handler(CommandHandler("failed", cmd_failed))
    app.add_handler(CommandHandler("dept", cmd_dept))
    app.add_handler(CommandHandler("logs", cmd_logs))
    app.add_handler(CommandHandler("summary", cmd_summary))

    print("Bot démarré. Ctrl+C pour arrêter.")
    if ALLOWED_CHAT_ID:
        print(f"Accès restreint au chat ID : {ALLOWED_CHAT_ID}")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
