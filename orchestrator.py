"""
Parallel department scan orchestrator.

Usage:
    python orchestrator.py                          # scan all departments
    python orchestrator.py --depts 49 75 13         # specific departments
    python orchestrator.py --workers 3              # parallel workers (default: 3)
    python orchestrator.py --resume                 # skip completed depts
    python orchestrator.py --source airbnb          # one source only
    python orchestrator.py --dry-run                # simulate

Each department gets:
    - its own DB:   data/dept_{code}.db
    - its own log:  logs/dept_{code}.log
    - Excel export: exports/dept_{code}_*.xlsx  (auto via run_scan.py)
"""

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()

STATE_FILE = Path("orchestrator_state.json")
DATA_DIR = Path("data")
LOG_DIR = Path("logs")
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── State helpers ──────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"completed": {}, "failed": {}, "started_at": None}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


# ── Department loader ──────────────────────────────────────────────────────────

def load_departments(filter_codes: list[str] | None = None) -> list[tuple[str, dict]]:
    with open("config/departments.yaml") as f:
        data = yaml.safe_load(f)
    depts = list(data["departments"].items())
    if filter_codes:
        codes = {c.lstrip("0") or "0" for c in filter_codes} | set(filter_codes)
        depts = [(k, v) for k, v in depts if k in codes or k.lstrip("0") in codes]
    return depts


# ── Worker ────────────────────────────────────────────────────────────────────

async def run_department(
    dept_code: str,
    dept_cfg: dict,
    source: str | None,
    dry_run: bool,
    max_pages: int,
    skip_days: int,
    worker_status: dict,
) -> dict:
    """Spawn run_scan.py for one department, stream output to log file."""
    dept_name = dept_cfg["name"]
    db_path = DATA_DIR / f"dept_{dept_code}.db"
    log_path = LOG_DIR / f"dept_{dept_code}.log"

    cmd = [
        sys.executable, "run_scan.py",
        "--city", dept_name,
        "--db-path", str(db_path),
        "--max-pages", str(max_pages),
        "--skip-days", str(skip_days),
    ]
    if source:
        cmd += ["--source", source]
    if dry_run:
        cmd += ["--dry-run"]

    worker_status[dept_code] = {
        "name": dept_name,
        "status": "running",
        "started": time.time(),
        "lines": 0,
        "last_line": "",
    }

    result = {"dept_code": dept_code, "dept_name": dept_name, "success": False, "duration": 0}
    t0 = time.time()

    try:
        with open(log_path, "a") as log_f:
            log_f.write(f"\n\n{'='*60}\n")
            log_f.write(f"Started at {datetime.now().isoformat()}\n")
            log_f.write(f"CMD: {' '.join(cmd)}\n")
            log_f.write(f"{'='*60}\n\n")

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env={
                    **__import__("os").environ,
                    "RENTAL_DB_PATH": str(db_path),
                    "FORCE_COLOR": "0",
                    "NO_COLOR": "1",
                },
            )

            async for raw_line in proc.stdout:
                line = raw_line.decode("utf-8", errors="replace").rstrip()
                log_f.write(line + "\n")
                log_f.flush()
                ws = worker_status[dept_code]
                ws["lines"] += 1
                ws["last_line"] = line[:80]

            await proc.wait()
            result["success"] = proc.returncode == 0
            result["returncode"] = proc.returncode

    except Exception as e:
        result["error"] = str(e)
        with open(log_path, "a") as log_f:
            log_f.write(f"\nORCHESTRATOR ERROR: {e}\n")

    result["duration"] = time.time() - t0
    worker_status[dept_code]["status"] = "done" if result["success"] else "failed"
    worker_status[dept_code]["duration"] = result["duration"]
    return result


# ── Progress display ───────────────────────────────────────────────────────────

def build_table(
    worker_status: dict,
    state: dict,
    total: int,
    start_time: float,
) -> Panel:
    table = Table(show_header=True, header_style="bold cyan", expand=True)
    table.add_column("Dept", style="bold", width=6)
    table.add_column("Nom", min_width=20)
    table.add_column("Statut", width=10)
    table.add_column("Durée", width=8)
    table.add_column("Dernière ligne", ratio=1)

    for code, ws in sorted(worker_status.items()):
        status = ws["status"]
        if status == "running":
            status_txt = Text("⟳ running", style="yellow")
        elif status == "done":
            status_txt = Text("✓ done", style="green")
        else:
            status_txt = Text("✗ failed", style="red")

        dur = ws.get("duration", time.time() - ws["started"])
        dur_txt = f"{int(dur//60)}m{int(dur%60):02d}s"
        table.add_row(
            code,
            ws["name"][:28],
            status_txt,
            dur_txt,
            ws.get("last_line", "")[:60],
        )

    completed = len(state["completed"])
    failed = len(state["failed"])
    running = sum(1 for ws in worker_status.values() if ws["status"] == "running")
    elapsed = time.time() - start_time

    title = (
        f"[bold]Orchestrateur départements[/bold]  "
        f"[green]{completed}✓[/green]  [red]{failed}✗[/red]  [yellow]{running}⟳[/yellow]  "
        f"/ {total}  —  {int(elapsed//60)}m{int(elapsed%60):02d}s"
    )
    return Panel(table, title=title, border_style="blue")


# ── Main orchestration ────────────────────────────────────────────────────────

async def orchestrate(args: argparse.Namespace) -> None:
    depts = load_departments(args.depts if args.depts else None)
    if not depts:
        console.print("[red]Aucun département trouvé avec les filtres donnés.[/red]")
        sys.exit(1)

    state = load_state()
    if not state["started_at"]:
        state["started_at"] = datetime.now().isoformat()

    if args.resume:
        original_count = len(depts)
        depts = [(k, v) for k, v in depts if k not in state["completed"]]
        console.print(
            f"[cyan]RESUME : {original_count - len(depts)} déjà complétés, "
            f"{len(depts)} restants.[/cyan]"
        )

    if not depts:
        console.print("[green]Tous les départements sont déjà complétés.[/green]")
        return

    console.print(
        f"[bold]Démarrage : {len(depts)} département(s), "
        f"{args.workers} workers parallèles[/bold]"
    )

    semaphore = asyncio.Semaphore(args.workers)
    worker_status: dict = {}
    results: list[dict] = []
    start_time = time.time()

    async def throttled_worker(dept_code: str, dept_cfg: dict) -> None:
        async with semaphore:
            r = await run_department(
                dept_code, dept_cfg,
                source=args.source,
                dry_run=args.dry_run,
                max_pages=args.max_pages,
                skip_days=args.skip_days,
                worker_status=worker_status,
            )
            results.append(r)
            if r["success"]:
                state["completed"][dept_code] = {
                    "name": dept_cfg["name"],
                    "finished_at": datetime.now().isoformat(),
                    "duration_s": round(r["duration"]),
                }
            else:
                state["failed"][dept_code] = {
                    "name": dept_cfg["name"],
                    "finished_at": datetime.now().isoformat(),
                    "error": r.get("error", f"returncode={r.get('returncode')}"),
                }
            save_state(state)

    tasks = [throttled_worker(k, v) for k, v in depts]

    with Live(
        build_table(worker_status, state, len(depts), start_time),
        refresh_per_second=2,
        console=console,
    ) as live:
        async def refresh_loop():
            while True:
                live.update(build_table(worker_status, state, len(depts), start_time))
                await asyncio.sleep(0.5)

        refresh_task = asyncio.create_task(refresh_loop())
        await asyncio.gather(*tasks)
        refresh_task.cancel()
        live.update(build_table(worker_status, state, len(depts), start_time))

    # ── Final report ──────────────────────────────────────────────────────────
    total_dur = time.time() - start_time
    console.print()
    console.print(
        Panel(
            f"[green]✓ Succès : {len(state['completed'])}[/green]\n"
            f"[red]✗ Échecs : {len(state['failed'])}[/red]\n"
            f"Durée totale : {int(total_dur//60)}m{int(total_dur%60):02d}s",
            title="[bold]Bilan final[/bold]",
            expand=False,
        )
    )
    if state["failed"]:
        console.print("[red]Départements en erreur :[/red]")
        for code, info in state["failed"].items():
            console.print(f"  [{code}] {info['name']} — {info.get('error', '')}")
        console.print("\nRelancer avec [bold]--resume[/bold] pour retenter les échecs.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrateur parallèle — scan par département",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python orchestrator.py                        # tous les depts, 3 workers
  python orchestrator.py --depts 49 44 72       # depts spécifiques
  python orchestrator.py --workers 5 --resume   # 5 workers, reprend les echecs
  python orchestrator.py --source airbnb        # une seule source
        """,
    )
    parser.add_argument(
        "--depts", nargs="+", metavar="CODE",
        help="Codes département (ex: 49 75 13). Défaut: tous.",
    )
    parser.add_argument(
        "--workers", type=int, default=3,
        help="Nombre de workers parallèles (défaut: 3)",
    )
    parser.add_argument(
        "--source", choices=["airbnb", "booking", "vrbo"], default=None,
        help="Limiter à une source (défaut: toutes)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=15,
        help="Pages max par ville (défaut: 15)",
    )
    parser.add_argument(
        "--skip-days", type=int, default=7,
        help="Ne re-scanner qu'après N jours (défaut: 7)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simuler sans sauvegarder",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Reprendre en ignorant les départements déjà complétés",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Réinitialiser l'état (orchestrator_state.json)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.reset:
        STATE_FILE.unlink(missing_ok=True)
        console.print("[yellow]État réinitialisé.[/yellow]")

    asyncio.run(orchestrate(args))
