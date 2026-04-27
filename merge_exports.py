"""
Fusionne tous les fichiers XLS de départements en un seul fichier France.

Usage:
    python merge_exports.py                        # fusionne exports/*.xlsx → exports/france_complet.xlsx
    python merge_exports.py --dir /chemin/exports  # dossier custom
    python merge_exports.py --out france.xlsx       # nom du fichier de sortie
"""

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
from rich.console import Console

console = Console()


def merge_exports(export_dir: Path, output_path: Path) -> None:
    xlsx_files = sorted(export_dir.glob("scan_*.xlsx"))
    if not xlsx_files:
        console.print(f"[red]Aucun fichier scan_*.xlsx trouvé dans {export_dir}[/red]")
        return

    console.print(f"[cyan]{len(xlsx_files)} fichiers trouvés :[/cyan]")
    for f in xlsx_files:
        console.print(f"  {f.name}")

    all_listings: list[pd.DataFrame] = []
    all_stats: list[pd.DataFrame] = []

    for xlsx_path in xlsx_files:
        try:
            dept_name = xlsx_path.stem.replace("scan_", "").replace("_", " ").title()
            xl = pd.ExcelFile(xlsx_path)

            if "Listings" in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name="Listings")
                if not df.empty:
                    df["Fichier source"] = xlsx_path.name
                    all_listings.append(df)
                    console.print(f"  [green]✓ {xlsx_path.name} — {len(df)} listings[/green]")

            if "Résumé" in xl.sheet_names:
                df_stats = pd.read_excel(xl, sheet_name="Résumé")
                if not df_stats.empty:
                    df_stats.insert(0, "Département", dept_name)
                    all_stats.append(df_stats)
        except Exception as e:
            console.print(f"  [red]✗ {xlsx_path.name} — erreur: {e}[/red]")

    if not all_listings:
        console.print("[red]Aucun listing à fusionner.[/red]")
        return

    df_france = pd.concat(all_listings, ignore_index=True)

    # Dédupliquer sur l'ID si la colonne existe
    if "ID" in df_france.columns:
        before = len(df_france)
        df_france = df_france.drop_duplicates(subset=["ID"], keep="last")
        dupes = before - len(df_france)
        if dupes:
            console.print(f"[yellow]{dupes} doublons supprimés[/yellow]")

    df_stats_france = pd.concat(all_stats, ignore_index=True) if all_stats else pd.DataFrame()

    # Résumé global par source
    summary_cols = {}
    if "Prix/nuit (€)" in df_france.columns:
        summary_cols["Prix/nuit (€)"] = ["mean", "median"]
    if "Taux rempl. 90j (%)" in df_france.columns:
        summary_cols["Taux rempl. 90j (%)"] = "mean"
    if "RevPAR (€)" in df_france.columns:
        summary_cols["RevPAR (€)"] = "mean"

    if summary_cols and "Source" in df_france.columns:
        df_global = df_france.groupby("Source").agg(
            Nb_listings=("ID" if "ID" in df_france.columns else df_france.columns[0], "count"),
            **{
                "Prix moyen (€)": pd.NamedAgg("Prix/nuit (€)", "mean") if "Prix/nuit (€)" in df_france.columns else pd.NamedAgg(df_france.columns[0], "count"),
                "Prix médian (€)": pd.NamedAgg("Prix/nuit (€)", "median") if "Prix/nuit (€)" in df_france.columns else pd.NamedAgg(df_france.columns[0], "count"),
            }
        ).round(2).reset_index()
    else:
        df_global = pd.DataFrame()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_france.to_excel(writer, sheet_name="Tous les listings", index=False)
        if not df_stats_france.empty:
            df_stats_france.to_excel(writer, sheet_name="Résumé par dépt", index=False)
        if not df_global.empty:
            df_global.to_excel(writer, sheet_name="Résumé France", index=False)

        # Auto-adjust column widths
        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col_cells in ws.columns:
                max_len = max((len(str(c.value or "")) for c in col_cells), default=10)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 3, 50)

    console.print(f"\n[bold green]Fusion terminée : {output_path}[/bold green]")
    console.print(f"  {len(df_france)} listings au total ({len(xlsx_files)} départements)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fusionne les exports XLS départementaux en un fichier France")
    parser.add_argument("--dir", default="exports", help="Dossier contenant les scan_*.xlsx (défaut: exports/)")
    parser.add_argument("--out", default=None, help="Fichier de sortie (défaut: exports/france_complet_YYYYMMDD.xlsx)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    export_dir = Path(args.dir)
    if args.out:
        output_path = Path(args.out)
    else:
        stamp = datetime.now().strftime("%Y%m%d")
        output_path = export_dir / f"france_complet_{stamp}.xlsx"

    merge_exports(export_dir, output_path)
