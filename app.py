import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import streamlit as st

st.set_page_config(page_title="WEX HVAC Pricebook → Upload Tool", layout="wide")

REPO_ROOT = Path(__file__).resolve().parent
TOOLS_DIR = REPO_ROOT / "tools"
RULES_DIR = REPO_ROOT / "rules"

# v2 scripts
CONVERTER = TOOLS_DIR / "convert_pricebook_to_wex_skeleton_profiles_v2.py"
ENRICHER = TOOLS_DIR / "universal_ingestion_tool_with_feedback_v2.py"

TEMPLATES = {
    "WEX Single Part": "wex_single_part",
    "WEX Supplier Loader (JUN2024)": "wex_supplier_loader",
    "WEX Bundle (1 part + 1 labor)": "wex_bundle_1part_1labor",
}

BRANDS = ["Bryant", "Carrier", "Day & Night", "Mitsubishi", "Other"]

st.title("WEX HVAC Pricebook → Upload Tool")
st.caption("Upload distributor files + choose template → get a WEX-ready spreadsheet. No API keys. Just scraping, rules, and coping.")


def run_cmd(cmd, cwd):
    """Run a subprocess and stream logs to UI."""
    st.code(" ".join(cmd), language="bash")
    p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out_lines = []
    placeholder = st.empty()
    for line in p.stdout:
        out_lines.append(line)
        placeholder.text("".join(out_lines[-120:]))
    rc = p.wait()
    if rc != 0:
        raise RuntimeError(f"Command failed with exit code {rc}")
    return "".join(out_lines)


def write_uploaded_or_default(upload, default_path: Path | None, out_path: Path):
    """
    Write upload bytes if provided, else copy default file if it is a real file,
    else create a safe placeholder file.
    """
    if upload is not None:
        out_path.write_bytes(upload.getbuffer())
        return

    if default_path is not None and default_path.exists() and default_path.is_file():
        shutil.copy(default_path, out_path)
        return

    # Safe stub so downstream code doesn't explode
    suf = out_path.suffix.lower()
    if suf == ".json":
        out_path.write_text("[]", encoding="utf-8")
    elif suf == ".csv":
        out_path.write_text("brand,domains\n", encoding="utf-8")
    else:
        out_path.write_text("", encoding="utf-8")


with st.sidebar:
    brand = st.selectbox("Brand", BRANDS, index=0)
    template_label = st.selectbox("Output template type", list(TEMPLATES.keys()), index=0)
    template_profile = TEMPLATES[template_label]

    st.markdown("### Quality / Learning")
    write_review = st.checkbox("Write Review sheet (confidence + flags)", value=True)
    min_conf = st.slider("Min confidence threshold", 0.0, 1.0, 0.80, 0.05)
    apply_overrides = st.checkbox("Apply saved overrides (feedback DB)", value=True)
    save_observations = st.checkbox("Save observations to feedback DB", value=True)
    overwrite = st.checkbox("Overwrite existing fields", value=False)
    dedupe = st.checkbox("Deduplicate models during conversion", value=True)

    st.markdown("### Rules (optional)")
    use_repo_rules = st.checkbox("Use repo rules by default", value=True)
    st.caption("Rules improve naming + warranty consistency when scraping is weak.")


st.markdown("## Upload your distributor file(s)")
uploads = st.file_uploader(
    "Upload one or more files (xlsx/pdf/docx/png/jpg/jpeg/zip).",
    accept_multiple_files=True,
    type=["xlsx", "xls", "pdf", "docx", "png", "jpg", "jpeg", "zip"],
)

st.markdown("## Upload the WEX template you want to fill")
template_upload = st.file_uploader(
    "Upload the blank WEX template XLSX (Single Part / Supplier Loader / Part+Labor bundle).",
    accept_multiple_files=False,
    type=["xlsx", "xls"],
)

st.markdown("## Optional: Upload config/rules files (if you don’t want repo defaults)")
colA, colB, colC, colD = st.columns(4)

with colA:
    templates_json_upload = st.file_uploader(
        "wex_templates.json",
        accept_multiple_files=False,
        type=["json"],
        key="tpljson",
    )

with colB:
    manufacturers_csv_upload = st.file_uploader(
        "manufacturers_us_seed.csv",
        accept_multiple_files=False,
        type=["csv"],
        key="mfgcsv",
    )

with colC:
    warranty_rules_upload = st.file_uploader(
        "warranty_rules.json",
        accept_multiple_files=False,
        type=["json"],
        key="warrantyjson",
    )

with colD:
    series_rules_upload = st.file_uploader(
        "series_rules.json",
        accept_multiple_files=False,
        type=["json"],
        key="seriesjson",
    )

run = st.button("Run conversion + enrichment", type="primary", disabled=not (uploads and template_upload))

if run:
    # Guardrails
    if not CONVERTER.exists():
        st.error(f"Missing converter: {CONVERTER}. Put it in tools/.")
        st.stop()
    if not ENRICHER.exists():
        st.error(f"Missing enricher: {ENRICHER}. Put it in tools/.")
        st.stop()

    with tempfile.TemporaryDirectory() as td:
        workdir = Path(td)
        in_dir = workdir / "pricebooks_in"
        out_dir = workdir / "out"
        rules_dir = workdir / "rules"

        in_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        rules_dir.mkdir(parents=True, exist_ok=True)

        # Save distributor uploads
        for f in uploads:
            (in_dir / f.name).write_bytes(f.getbuffer())

        # Save template upload
        template_path = workdir / template_upload.name
        template_path.write_bytes(template_upload.getbuffer())

        # Config files
        templates_json_path = workdir / "wex_templates.json"
        manufacturers_csv_path = workdir / "manufacturers_us_seed.csv"

        write_uploaded_or_default(templates_json_upload, REPO_ROOT / "wex_templates.json", templates_json_path)
        write_uploaded_or_default(manufacturers_csv_upload, REPO_ROOT / "manufacturers_us_seed.csv", manufacturers_csv_path)

        # Rules files
        warranty_rules_path = rules_dir / "warranty_rules.json"
        series_rules_path = rules_dir / "series_rules.json"

        if use_repo_rules:
            repo_warranty = RULES_DIR / "warranty_rules.json"
            repo_series = RULES_DIR / "series_rules.json"

            write_uploaded_or_default(None, repo_warranty if repo_warranty.exists() else None, warranty_rules_path)
            write_uploaded_or_default(None, repo_series if repo_series.exists() else None, series_rules_path)
        else:
            write_uploaded_or_default(warranty_rules_upload, None, warranty_rules_path)
            write_uploaded_or_default(series_rules_upload, None, series_rules_path)

        feedback_db = workdir / "feedback_memory.sqlite"
        cache_db = workdir / "ingest_cache.sqlite"

        safe_brand = brand.replace(" ", "_").replace("&", "and")
        skeleton_path = out_dir / f"WEX_skeleton_{safe_brand}_{template_profile}.xlsx"
        ready_path = out_dir / f"WEX_READY_{safe_brand}_{template_profile}.xlsx"

        st.markdown("## Running pipeline")

        try:
            st.markdown("### Stage A: Convert distributor file(s) → WEX skeleton (v2)")
            convert_cmd = [
                "python",
                str(CONVERTER),
                "--input", str(in_dir),
                "--template", str(template_path),
                "--templates-config", str(templates_json_path),
                "--template-profile", template_profile,
                "--brand", brand,
                "--out", str(skeleton_path),
            ]
            if dedupe:
                convert_cmd.append("--dedupe")

            run_cmd(convert_cmd, cwd=str(REPO_ROOT))

            st.markdown("### Stage B: Enrich skeleton → WEX-ready (v2 + rules)")
            enrich_cmd = [
                "python",
                str(ENRICHER),
                "--input", str(skeleton_path),
                "--brand", brand,
                "--manufacturer-csv", str(manufacturers_csv_path),
                "--cache", str(cache_db),
                "--feedback-db", str(feedback_db),
                "--out", str(ready_path),
                "--min-confidence", str(min_conf),
            ]

            # Pass rules only if they exist and are non-trivial
            if warranty_rules_path.exists() and warranty_rules_path.stat().st_size > 2:
                enrich_cmd += ["--warranty-rules", str(warranty_rules_path)]
            if series_rules_path.exists() and series_rules_path.stat().st_size > 2:
                enrich_cmd += ["--series-rules", str(series_rules_path)]

            if write_review:
                enrich_cmd.append("--write-review")
            if apply_overrides:
                enrich_cmd.append("--apply-overrides")
            if save_observations:
                enrich_cmd.append("--save-observations")
            if overwrite:
                enrich_cmd.append("--overwrite")

            run_cmd(enrich_cmd, cwd=str(REPO_ROOT))

            st.success("Done. Download your output(s) below.")

            st.markdown("## Outputs")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button(
                    "Download WEX READY XLSX",
                    data=ready_path.read_bytes(),
                    file_name=ready_path.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            with c2:
                st.download_button(
                    "Download Skeleton XLSX",
                    data=skeleton_path.read_bytes(),
                    file_name=skeleton_path.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            st.markdown("## Optional outputs")
            c3, c4 = st.columns(2)
            with c3:
                st.download_button(
                    "Download feedback_memory.sqlite",
                    data=feedback_db.read_bytes() if feedback_db.exists() else b"",
                    file_name="feedback_memory.sqlite",
                    mime="application/octet-stream",
                    disabled=not feedback_db.exists(),
                )
            with c4:
                st.caption("Skeleton v2 writes a 'Skipped' sheet into the skeleton output if it couldn't detect model numbers in some rows.")

        except Exception as e:
            st.error(str(e))
            st.info("Paste the last ~50 lines of the log output above and I’ll tell you exactly what broke.")
