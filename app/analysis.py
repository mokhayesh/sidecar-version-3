import csv
import io
import json
import re
from datetime import datetime

import numpy as np
import pandas as pd
import requests

# ──────────────────────────────────────────────────────────────────────────────
# CSV/Parsing helpers
# ──────────────────────────────────────────────────────────────────────────────

def detect_and_split_data(text: str):
    lines = text.strip().splitlines()
    if not lines:
        return [], []
    delim = "," if "," in lines[0] else "|"
    rows = list(csv.reader(lines, delimiter=delim))
    return (rows[0], rows[1:]) if len(rows) > 1 else ([], [])


_SPLIT_CAMEL = re.compile(r'(?<=[a-z0-9])(?=[A-Z])')
def _split_words(col: str) -> str:
    return _SPLIT_CAMEL.sub(" ", col.replace("_", " "))


# ──────────────────────────────────────────────────────────────────────────────
# Profile / Quality / Catalog / Compliance (original logic)
# ──────────────────────────────────────────────────────────────────────────────

def profile_analysis(df: pd.DataFrame):
    now, total = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(df)
    rows = []
    for col in df.columns:
        s = df[col]
        nulls = int(s.isnull().sum())
        blanks = int((s.astype(str).str.strip() == "").sum())
        uniq = int(s.nunique(dropna=True))
        comp = round(100 * (total - nulls - blanks) / total, 2) if total else 0
        if pd.api.types.is_numeric_dtype(s):
            vals = pd.to_numeric(s, errors="coerce").dropna()
            stats = (vals.min(), vals.max(), vals.median(), vals.std()) if not vals.empty else ("N/A",) * 4
        else:
            lengths = s.dropna().astype(str).str.strip().replace("", pd.NA).dropna().str.len()
            stats = (
                lengths.min() if not lengths.empty else "N/A",
                lengths.max() if not lengths.empty else "N/A",
                lengths.median() if not lengths.empty else "N/A",
                "N/A"
            )
        rows.append([col, total, uniq, comp, nulls, blanks, *stats, now])
    hdr = ["Field", "Total", "Unique", "Completeness (%)",
           "Nulls", "Blanks", "Min", "Max", "Median", "Std", "Analysis Date"]
    return hdr, rows


_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
_DATE_PARSE = lambda x: pd.to_datetime(x, errors="coerce")

def _default_valid_count(s: pd.Series) -> int:
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").notna().sum()
    if "date" in s.name.lower() or pd.api.types.is_datetime64_any_dtype(s):
        return _DATE_PARSE(s).notna().sum()
    if "email" in s.name.lower():
        return s.astype(str).str.match(_EMAIL_RE).sum()
    return s.astype(str).str.strip().ne("").sum()

def quality_analysis(df: pd.DataFrame, rules: dict[str, re.Pattern] | None = None):
    now, total = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(df)
    rows = []
    for col in df.columns:
        s = df[col]
        nulls = int(s.isnull().sum())
        blanks = int((s.astype(str).str.strip() == "").sum())
        comp_pct = round(100 * (total - nulls - blanks) / total, 2) if total else 0
        uniq_pct = round(100 * s.nunique(dropna=True) / total, 2) if total else 0
        if rules and col in rules:
            valid_cnt = s.dropna().astype(str).str.match(rules[col]).sum()
        else:
            valid_cnt = _default_valid_count(s)
        valid_pct = round(100 * valid_cnt / total, 2) if total else 0
        score = round((comp_pct + valid_pct) / 2, 2)
        rows.append([col, total, comp_pct, uniq_pct, valid_pct, score, now])
    hdr = ["Field", "Total", "Completeness (%)", "Uniqueness (%)",
           "Validity (%)", "Quality Score (%)", "Analysis Date"]
    return hdr, rows


def _business_description(col: str) -> str:
    name = col.lower()
    clean = re.sub(r'[^a-z0-9_]', ' ', name)
    tokens = [t for t in re.split(r'[_\s]+', clean) if t]
    if not tokens:
        return "Field describing the record."
    noun = " ".join(tokens).replace(" id", "").strip()
    if tokens[-1] == "id":
        ent = " ".join(tokens[:-1]) or "record"
        return f"Unique identifier for each {ent}."
    if "email" in tokens:
        return f"Email address of the {noun}."
    if any(t in tokens for t in ("phone", "tel", "telephone")):
        return f"Telephone number associated with the {noun}."
    if "date" in tokens or "timestamp" in tokens:
        return f"Date or time related to the {noun}."
    if {"amount","total","price","cost","balance"} & set(tokens):
        return f"Monetary amount representing the {noun}."
    if {"qty","quantity","count","number"} & set(tokens):
        return f"Number of {noun}."
    if "status" in tokens:
        return f"Current status of the {noun}."
    if "flag" in tokens:
        return f"Indicator flag for the {noun}."
    if "type" in tokens or "category" in tokens:
        return f"Classification type of the {noun}."
    if "code" in tokens:
        return f"Standard code representing the {noun}."
    return f"{_split_words(col).title()} for each record."

def catalog_analysis(df: pd.DataFrame):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for col in df.columns:
        s = df[col]
        friendly = _split_words(col).title()
        descr = _business_description(col)
        dtype = ("Numeric" if pd.api.types.is_numeric_dtype(s)
                 else "Date" if "date" in descr else "Text")
        nullable = "Yes" if s.isnull().any() else "No"
        example = str(s.dropna().iloc[0]) if not s.dropna().empty else ""
        rows.append([col, friendly, descr, dtype, nullable, example, now])
    hdr = ["Field", "Friendly Name", "Description",
           "Data Type", "Nullable", "Example", "Analysis Date"]
    return hdr, rows


def compliance_analysis(_df: pd.DataFrame):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        ["Quality","MyApp","DataLake","Table","85%","80%","✔","Meets SLA",now],
        ["Completeness","MyApp","DataLake","Table","85%","80%","✔","Meets SLA",now],
        ["Validity","MyApp","DataLake","Table","85%","80%","✔","Meets SLA",now],
        ["GLBA","MyApp","DataLake","Table","85%","80%","✔","Meets SLA",now],
        ["CCPA","MyApp","DataLake","Table","70%","80%","✘","Below SLA",now],
    ]
    hdr = ["Aspect","Application","Layer","Table",
           "Score","SLA","Compliant","Notes","Analysis Date"]
    return hdr, rows


# ──────────────────────────────────────────────────────────────────────────────
# Baseline rule-based anomaly detector (fallback)
# ──────────────────────────────────────────────────────────────────────────────

def _rule_based_anomalies(df: pd.DataFrame):
    findings = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for col in df.columns:
        s = df[col].astype(str).str.strip()
        blanks = int((s == "").sum())
        nulls = int((s.str.lower() == "nan").sum())
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().any():
            neg = int((numeric < 0).sum())
            std = numeric.std(skipna=True) or 0
            huge = int((numeric > numeric.mean(skipna=True) + 6*std).sum())
        else:
            neg = huge = 0

        bad_email = 0
        if "email" in col.lower():
            bad_email = int(~s.str.contains(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", regex=True, na=True).sum())

        if blanks or nulls or neg or huge or bad_email:
            reason = []
            if blanks: reason.append(f"{blanks} blank")
            if nulls: reason.append(f"{nulls} 'nan'")
            if neg: reason.append(f"{neg} negative")
            if huge: reason.append(f"{huge} outlier")
            if bad_email: reason.append(f"{bad_email} invalid email")
            rec = "Review source, add validation, and backfill where possible."
            findings.append([col, " | ".join(reason), rec, now])

    if not findings:
        findings = [["(none)", "No obvious anomalies found", "No action", now]]

    hdr = ["Field", "Reason", "Recommendation", "Detected At"]
    return hdr, findings


# ──────────────────────────────────────────────────────────────────────────────
# Heuristic anomalies used by the UI (adds duplicates & z-score outliers)
# ──────────────────────────────────────────────────────────────────────────────

def anomalies_analysis(df: pd.DataFrame):
    """Return (headers, rows) of anomalies suitable for the grid.

    Rules:
      • Missing / blank cells
      • Duplicate full rows
      • Numeric outliers (|z| > 3)
      • Email format checks for columns with 'email' in the name
    """
    findings = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 0) Duplicate rows
    dups = df.duplicated(keep="first")
    for idx in dups[dups].index.tolist():
        findings.append(["(row)", "Duplicate row", "Deduplicate or add a key", now])

    # 1) Missing / blank cells
    for col in df.columns:
        s = df[col]
        blanks_mask = s.astype(str).str.strip().eq("") | s.isna()
        n_blanks = int(blanks_mask.sum())
        if n_blanks:
            findings.append([col, f"{n_blanks} missing/blank", "Impute, drop or enforce NOT NULL", now])

    # 2) Numeric outliers via z-score > 3
    for col in df.columns:
        s_num = pd.to_numeric(df[col], errors="coerce")
        if s_num.notna().sum() == 0:
            continue
        mu = s_num.mean()
        sigma = s_num.std(ddof=0)
        if sigma and np.isfinite(sigma) and sigma > 0:
            z = (s_num - mu).abs() / sigma
            out_idx = z[z > 3].index.tolist()
            if out_idx:
                findings.append([col, f"{len(out_idx)} numeric outlier(s) |z|>3", "Investigate/clip/winsorize", now])

    # 3) Email format check
    email_cols = [c for c in df.columns if "email" in c.lower()]
    if email_cols:
        email_re = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        for col in email_cols:
            bad = 0
            for _, val in df[col].items():
                if pd.isna(val):
                    continue
                v = str(val).strip()
                if v and not email_re.match(v):
                    bad += 1
            if bad:
                findings.append([col, f"{bad} invalid email(s)", "Validate with regex & cleanse source", now])

    if not findings:
        findings = [["(none)", "No anomalies found", "", now]]

    hdr = ["Field", "Reason", "Recommendation", "Detected At"]
    return hdr, findings

# Backwards/compat alias for older wiring
def detect_anomalies(df: pd.DataFrame):
    return anomalies_analysis(df)


# ──────────────────────────────────────────────────────────────────────────────
# LLM plumbing
# ──────────────────────────────────────────────────────────────────────────────

def _provider_from_defaults(defaults: dict) -> str:
    # Expected: "openai" or "gemini" (case-insensitive). Anything else -> no-op.
    return (defaults.get("provider") or defaults.get("ai_provider") or "").strip().lower()

def _openai_chat_json(defaults: dict, prompt: str, model: str | None = None, timeout=60):
    api_key = (defaults.get("openai_api_key") or defaults.get("api_key") or "").strip()
    url = (defaults.get("chat_url") or "https://api.openai.com/v1/chat/completions").strip()
    mdl = model or defaults.get("default_model") or "gpt-4o-mini"
    if not api_key:
        raise RuntimeError("OpenAI API key not configured")

    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": mdl,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": "You are a precise data analyst. Respond ONLY with valid JSON."},
                {"role": "user", "content": prompt},
            ],
            "temperature": float(defaults.get("temperature", 0.3)),
            "max_tokens": int(defaults.get("max_tokens", 1200)),
        },
        timeout=timeout,
        verify=False,
    )
    resp.raise_for_status()
    return json.loads(resp.json()["choices"][0]["message"]["content"])

def _gemini_json(defaults: dict, prompt: str, model: str | None = None, timeout=60):
    api_key = (defaults.get("gemini_api_key") or "").strip()
    base = (defaults.get("gemini_base_url") or "https://generativelanguage.googleapis.com/v1beta").rstrip("/")
    mdl = model or defaults.get("default_model") or "gemini-1.5-flash"
    if not api_key:
        raise RuntimeError("Gemini API key not configured")

    url = f"{base}/models/{mdl}:generateContent?key={api_key}"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": float(defaults.get("temperature", 0.3))},
    }
    resp = requests.post(url, json=body, timeout=timeout)
    resp.raise_for_status()
    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
    # try to isolate a JSON block if the model wrapped it in prose
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start:end+1]
    return json.loads(text)

def _llm_json(defaults: dict, prompt: str, model: str | None = None, timeout=60):
    prov = _provider_from_defaults(defaults)
    if prov == "openai":
        return _openai_chat_json(defaults, prompt, model, timeout)
    if prov == "gemini":
        return _gemini_json(defaults, prompt, model, timeout)
    raise RuntimeError("No AI provider configured")


# ──────────────────────────────────────────────────────────────────────────────
# AI Catalog & AI Anomalies (with fallbacks)
# ──────────────────────────────────────────────────────────────────────────────

def ai_catalog_analysis(df: pd.DataFrame, defaults: dict):
    """LLM-backed catalog; falls back to heuristic if the call fails."""
    try:
        preview_rows = min(12, len(df))
        sample = df.head(preview_rows).astype(str).to_dict(orient="records")
        schema = []
        for c in df.columns:
            dtype = str(df[c].dtype)
            nulls = int(df[c].isna().sum())
            uniq = int(df[c].nunique(dropna=True))
            schema.append({"name": c, "dtype": dtype, "nulls": nulls, "unique": uniq})

        prompt = (
            "You are generating a data catalog for a tabular dataset.\n"
            "Return STRICT JSON with this structure:\n"
            "{ \"items\": [\n"
            "  {\"field\": str, \"friendly_name\": str, \"description\": str,\n"
            "   \"data_type\": one_of[\"Text\",\"Numeric\",\"Date\",\"Boolean\",\"Categorical\"],\n"
            "   \"nullable\": one_of[\"Yes\",\"No\"], \"example\": str }\n"
            "]}\n\n"
            f"Columns & quick stats:\n{json.dumps(schema, ensure_ascii=False)}\n\n"
            f"Sample rows (strings):\n{json.dumps(sample, ensure_ascii=False)}\n\n"
            "Be concise and business-friendly in descriptions. Do NOT include any text outside the JSON."
        )

        obj = _llm_json(defaults, prompt)
        items = obj.get("items") or []
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for it in items:
            rows.append([
                it.get("field",""),
                it.get("friendly_name",""),
                it.get("description",""),
                it.get("data_type",""),
                it.get("nullable",""),
                it.get("example",""),
                now
            ])
        if not rows:
            raise RuntimeError("Empty AI catalog")
        hdr = ["Field", "Friendly Name", "Description", "Data Type", "Nullable", "Example", "Analysis Date"]
        return hdr, rows

    except Exception:
        return catalog_analysis(df)


def ai_detect_anomalies(df: pd.DataFrame, defaults: dict):
    """LLM-backed anomaly detection; falls back to rule-based if the call fails."""
    try:
        preview_rows = min(30, len(df))
        sample = df.head(preview_rows).astype(str).to_dict(orient="records")
        quick = {}
        for c in df.columns:
            s = df[c]
            quick[c] = {
                "nulls": int(s.isna().sum()),
                "blanks": int((s.astype(str).str.strip() == "").sum()),
                "unique": int(s.nunique(dropna=True)),
                "dtype": str(s.dtype),
            }

        prompt = (
            "You are a data quality expert. Find likely anomalies in this dataset.\n"
            "Return STRICT JSON ONLY in this format:\n"
            "{ \"items\": [ {\"field\": str, \"reason\": str, \"recommendation\": str} ] }\n"
            "Reasons should be specific (e.g., 'outliers > 6 sigma', 'invalid email format', 'suspicious zero balance').\n"
            "Recommendations should be actionable (e.g., 'validate format with regex', 'clip to 3σ', 'backfill from source').\n\n"
            f"Quick stats per column: {json.dumps(quick, ensure_ascii=False)}\n\n"
            f"Sample rows (strings): {json.dumps(sample, ensure_ascii=False)}\n\n"
            "Output ONLY valid JSON."
        )

        obj = _llm_json(defaults, prompt)
        items = obj.get("items") or []
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for it in items:
            rows.append([
                it.get("field",""),
                it.get("reason",""),
                it.get("recommendation",""),
                now
            ])
        if not rows:
            raise RuntimeError("Empty AI anomalies")
        hdr = ["Field", "Reason", "Recommendation", "Detected At"]
        return hdr, rows

    except Exception:
        return _rule_based_anomalies(df)
