#!/usr/bin/env python3
"""
Patch app/main_window.py to inject a global domain system prompt and today's date.
- Adds DOMAIN_SYSTEM_PROMPT near the imports if not already present.
- Modifies on_little_buddy(...) to set SIDECAR_SYSTEM_PROMPT and SIDECAR_TODAY env vars.
Usage:
    python patch_main_window_system_prompt.py
"""
import io
import os
import re
from datetime import datetime

TARGET_FILE = os.path.join("app", "main_window.py")

DOMAIN_PROMPT = r'''
YOU ARE: Aldin-Mini — a domain assistant for:
• Data Governance (policies, lineage, stewardship, controls)
• Data Management (MDM, metadata, cataloging, lifecycle)
• Data Architecture (patterns, lakehouse/mesh/warehouse, modeling)
• Analytics & BI, and Data Science/ML

GUIDANCE:
- Ground answers in practical, auditable steps (policies → controls → roles → artifacts).
- Prefer bullet points, numbered steps, and concrete examples.
- When asked for “today’s” info, use the injected date: {{TODAY}}.
- If a question is vague or generic, ask one clarifying question first, then answer.
'''

HEADER_BLOCK = (
    "# >>> AUTO-INSERTED BY patch_main_window_system_prompt.py >>>\n"
    "from datetime import datetime as __patch_dt\n"
    "DOMAIN_SYSTEM_PROMPT = r\"\"\"" + DOMAIN_PROMPT.strip() + "\"\"\"\n"
    "# <<< AUTO-INSERTED BY patch_main_window_system_prompt.py <<<\n"
)

def ensure_header_block(text: str) -> str:
    if "DOMAIN_SYSTEM_PROMPT = r\"\"\"" in text:
        return text  # already present
    # Insert after the last import block
    m = re.search(r"^(?:from\s+\S+\s+import\s+.*|import\s+\S+).*?$", text, flags=re.M|re.S)
    if not m:
        # If imports not matched well, put at the very top
        return HEADER_BLOCK + "\n" + text
    # Find end of the import section (last import occurrence)
    last = None
    for match in re.finditer(r"^(?:from\s+\S+\s+import\s+.*|import\s+\S+).*?$", text, flags=re.M):
        last = match
    if last:
        insert_at = last.end()
        return text[:insert_at] + "\n" + HEADER_BLOCK + text[insert_at:]
    return HEADER_BLOCK + "\n" + text

def patch_on_little_buddy(text: str) -> str:
    # We will insert two environment lines inside on_little_buddy right after dlg = DataBuddyDialog(self)
    # - os.environ['SIDECAR_TODAY'] = datetime.now().date().isoformat()
    # - os.environ['SIDECAR_SYSTEM_PROMPT'] = DOMAIN_SYSTEM_PROMPT.replace('{{TODAY}}', os.environ.get('SIDECAR_TODAY', ''))
    pattern = (r"(def\s+on_little_buddy\s*\(.*?\):\s*\n(?:\s*.*\n)*?"
               r"\s*dlg\s*=\s*DataBuddyDialog\(self\)\s*\n)")
    def repl(m):
        prefix = m.group(1)
        inject = (
            "        # Auto-injected domain prompt + today date\n"
            "        os.environ['SIDECAR_TODAY'] = __patch_dt.now().date().isoformat()\n"
            "        os.environ['SIDECAR_SYSTEM_PROMPT'] = DOMAIN_SYSTEM_PROMPT.replace('{{TODAY}}', os.environ['SIDECAR_TODAY'])\n"
        )
        if "SIDECAR_SYSTEM_PROMPT" in prefix:
            return prefix  # already patched in this location
        return prefix + inject
    new_text, n = re.subn(pattern, repl, text, flags=re.S)
    if n == 0:
        # Fallback: try a simpler locate by the line
        simple_pat = "dlg = DataBuddyDialog(self)"
        if simple_pat in text and "SIDECAR_SYSTEM_PROMPT" not in text:
            new_text = text.replace(
                simple_pat,
                simple_pat + "\n        # Auto-injected domain prompt + today date"
                           + "\n        os.environ['SIDECAR_TODAY'] = __patch_dt.now().date().isoformat()"
                           + "\n        os.environ['SIDECAR_SYSTEM_PROMPT'] = DOMAIN_SYSTEM_PROMPT.replace('{{TODAY}}', os.environ['SIDECAR_TODAY'])"
            )
        else:
            new_text = text
    return new_text

def main():
    if not os.path.exists(TARGET_FILE):
        raise SystemExit(f"Could not find {TARGET_FILE}. Run this from your project root.")
    with io.open(TARGET_FILE, 'r', encoding='utf-8') as f:
        text = f.read()
    orig = text

    text = ensure_header_block(text)
    text = patch_on_little_buddy(text)

    if text != orig:
        with io.open(TARGET_FILE, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f"Patched {TARGET_FILE} ✔\nInjected DOMAIN_SYSTEM_PROMPT and SIDECAR_TODAY handling.")
    else:
        print("No changes made (already patched)." )

if __name__ == '__main__':
    main()
