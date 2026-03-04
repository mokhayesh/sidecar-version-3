# main.py
import os
import json
import wx

# ──────────────────────────────────────────────────────────────────────────────
# Config loading/saving
# ──────────────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULTS_FILE = os.path.join(BASE_DIR, "defaults.json")

# Default settings (can be overridden by defaults.json)
defaults = {
    "api_key": "",
    "filepath": os.path.expanduser("~"),
    "default_model": "gpt-4",
    "max_tokens": "800",
    "temperature": "0.6",
    "top_p": "1.0",
    "frequency_penalty": "0.0",
    "presence_penalty": "0.0",
    "url": "https://api.openai.com/v1/chat/completions",
    "image_generation_url": "https://api.openai.com/v1/images/generations",

    # AWS

    # Snowflake
    "sf_account": "",
    "sf_user": "",
    "sf_password": "",
    "sf_role": "",
    "sf_warehouse": "",
    "sf_database": "",
    "sf_schema": "",
    "sf_authenticator": "snowflake",

    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
    "aws_s3_region": "us-east-1",
    "aws_profile_bucket": "",
    "aws_quality_bucket": "",
    "aws_catalog_bucket": "",
    "aws_compliance_bucket": "",

    # Email
    "smtp_server": "",
    "smtp_port": "",
    "email_username": "",
    "email_password": "",
    "from_email": "",
    "to_email": ""
}

def load_defaults():
    """Load defaults.json from the repo root if present."""
    if os.path.exists(DEFAULTS_FILE):
        try:
            with open(DEFAULTS_FILE, "r", encoding="utf-8") as f:
                defaults.update(json.load(f))
        except Exception as e:
            print(f"Warning: Could not load defaults.json — {e}")

def save_defaults():
    """Write the current defaults dict back to defaults.json."""
    try:
        with open(DEFAULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(defaults, f, indent=2)
        wx.MessageBox("Settings saved.", "Settings", wx.OK | wx.ICON_INFORMATION)
    except Exception as e:
        wx.MessageBox(f"Failed to save settings:\n{e}", "Settings", wx.OK | wx.ICON_ERROR)

# ──────────────────────────────────────────────────────────────────────────────
# App entrypoint
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    load_defaults()

    app = wx.App(False)

    # Import AFTER wx.App() is created so child windows can use wx safely.
    # Use the package path ("app.main_window") so Python finds the file in /app.
    from app.main_window import MainWindow

    MainWindow()
    app.MainLoop()
