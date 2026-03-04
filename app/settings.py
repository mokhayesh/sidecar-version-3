import os
import json
import wx
import wx.lib.scrolledpanel as scrolled

# ──────────────────────────────────────────────────────────────────────────────
# Defaults & persistence
# ──────────────────────────────────────────────────────────────────────────────

DEFAULTS_FILE = "defaults.json"

# sensible base defaults
defaults = {
    "provider": "custom",

    # ACTIVE (used by the app today)
    "url": "http://127.0.0.1:8000/v1/chat/completions",
    "api_key": "sk-aldin-local-123",
    "default_model": "aldin-mini",
    "fast_model": "aldin-mini",

    # stored profiles (so you don't retype)
    "custom_url": "http://127.0.0.1:8000/v1/chat/completions",
    "custom_api_key": "sk-aldin-local-123",
    "custom_model": "aldin-mini",

    "openai_url": "https://api.openai.com/v1/chat/completions",
    "openai_api_key": "",
    "openai_org": "",
    "openai_default_model": "gpt-4o",
    "openai_fast_model": "gpt-4o-mini",

    "gemini_text_url": "https://generativelanguage.googleapis.com/v1beta/models",
    "gemini_api_key": "",
    "gemini_default_model": "gemini-1.5-pro",
    "gemini_fast_model": "gemini-1.5-flash",

    "max_tokens": "800",
    "temperature": "0.6",
    "top_p": "1.0",
    "frequency_penalty": "0.0",
    "presence_penalty": "0.0",

    # images (unchanged)
    "image_provider": "auto",
    "image_model": "gpt-image-1",
    "image_generation_url": "https://api.openai.com/v1/images/generations",
    "stability_api_key": "",

    # optional TTS
    "azure_tts_key": "",
    "azure_tts_region": "",

    # Snowflake
    "sf_account": "",
    "sf_user": "",
    "sf_password": "",
    "sf_role": "",
    "sf_warehouse": "",
    "sf_database": "",
    "sf_schema": "",
    "sf_authenticator": "snowflake",

    # AWS
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "aws_session_token": "",
    "aws_s3_region": "us-east-1",
    "aws_profile_bucket": "",
    "aws_quality_bucket": "",
    "aws_catalog_bucket": "",
    "aws_compliance_bucket": "",
    "aws_anomalies_bucket": "",
    "aws_synthetic_bucket": "",

    # email
    "smtp_server": "",
    "smtp_port": "",
    "email_username": "",
    "email_password": "",
    "from_email": "",
    "to_email": "",

    "filepath": os.path.expanduser("~"),
}

if os.path.exists(DEFAULTS_FILE):
    try:
        defaults.update(json.load(open(DEFAULTS_FILE, "r", encoding="utf-8")))
    except Exception:
        pass


def save_defaults() -> None:
    json.dump(defaults, open(DEFAULTS_FILE, "w", encoding="utf-8"), indent=2)
    wx.MessageBox("Settings saved.", "Settings", wx.OK | wx.ICON_INFORMATION)


# ──────────────────────────────────────────────────────────────────────────────
# Settings window with provider & model dropdowns
# ──────────────────────────────────────────────────────────────────────────────

OPENAI_MAIN = ["gpt-4o", "o4-mini", "gpt-4.1", "gpt-4.1-mini", "gpt-4o-mini"]
OPENAI_FAST = ["gpt-4o-mini", "gpt-4.1-mini", "o4-mini"]
OPENAI_IMAGE = ["gpt-image-1"]

GEMINI_MAIN = ["gemini-1.5-pro", "gemini-1.5-flash"]
GEMINI_FAST = ["gemini-1.5-flash", "gemini-1.5-flash-8b"]
GEMINI_IMAGE = ["gemini-1.5-flash", "gemini-1.5-pro"]  # used for image output

STABILITY_IMAGE = ["sdxl", "sd3-medium"]

# include local models in "custom"
CUSTOM_MAIN = ["aldin-mini"]        # extend as you add more
CUSTOM_FAST = ["aldin-mini"]
IMAGE_PROVIDERS = ["auto", "openai", "gemini", "stability", "none"]
PROVIDERS = ["custom", "openai", "gemini", "auto"]

# how to map a provider to the keys in defaults.json
PROFILE_KEYS = {
    "custom": {
        "url": "custom_url",
        "api_key": "custom_api_key",
        "default_model": "custom_model",
        "fast_model": "custom_model",
    },
    "openai": {
        "url": "openai_url",
        "api_key": "openai_api_key",
        "org": "openai_org",
        "default_model": "openai_default_model",
        "fast_model": "openai_fast_model",
    },
    "gemini": {
        "url": "gemini_text_url",  # Gemini uses base+model in client, we still show/save here
        "api_key": "gemini_api_key",
        "default_model": "gemini_default_model",
        "fast_model": "gemini_fast_model",
    },
}


class SettingsWindow(wx.Frame):
    def __init__(self, parent):
        super().__init__(parent, title="Settings", size=(640, 860))

        # Make settings scrollable so Snowflake section is always visible
        panel = scrolled.ScrolledPanel(self)
        panel.SetupScrolling(scroll_x=False, scroll_y=True)

        s = wx.GridBagSizer(6, 6)
        row = 0

        # Provider
        s.Add(wx.StaticText(panel, label="Provider:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.provider = wx.Choice(panel, choices=PROVIDERS)
        sel = defaults.get("provider", "custom")
        try:
            self.provider.SetSelection(PROVIDERS.index(sel))
        except ValueError:
            self.provider.SetSelection(0)
        self.provider.Bind(wx.EVT_CHOICE, self._on_provider_change)
        s.Add(self.provider, (row, 1), span=(1, 3), flag=wx.EXPAND)
        row += 1

        # Keys / URLs
        s.Add(wx.StaticText(panel, label="API Key:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.api_key = wx.TextCtrl(panel, value=defaults.get("api_key", ""))
        s.Add(self.api_key, (row, 1), span=(1, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="OpenAI Org (optional):"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.org = wx.TextCtrl(panel, value=defaults.get("openai_org", ""))
        s.Add(self.org, (row, 1), span=(1, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Chat URL:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.chat_url = wx.TextCtrl(panel, value=defaults.get("url", "http://127.0.0.1:8000/v1/chat/completions"))
        s.Add(self.chat_url, (row, 1), span=(1, 3), flag=wx.EXPAND)
        row += 1

        # Model dropdowns
        s.Add(wx.StaticText(panel, label="Default Model:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.default_model = wx.Choice(panel)
        s.Add(self.default_model, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Fast Model:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.fast_model = wx.Choice(panel)
        s.Add(self.fast_model, (row, 3), flag=wx.EXPAND)
        row += 1

        # Gen settings
        s.Add(wx.StaticText(panel, label="Max Tokens:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.max_tokens = wx.TextCtrl(panel, value=str(defaults.get("max_tokens", "800")))
        s.Add(self.max_tokens, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Temperature:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.temperature = wx.TextCtrl(panel, value=str(defaults.get("temperature", "0.6")))
        s.Add(self.temperature, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Top P:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.top_p = wx.TextCtrl(panel, value=str(defaults.get("top_p", "1.0")))
        s.Add(self.top_p, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Frequency Penalty:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.freq_pen = wx.TextCtrl(panel, value=str(defaults.get("frequency_penalty", "0.0")))
        s.Add(self.freq_pen, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Presence Penalty:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.pres_pen = wx.TextCtrl(panel, value=str(defaults.get("presence_penalty", "0.0")))
        s.Add(self.pres_pen, (row, 1), flag=wx.EXPAND)
        row += 1

        # Image provider + model
        s.Add(wx.StaticText(panel, label="Image Provider:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.image_provider = wx.Choice(panel, choices=IMAGE_PROVIDERS)
        try:
            self.image_provider.SetSelection(IMAGE_PROVIDERS.index(defaults.get("image_provider", "auto")))
        except ValueError:
            self.image_provider.SetSelection(0)
        self.image_provider.Bind(wx.EVT_CHOICE, self._on_image_provider_change)
        s.Add(self.image_provider, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Image Model:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.image_model = wx.Choice(panel)
        s.Add(self.image_model, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Image URL (OpenAI):"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.image_url = wx.TextCtrl(panel, value=defaults.get("image_generation_url", "https://api.openai.com/v1/images/generations"))
        s.Add(self.image_url, (row, 1), span=(1, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Stability API Key:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.stability = wx.TextCtrl(panel, value=defaults.get("stability_api_key", ""))
        s.Add(self.stability, (row, 1), span=(1, 3), flag=wx.EXPAND)
        row += 1

        # TTS
        s.Add(wx.StaticText(panel, label="Azure TTS Key:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.azure_key = wx.TextCtrl(panel, value=defaults.get("azure_tts_key", ""))
        s.Add(self.azure_key, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Azure TTS Region:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.azure_region = wx.TextCtrl(panel, value=defaults.get("azure_tts_region", ""))
        s.Add(self.azure_region, (row, 3), flag=wx.EXPAND)
        row += 1

        # AWS keys/region
        s.Add(wx.StaticText(panel, label="AWS Access Key:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.aws_key = wx.TextCtrl(panel, value=defaults.get("aws_access_key_id", ""))
        s.Add(self.aws_key, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="AWS Secret Key:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.aws_secret = wx.TextCtrl(panel, value=defaults.get("aws_secret_access_key", ""))
        s.Add(self.aws_secret, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="AWS Session Token:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.aws_token = wx.TextCtrl(panel, value=defaults.get("aws_session_token", ""))
        s.Add(self.aws_token, (row, 1), span=(1, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="AWS Region:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.aws_region = wx.TextCtrl(panel, value=defaults.get("aws_s3_region", "us-east-1"))
        s.Add(self.aws_region, (row, 1), flag=wx.EXPAND)
        row += 1

        # Buckets
        s.Add(wx.StaticText(panel, label="Profile Bucket:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.bucket_profile = wx.TextCtrl(panel, value=defaults.get("aws_profile_bucket", ""))
        s.Add(self.bucket_profile, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Quality Bucket:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.bucket_quality = wx.TextCtrl(panel, value=defaults.get("aws_quality_bucket", ""))
        s.Add(self.bucket_quality, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Catalog Bucket:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.bucket_catalog = wx.TextCtrl(panel, value=defaults.get("aws_catalog_bucket", ""))
        s.Add(self.bucket_catalog, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Compliance Bucket:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.bucket_compliance = wx.TextCtrl(panel, value=defaults.get("aws_compliance_bucket", ""))
        s.Add(self.bucket_compliance, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Anomalies Bucket:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.bucket_anomalies = wx.TextCtrl(panel, value=defaults.get("aws_anomalies_bucket", ""))
        s.Add(self.bucket_anomalies, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Synthetic Data Bucket:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.bucket_synth = wx.TextCtrl(panel, value=defaults.get("aws_synthetic_bucket", ""))
        s.Add(self.bucket_synth, (row, 3), flag=wx.EXPAND)
        row += 1

        # ── Snowflake (properly indented inside __init__) ──────────────────────
        s.Add(wx.StaticText(panel, label="Snowflake Account:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.sf_account = wx.TextCtrl(panel, value=defaults.get("sf_account", ""))
        s.Add(self.sf_account, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Snowflake User:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.sf_user = wx.TextCtrl(panel, value=defaults.get("sf_user", ""))
        s.Add(self.sf_user, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Snowflake Password:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.sf_password = wx.TextCtrl(panel, value=defaults.get("sf_password", ""), style=wx.TE_PASSWORD)
        s.Add(self.sf_password, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Snowflake Role:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.sf_role = wx.TextCtrl(panel, value=defaults.get("sf_role", ""))
        s.Add(self.sf_role, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Warehouse:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.sf_warehouse = wx.TextCtrl(panel, value=defaults.get("sf_warehouse", ""))
        s.Add(self.sf_warehouse, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Database:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.sf_database = wx.TextCtrl(panel, value=defaults.get("sf_database", ""))
        s.Add(self.sf_database, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Schema:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.sf_schema = wx.TextCtrl(panel, value=defaults.get("sf_schema", ""))
        s.Add(self.sf_schema, (row, 1), flag=wx.EXPAND)

        btn_sf_test = wx.Button(panel, label="Test Snowflake Connection")
        s.Add(btn_sf_test, (row, 2), span=(1, 2), flag=wx.EXPAND)
        btn_sf_test.Bind(wx.EVT_BUTTON, self.on_test_snowflake)
        row += 1

        # Email
        s.Add(wx.StaticText(panel, label="SMTP Server:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.smtp_server = wx.TextCtrl(panel, value=defaults.get("smtp_server", ""))
        s.Add(self.smtp_server, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="SMTP Port:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.smtp_port = wx.TextCtrl(panel, value=defaults.get("smtp_port", ""))
        s.Add(self.smtp_port, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="Email Username:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.email_user = wx.TextCtrl(panel, value=defaults.get("email_username", ""))
        s.Add(self.email_user, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="Email Password:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.email_pass = wx.TextCtrl(panel, value=defaults.get("email_password", ""))
        s.Add(self.email_pass, (row, 3), flag=wx.EXPAND)
        row += 1

        s.Add(wx.StaticText(panel, label="From Email:"), (row, 0), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.from_email = wx.TextCtrl(panel, value=defaults.get("from_email", ""))
        s.Add(self.from_email, (row, 1), flag=wx.EXPAND)

        s.Add(wx.StaticText(panel, label="To Email:"), (row, 2), flag=wx.ALIGN_RIGHT | wx.ALIGN_CENTER_VERTICAL)
        self.to_email = wx.TextCtrl(panel, value=defaults.get("to_email", ""))
        s.Add(self.to_email, (row, 3), flag=wx.EXPAND)
        row += 1

        hint = wx.StaticText(
            panel,
            label=("Tip: switch Provider to automatically load URL/API key/models from your stored profiles "
                   "(Custom/OpenAI/Gemini).")
        )
        hint.Wrap(560)
        s.Add(hint, (row, 0), span=(1, 4), flag=wx.ALL | wx.EXPAND, border=6)
        row += 1

        save_btn = wx.Button(panel, label="Save")
        save_btn.Bind(wx.EVT_BUTTON, self.on_save)
        s.Add(save_btn, (row, 0), span=(1, 4), flag=wx.ALIGN_CENTER | wx.ALL, border=10)

        panel.SetSizer(s)

        # init choices + values
        self._refresh_model_choices()
        self._refresh_image_models()
        self._apply_provider_profile(PROVIDERS[self.provider.GetSelection()])
        self._select_choice(self.default_model, defaults.get("default_model"))
        self._select_choice(self.fast_model, defaults.get("fast_model"))

        self.Layout()

    # utils
    def _select_choice(self, choice_ctrl: wx.Choice, value):
        if not value:
            return
        items = [choice_ctrl.GetString(i) for i in range(choice_ctrl.GetCount())]
        if value in items:
            choice_ctrl.SetSelection(items.index(value))

    def _on_provider_change(self, _):
        provider = PROVIDERS[self.provider.GetSelection()]
        self._refresh_model_choices()
        self._apply_provider_profile(provider)

    def _on_image_provider_change(self, _):
        self._refresh_image_models()

    def _refresh_model_choices(self):
        provider = PROVIDERS[self.provider.GetSelection()]
        self.default_model.Clear()
        self.fast_model.Clear()

        if provider in ("custom", "auto"):
            for m in CUSTOM_MAIN:
                self.default_model.Append(m)
            for m in CUSTOM_FAST:
                self.fast_model.Append(m)
        elif provider == "openai":
            for m in OPENAI_MAIN:
                self.default_model.Append(m)
            for m in OPENAI_FAST:
                self.fast_model.Append(m)
        elif provider == "gemini":
            for m in GEMINI_MAIN:
                self.default_model.Append(m)
            for m in GEMINI_FAST:
                self.fast_model.Append(m)

        # fallback
        if self.default_model.GetCount() == 0:
            self.default_model.Append(defaults.get("default_model", "aldin-mini"))
        if self.fast_model.GetCount() == 0:
            self.fast_model.Append(defaults.get("fast_model", "aldin-mini"))

    def _refresh_image_models(self):
        prov = IMAGE_PROVIDERS[self.image_provider.GetSelection()]
        self.image_model.Clear()
        if prov in ("auto", "openai"):
            for m in OPENAI_IMAGE:
                self.image_model.Append(m)
        elif prov == "gemini":
            for m in GEMINI_IMAGE:
                self.image_model.Append(m)
        elif prov == "stability":
            for m in STABILITY_IMAGE:
                self.image_model.Append(m)
        else:
            self.image_model.Append(defaults.get("image_model", "gpt-image-1"))

        self._select_choice(self.image_model, defaults.get("image_model"))

    # Load stored profile values into the UI when provider changes
    def _apply_provider_profile(self, provider: str):
        keys = PROFILE_KEYS.get(provider, {})
        # URL
        url_key = keys.get("url")
        url_val = defaults.get(url_key, defaults.get("url", ""))
        self.chat_url.SetValue(url_val)

        # API key
        api_key_key = keys.get("api_key")
        api_val = defaults.get(api_key_key, defaults.get("api_key", ""))
        self.api_key.SetValue(api_val)

        # Org (OpenAI only)
        if provider == "openai":
            self.org.Enable(True)
            self.org.SetValue(defaults.get("openai_org", ""))
        else:
            self.org.Enable(False)

        # models
        def_sel = defaults.get(keys.get("default_model", ""), defaults.get("default_model"))
        fast_sel = defaults.get(keys.get("fast_model", ""), defaults.get("fast_model"))
        self._select_choice(self.default_model, def_sel)
        self._select_choice(self.fast_model, fast_sel)

    def on_test_snowflake(self, _evt=None):
        try:
            import snowflake.connector
            conn = snowflake.connector.connect(
                account=self.sf_account.GetValue().strip(),
                user=self.sf_user.GetValue().strip(),
                password=self.sf_password.GetValue(),
                role=self.sf_role.GetValue().strip() or None,
                warehouse=self.sf_warehouse.GetValue().strip() or None,
                database=self.sf_database.GetValue().strip() or None,
                schema=self.sf_schema.GetValue().strip() or None,
            )
            cur = conn.cursor()
            try:
                cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
                _ = cur.fetchone()
            finally:
                cur.close()
                conn.close()
            wx.MessageBox("Snowflake connection successful.", "Snowflake", wx.OK | wx.ICON_INFORMATION)
        except Exception as e:
            wx.MessageBox(f"Snowflake connection failed:\n{e}", "Snowflake", wx.OK | wx.ICON_ERROR)

    def on_save(self, _):
        # which provider is selected?
        provider = PROVIDERS[self.provider.GetSelection()]
        defaults["provider"] = provider

        # write ACTIVE top-level
        defaults["url"] = self.chat_url.GetValue().strip()
        defaults["api_key"] = self.api_key.GetValue().strip()
        defaults["openai_org"] = self.org.GetValue().strip()
        defaults["default_model"] = self.default_model.GetStringSelection() or self.default_model.GetString(0)
        defaults["fast_model"] = self.fast_model.GetStringSelection() or self.fast_model.GetString(0)
        defaults["max_tokens"] = self.max_tokens.GetValue().strip()
        defaults["temperature"] = self.temperature.GetValue().strip()
        defaults["top_p"] = self.top_p.GetValue().strip()
        defaults["frequency_penalty"] = self.freq_pen.GetValue().strip()
        defaults["presence_penalty"] = self.pres_pen.GetValue().strip()

        # also persist back into the selected provider profile
        keys = PROFILE_KEYS.get(provider, {})
        if "url" in keys:
            defaults[keys["url"]] = defaults["url"]
        if "api_key" in keys:
            defaults[keys["api_key"]] = defaults["api_key"]
        if "default_model" in keys:
            defaults[keys["default_model"]] = defaults["default_model"]
        if "fast_model" in keys:
            defaults[keys["fast_model"]] = defaults["fast_model"]
        if provider == "openai":
            defaults["openai_org"] = self.org.GetValue().strip()

        # images
        defaults["image_provider"] = IMAGE_PROVIDERS[self.image_provider.GetSelection()]
        defaults["image_model"] = self.image_model.GetStringSelection() or self.image_model.GetString(0)
        defaults["image_generation_url"] = self.image_url.GetValue().strip()
        defaults["stability_api_key"] = self.stability.GetValue().strip()

        # TTS
        defaults["azure_tts_key"] = self.azure_key.GetValue().strip()
        defaults["azure_tts_region"] = self.azure_region.GetValue().strip()

        # AWS
        defaults["aws_access_key_id"] = self.aws_key.GetValue().strip()
        defaults["aws_secret_access_key"] = self.aws_secret.GetValue().strip()
        defaults["aws_session_token"] = self.aws_token.GetValue().strip()
        defaults["aws_s3_region"] = self.aws_region.GetValue().strip()
        defaults["aws_profile_bucket"] = self.bucket_profile.GetValue().strip()
        defaults["aws_quality_bucket"] = self.bucket_quality.GetValue().strip()
        defaults["aws_catalog_bucket"] = self.bucket_catalog.GetValue().strip()
        defaults["aws_compliance_bucket"] = self.bucket_compliance.GetValue().strip()
        defaults["aws_anomalies_bucket"] = self.bucket_anomalies.GetValue().strip()
        defaults["aws_synthetic_bucket"] = self.bucket_synth.GetValue().strip()

        # Snowflake
        defaults["sf_account"] = self.sf_account.GetValue().strip()
        defaults["sf_user"] = self.sf_user.GetValue().strip()
        defaults["sf_password"] = self.sf_password.GetValue()
        defaults["sf_role"] = self.sf_role.GetValue().strip()
        defaults["sf_warehouse"] = self.sf_warehouse.GetValue().strip()
        defaults["sf_database"] = self.sf_database.GetValue().strip()
        defaults["sf_schema"] = self.sf_schema.GetValue().strip()

        # Email
        defaults["smtp_server"] = self.smtp_server.GetValue().strip()
        defaults["smtp_port"] = self.smtp_port.GetValue().strip()
        defaults["email_username"] = self.email_user.GetValue().strip()
        defaults["email_password"] = self.email_pass.GetValue().strip()
        defaults["from_email"] = self.from_email.GetValue().strip()
        defaults["to_email"] = self.to_email.GetValue().strip()

        save_defaults()
        self.Close()
