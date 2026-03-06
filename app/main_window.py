# app/main_window.py
# Lavender UI + full functionality (MDM, Synthetic Data, Tasks, Knowledge Files)
# Catalog: SLA column, editable & persisted + catalog toolbar
# Rebranded to Data Wizard
#
# UPDATE (2026-02-20):
# - Added "Config File" button next to "Export"
# - Generates AWS pipeline configuration + source-to-target mappings driven from app inputs/grid
# - Outputs: pipeline_config.json, pipeline_params.json, field_mappings.json, field_mappings.csv, README.txt

import os
import re
import json
import random
import threading
import inspect
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from difflib import SequenceMatcher

# dbt bundle generator (optional)
try:
    from app.dbt_generator import generate_dbt_models
except Exception:
    generate_dbt_models = None
import requests
import wx
import wx.grid as gridlib

# ──────────────────────────────────────────────────────────────────────────────
# Premium UI Theme (centralized design tokens)
# ──────────────────────────────────────────────────────────────────────────────
THEME = {
    # Surfaces
    "BG": (242, 240, 248),          # app background (soft lavender off‑white)
    "HEADER": (53, 29, 102),        # deep lavender
    "PANEL": (248, 245, 255),       # panel surface
    "CARD": (255, 255, 255),        # card surface

    # Text
    "TEXT_PRIMARY": (35, 30, 55),
    "TEXT_MUTED": (110, 110, 130),

    # Lines / borders
    "BORDER": (225, 225, 235),

    # Accent
    "ACCENT": (110, 90, 180),
    "ACCENT_HOVER": (130, 110, 200),
}

def _C(name: str) -> wx.Colour:
    """Theme color helper."""
    r, g, b = THEME[name]
    return wx.Colour(r, g, b)


import pandas as pd

from app.settings import SettingsWindow
from app.dialogs import QualityRuleDialog, DataBuddyDialog, SyntheticDataDialog
from app.s3_utils import download_text_from_uri, upload_to_s3
from app.analysis import (
    detect_and_split_data,
    profile_analysis,
    quality_analysis,
    catalog_analysis,
    compliance_analysis,
)

# ──────────────────────────────────────────────────────────────────────────────
# Kernel
# ──────────────────────────────────────────────────────────────────────────────

class KernelManager:
    def __init__(self, app_name="Data Wizard"):
        self.lock = threading.Lock()
        self.dir = os.path.join(os.path.expanduser("~"), ".sidecar")
        os.makedirs(self.dir, exist_ok=True)
        self.path = os.path.join(self.dir, "kernel.json")
        os.environ["SIDECAR_KERNEL_PATH"] = self.path

        self.data = {
            "kernel_version": "1.0",
            "creator": "Salah Mokhayesh",
            "app": {
                "name": app_name,
                "modules": [
                    "Knowledge Files", "Load File", "Load from URI/S3",
                    "MDM", "Synthetic Data", "Rule Assignment",
                    "Profile", "Quality", "Detect Anomalies",
                    "Catalog", "Compliance", "Tasks",
                    "Export CSV", "Export TXT", "Upload to S3",
                    "Config File"
                ,
                    "Snowflake Bundle",
                    "Fabric Bundle",
                    "Purview Export",
                    "DBT",
                    "dbt Bundle"]
            },
            "stats": {"launch_count": 0},
            "state": {"last_dataset": None, "kpis": {}, "catalog_meta": {}},
            "events": []
        }
        self._load_or_init()
        self.increment_launch()

    def _load_or_init(self):
        try:
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                existing.setdefault("kernel_version", "1.0")
                existing.setdefault("creator", "Salah Mokhayesh")
                existing.setdefault("app", self.data["app"])
                existing.setdefault("stats", {"launch_count": 0})
                existing.setdefault("state", {"last_dataset": None, "kpis": {}, "catalog_meta": {}})
                existing.setdefault("events", [])
                self.data = existing
            else:
                self._save()
        except Exception:
            self._save()

    def _save(self):
        with self.lock:
            try:
                ev = self.data.get("events", [])
                if len(ev) > 5000:
                    self.data["events"] = ev[-5000:]
                with open(self.path, "w", encoding="utf-8") as f:
                    json.dump(self.data, f, ensure_ascii=False, indent=2)
            except Exception:
                pass

    def increment_launch(self):
        with self.lock:
            self.data["stats"]["launch_count"] = int(self.data["stats"].get("launch_count", 0)) + 1
        self._save()

    def log(self, event_type, **payload):
        evt = {"ts": datetime.utcnow().isoformat() + "Z", "type": event_type, "payload": payload}
        with self.lock:
            self.data.setdefault("events", []).append(evt)
        self._save()

    def set_last_dataset(self, columns, rows_count):
        with self.lock:
            self.data["state"]["last_dataset"] = {
                "rows": int(rows_count),
                "cols": int(len(columns or [])),
                "columns": list(columns or []),
            }
        self._save()

    def set_kpis(self, kpi_dict):
        with self.lock:
            self.data["state"]["kpis"] = dict(kpi_dict or {})
        self._save()


# ──────────────────────────────────────────────────────────────────────────────
# Custom controls (buttons, badges, pill)
# ──────────────────────────────────────────────────────────────────────────────

class RoundedShadowButton(wx.Control):
    def __init__(self, parent, label, handler, colour=wx.Colour(115, 102, 192), radius=12):
        super().__init__(parent, style=wx.BORDER_NONE)
        self._label = label
        self._handler = handler
        self._colour = colour
        self._radius = radius
        self._hover = False
        self._down = False
        self.SetBackgroundStyle(wx.BG_STYLE_PAINT)
        self.Bind(wx.EVT_ERASE_BACKGROUND, lambda e: None)
        self.Bind(wx.EVT_PAINT, self.on_paint)
        self.Bind(wx.EVT_ENTER_WINDOW, lambda e: self._set_hover(True))
        self.Bind(wx.EVT_LEAVE_WINDOW, lambda e: self._set_hover(False))
        self.Bind(wx.EVT_LEFT_DOWN, self.on_down)
        self.Bind(wx.EVT_LEFT_UP, self.on_up)
        self._font = wx.Font(9, wx.FONTFAMILY_SWISS, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL)
        self._padx, self._pady = 16, 8

    def DoGetBestSize(self):
        dc = wx.ClientDC(self)
        dc.SetFont(self._font)
        tw, th = dc.GetTextExtent(self._label)
        return wx.Size(tw + self._padx * 2, th + self._pady * 2)

    def _set_hover(self, v):
        self._hover = v
        self.Refresh()

    def on_down(self, _):
        self._down = True
        self.CaptureMouse()
        self.Refresh()

    def _invoke(self, evt):
        try:
            sig = inspect.signature(self._handler)
            if len(sig.parameters) == 0:
                self._handler()
            else:
                self._handler(evt)
        except Exception as e:
            import traceback
            wx.MessageBox(f"{self._label} failed:\n\n{e}\n\n{traceback.format_exc()}",
                          "Action error", wx.OK | wx.ICON_ERROR)

    def on_up(self, evt):
        if self.HasCapture():
            self.ReleaseMouse()
        was_down = self._down
        self._down = False
        self.Refresh()
        if was_down and self.GetClientRect().Contains(evt.GetPosition()):
            self._invoke(evt)

    def on_paint(self, _):
        dc = wx.AutoBufferedPaintDC(self)
        w, h = self.GetClientSize()
        bg = self.GetParent().GetBackgroundColour()
        dc.SetBrush(wx.Brush(bg)); dc.SetPen(wx.Pen(bg)); dc.DrawRectangle(0, 0, w, h)

        base = self._colour
        if self._hover:
            base = wx.Colour(min(255, base.Red()+10), min(255, base.Green()+10), min(255, base.Blue()+10))
        if self._down:
            base = wx.Colour(max(0, base.Red()-20), max(0, base.Green()-20), max(0, base.Blue()-20))

        # shadow
        dc.SetBrush(wx.Brush(wx.Colour(0,0,0,60))); dc.SetPen(wx.Pen(wx.Colour(0,0,0,0)))
        dc.DrawRoundedRectangle(2, 3, w-4, h-3, self._radius+1)

        # pill
        dc.SetBrush(wx.Brush(base)); dc.SetPen(wx.Pen(base))
        dc.DrawRoundedRectangle(0, 0, w-2, h-2, self._radius)

        dc.SetTextForeground(wx.Colour(245,245,245))
        dc.SetFont(self._font)
        tw, th = dc.GetTextExtent(self._label)
        dc.DrawText(self._label, (w-tw)//2, (h-th)//2)

class LittleBuddyPill(wx.Control):
    def __init__(self, parent, label="Little Buddy", handler=None):
        super().__init__(parent, style=wx.BORDER_NONE)
        self._label = label; self._handler = handler
        self._hover = False; self._down = False
        self._h = 40; self.SetMinSize((150, self._h))
        self.SetBackgroundStyle(wx.BG_STYLE_PAINT)
        self.Bind(wx.EVT_ERASE_BACKGROUND, lambda e: None)
        self.Bind(wx.EVT_PAINT, self.on_paint)
        self.Bind(wx.EVT_ENTER_WINDOW, lambda e: self._set_hover(True))
        self.Bind(wx.EVT_LEAVE_WINDOW, lambda e: self._set_hover(False))
        self.Bind(wx.EVT_LEFT_DOWN, self.on_down)
        self.Bind(wx.EVT_LEFT_UP, self.on_up)
        self._font = wx.Font(9, wx.FONTFAMILY_SWISS, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD)

    def _set_hover(self, v): self._hover = v; self.Refresh()
    def on_down(self, _): self._down = True; self.CaptureMouse(); self.Refresh()

    def on_up(self, evt):
        if self.HasCapture(): self.ReleaseMouse()
        was = self._down; self._down = False; self.Refresh()
        if was and self.GetClientRect().Contains(evt.GetPosition()) and callable(self._handler):
            self._handler(evt)

    def on_paint(self, _evt):
        dc = wx.AutoBufferedPaintDC(self)
        w, h = self.GetClientSize()
        dc.SetBackground(wx.Brush(self.GetParent().GetBackgroundColour()))
        dc.Clear()

        gc = wx.GraphicsContext.Create(dc)
        base1 = wx.Colour(132, 86, 255); base2 = wx.Colour(108, 66, 238)
        if self._hover: base1, base2 = wx.Colour(150,104,255), wx.Colour(126,84,242)
        if self._down:  base1, base2 = wx.Colour(112,76,236),  wx.Colour(92,54,220)

        r = (h-6)//2
        gc.SetPen(wx.NullPen)
        gc.SetBrush(gc.CreateLinearGradientBrush(0,0,0,h, base1, base2))
        gc.DrawRoundedRectangle(0,0,w,h,r)

        gc.SetFont(self._font, wx.Colour(255,255,255))
        tw, th = gc.GetTextExtent(self._label)
        gc.DrawText(self._label, 14, (h-th)//2)

class KPIBadge(wx.Panel):
    def __init__(self, parent, title, init_value="—"):
        super().__init__(parent)
        self.SetMinSize((120, 88))
        self._title = title; self._value = init_value
        self.SetBackgroundStyle(wx.BG_STYLE_PAINT)
        self.Bind(wx.EVT_ERASE_BACKGROUND, lambda e: None)
        self.Bind(wx.EVT_PAINT, self.on_paint)

    def SetValue(self, v): self._value = v; self.Refresh()

    def on_paint(self, _):
        dc = wx.AutoBufferedPaintDC(self)
        w,h = self.GetClientSize()
        c1 = wx.Colour(247, 243, 255); c2 = wx.Colour(233, 225, 255)
        dc.GradientFillLinear(wx.Rect(0,0,w,h), c1, c2, wx.SOUTH)
        dc.SetPen(wx.Pen(wx.Colour(200,190,245))); dc.SetBrush(wx.TRANSPARENT_BRUSH)
        dc.DrawRoundedRectangle(1,1,w-2,h-2,8)

        dc.SetTextForeground(wx.Colour(94, 64, 150))
        dc.SetFont(wx.Font(8, wx.FONTFAMILY_SWISS, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL))
        dc.DrawText(self._title.upper(), 12, 10)

        dc.SetTextForeground(wx.Colour(44, 31, 72))
        dc.SetFont(wx.Font(13, wx.FONTFAMILY_SWISS, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD))
        dc.DrawText(str(self._value), 12, 34)


# ──────────────────────────────────────────────────────────────────────────────
# Main Window
# ──────────────────────────────────────────────────────────────────────────────

class MainWindow(wx.Frame):
    def __init__(self):
        super().__init__(None, title="Data Wizard", size=(1320, 840))

        # icon (best effort)
        for p in (
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "sidecar-01.ico"),
            os.path.join(os.getcwd(), "assets", "sidecar-01.ico"),
            os.path.join(os.getcwd(), "sidecar-01.ico"),
        ):
            if os.path.exists(p):
                try:
                    self.SetIcon(wx.Icon(p, wx.BITMAP_TYPE_ICO)); break
                except Exception:
                    pass

        self.kernel = KernelManager()
        self.kernel.log("app_started", version=self.kernel.data["kernel_version"])

        self.headers = []
        self.raw_data = []
        self.knowledge_files = []
        self.quality_rules = {}
        self.current_process = ""


        self._undo_stack = []
        self._redo_stack = []
        self.metrics = {
            "rows": None, "cols": None, "null_pct": None, "uniqueness": None,
            "dq_score": None, "validity": None, "completeness": None, "anomalies": None,
        }

        self._build_ui()
        self._ensure_kernel_in_knowledge()
        self.CenterOnScreen()
        self.Show()

    # UI
    def _build_ui(self):
        BG = _C("BG")
        HEADER = _C("HEADER")
        PANEL = _C("PANEL")
        TEXT_PRIMARY = _C("TEXT_PRIMARY")
        TEXT_MUTED = _C("TEXT_MUTED")
        BORDER = _C("BORDER")

        self.SetBackgroundColour(BG)
        main = wx.BoxSizer(wx.VERTICAL)

        # Header bar: title (left) + Little Buddy (right)
        header = wx.Panel(self); header.SetBackgroundColour(HEADER)
        hbox = wx.BoxSizer(wx.HORIZONTAL)

        title = wx.StaticText(header, label="Data Wizard")
        title.SetForegroundColour(wx.Colour(255, 255, 255))
        title.SetFont(wx.Font(12, wx.FONTFAMILY_SWISS, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD))
        hbox.Add(title, 0, wx.ALL | wx.ALIGN_CENTER_VERTICAL, 12)

        hbox.AddStretchSpacer(1)

        # Connection status (best-effort indicators)
        self._conn_labels = {}
        for key in ("AWS", "Snowflake", "dbt", "Fabric"):
            st = wx.StaticText(header, label=f"{key}: —")
            st.SetForegroundColour(wx.Colour(230, 225, 255))
            f = st.GetFont(); f.SetPointSize(8); st.SetFont(f)
            hbox.Add(st, 0, wx.RIGHT | wx.ALIGN_CENTER_VERTICAL, 10)
            self._conn_labels[key] = st

        self.little_pill = LittleBuddyPill(header, handler=getattr(self, 'on_little_buddy', lambda e: wx.MessageBox('Little Buddy handler missing (on_little_buddy).','Little Buddy', wx.OK|wx.ICON_ERROR)))
        hbox.Add(self.little_pill, 0, wx.ALL | wx.ALIGN_CENTER_VERTICAL, 8)

        header.SetSizer(hbox)
        main.Add(header, 0, wx.EXPAND)

        # Buttons toolbar (premium groups-as-cards with horizontal scroll)
        toolbar_scroller = wx.ScrolledWindow(self, style=wx.HSCROLL)
        toolbar_scroller.SetScrollRate(10, 0)
        toolbar_scroller.SetBackgroundColour(BG)

        toolbar_panel = wx.Panel(toolbar_scroller)
        toolbar_panel.SetBackgroundColour(BG)

        tb_outer = wx.BoxSizer(wx.HORIZONTAL)

        def _group(title: str, min_w: int, tint=None, proportion: int = 0, expand: bool = False):
            """Create a labeled 'card' section for toolbar buttons."""
            gp = wx.Panel(toolbar_panel, style=wx.BORDER_SIMPLE)
            gp.SetMinSize((min_w, -1))
            gp.SetBackgroundColour(_C("CARD") if tint is None else tint)

            v = wx.BoxSizer(wx.VERTICAL)

            lbl = wx.StaticText(gp, label=title)
            lf = lbl.GetFont()
            lf.SetPointSize(8)
            lf.SetWeight(wx.FONTWEIGHT_BOLD)
            lbl.SetFont(lf)
            lbl.SetForegroundColour(TEXT_MUTED)

            v.Add(lbl, 0, wx.LEFT | wx.TOP | wx.RIGHT, 10)
            v.Add(wx.StaticLine(gp), 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 6)

            # Wrap within the group (buttons wrap only inside the group if needed)
            s = wx.WrapSizer(wx.HORIZONTAL)
            v.Add(s, 0, wx.EXPAND | wx.ALL, 6)

            gp.SetSizer(v)
            tb_outer.Add(gp, proportion, wx.ALL | wx.ALIGN_TOP | (wx.EXPAND if expand else 0), 8)
            return s

        def _vsep():
            line = wx.StaticLine(toolbar_panel, style=wx.LI_VERTICAL)
            tb_outer.Add(line, 0, wx.EXPAND | wx.TOP | wx.BOTTOM, 10)

        def add_btn(sizer, label, handler):
            # IMPORTANT: parent must match the window that owns the sizer,
            # otherwise wx will assert: CheckExpectedParentIs(...)
            parent_win = None
            try:
                parent_win = sizer.GetContainingWindow()
            except Exception:
                parent_win = None
            if parent_win is None:
                parent_win = toolbar_panel
            b = RoundedShadowButton(parent_win, label, handler)
            sizer.Add(b, 0, wx.ALL, 6)
            return b

        # Subtle tints per section (still on-brand)
        tint_data    = wx.Colour(252, 250, 255)
        tint_analyze = wx.Colour(250, 250, 255)
        tint_actions = wx.Colour(252, 248, 255)

        # Group 1: Data Sources
        g_data = _group("DATA SOURCES", 240, tint=tint_data)
        add_btn(g_data, "Connect", self.on_upload_menu)

        _vsep()

        # Group 2: Analyze
        g_analyze = _group("ANALYZE", 470, tint=tint_analyze)
        add_btn(g_analyze, "Profile", lambda e: self.do_analysis_process("Profile"))
        add_btn(g_analyze, "Quality", lambda e: self.do_analysis_process("Quality"))
        add_btn(g_analyze, "Catalog", lambda e: self.do_analysis_process("Catalog"))
        add_btn(g_analyze, "Compliance", lambda e: self.do_analysis_process("Compliance"))
        add_btn(g_analyze, "Anomalies", lambda e: self.do_analysis_process("Detect Anomalies"))

        _vsep()

        # Group 3: Actions
        g_actions = _group("ACTIONS", 610, tint=tint_actions)
        add_btn(g_actions, "Rule Assignment", self.on_rules)
        add_btn(g_actions, "Transform", self.on_transform_menu)
        add_btn(g_actions, "Undo", self.on_undo)
        add_btn(g_actions, "Redo", self.on_redo)
        add_btn(g_actions, "Knowledge Files", self.on_load_knowledge)
        add_btn(g_actions, "MDM", self.on_mdm)
        add_btn(g_actions, "Synthetic Data", self.on_generate_synth)
        add_btn(g_actions, "To Do", self.on_run_tasks)
        add_btn(g_actions, "DBT", self.on_dbt_menu)

        _vsep()

        # Group 4: Deploy
        g_deploy = _group("DEPLOY", 700, tint=tint_actions)
        add_btn(g_deploy, "Export", self.on_export_menu)
        add_btn(g_deploy, "AWS Glue Bundle", self.on_generate_config_files)
        add_btn(g_deploy, "Snowflake Bundle", self.on_generate_snowflake_bundle)
        add_btn(g_deploy, "Fabric Bundle", self.on_generate_fabric_bundle)
        add_btn(g_deploy, "Purview Export", self.on_purview_export)
        add_btn(g_deploy, "dbt Bundle", self.on_generate_dbt_bundle)

        tb_outer.AddStretchSpacer(1)
        toolbar_panel.SetSizer(tb_outer)

        # Fit scroller to content width (single-row feel; scrolls if too narrow)
        toolbar_panel.Layout()
        toolbar_scroller.SetVirtualSize(toolbar_panel.GetBestSize())
        toolbar_scroller.SetSizer(wx.BoxSizer(wx.VERTICAL))
        toolbar_scroller.GetSizer().Add(toolbar_panel, 0, wx.EXPAND | wx.ALL, 8)
        toolbar_scroller.FitInside()

        # KPI bar
        kpi_panel = wx.Panel(self); kpi_panel.SetBackgroundColour(BG)
        krow = wx.BoxSizer(wx.HORIZONTAL)
        self.card_rows     = KPIBadge(kpi_panel, "Rows")
        self.card_cols     = KPIBadge(kpi_panel, "Columns")
        self.card_nulls    = KPIBadge(kpi_panel, "Null %")
        self.card_unique   = KPIBadge(kpi_panel, "Uniqueness")
        self.card_quality  = KPIBadge(kpi_panel, "DQ Score")
        self.card_validity = KPIBadge(kpi_panel, "Validity")
        self.card_complete = KPIBadge(kpi_panel, "Completeness")
        self.card_anoms    = KPIBadge(kpi_panel, "Anomalies")
        for c in (self.card_rows, self.card_cols, self.card_nulls, self.card_unique,
                  self.card_quality, self.card_validity, self.card_complete, self.card_anoms):
            krow.Add(c, 1, wx.ALL | wx.EXPAND, 6)
        kpi_panel.SetSizer(krow)

        # Order: KPIs first, toolbar second
        main.Add(kpi_panel, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 6)
        main.Add(toolbar_scroller, 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 6)

        # Knowledge files strip
        info_panel = wx.Panel(self); info_panel.SetBackgroundColour(wx.Colour(243, 239, 255))
        hz = wx.BoxSizer(wx.HORIZONTAL)
        lab = wx.StaticText(info_panel, label="Knowledge Files:")
        lab.SetForegroundColour(wx.Colour(44,31,72))
        self.knowledge_lbl = wx.StaticText(info_panel, label="(none)")
        self.knowledge_lbl.SetForegroundColour(wx.Colour(94,64,150))
        hz.Add(lab, 0, wx.ALL | wx.ALIGN_CENTER_VERTICAL, 6)
        hz.Add(self.knowledge_lbl, 0, wx.ALL | wx.ALIGN_CENTER_VERTICAL, 6)
        hz.AddStretchSpacer()
        info_panel.SetSizer(hz)
        main.Add(info_panel, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 6)

        # Catalog toolbar (hidden unless Catalog is active)
        self.catalog_toolbar_panel = wx.Panel(self)
        self.catalog_toolbar_panel.SetBackgroundColour(wx.Colour(243, 239, 255))
        ct = wx.BoxSizer(wx.HORIZONTAL)
        self.btn_catalog_save  = RoundedShadowButton(self.catalog_toolbar_panel, "Save Catalog Edits", self.on_catalog_save)
        self.btn_catalog_reset = RoundedShadowButton(self.catalog_toolbar_panel, "Reset Catalog Edits", self.on_catalog_reset, colour=wx.Colour(160, 120, 200))
        ct.Add(self.btn_catalog_save, 0, wx.ALL, 6)
        ct.Add(self.btn_catalog_reset, 0, wx.ALL, 6)
        ct.AddStretchSpacer(1)
        self.catalog_toolbar_panel.SetSizer(ct)
        self.catalog_toolbar_panel.Hide()
        main.Add(self.catalog_toolbar_panel, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 4)

        # Main content: Grid (top) + Wizard Console (bottom)
        splitter = wx.SplitterWindow(self, style=wx.SP_LIVE_UPDATE)
        splitter.SetMinimumPaneSize(140)

        # Top pane: data grid
        top_panel = wx.Panel(splitter)
        top_panel.SetBackgroundColour(BG)
        self.grid = gridlib.Grid(top_panel); self.grid.CreateGrid(0, 0)
        self.grid.SetDefaultCellTextColour(wx.Colour(35, 31, 51))
        self.grid.SetDefaultCellBackgroundColour(wx.Colour(255,255,255))
        self.grid.SetLabelTextColour(wx.Colour(60,60,90))
        self.grid.SetLabelBackgroundColour(wx.Colour(235,231,250))
        self.grid.SetGridLineColour(wx.Colour(220,214,245))
        self.grid.EnableEditing(False)
        self.grid.SetRowLabelSize(36); self.grid.SetColLabelSize(28)
        self.grid.Bind(wx.EVT_SIZE, self.on_grid_resize)
        self.grid.Bind(gridlib.EVT_GRID_CELL_CHANGED, self.on_cell_changed)

        tp = wx.BoxSizer(wx.VERTICAL)
        tp.Add(self.grid, 1, wx.EXPAND | wx.ALL, 8)
        top_panel.SetSizer(tp)

        # Bottom pane: Wizard Console (logs/issues/summary + progress)
        bottom_panel = wx.Panel(splitter)
        bottom_panel.SetBackgroundColour(wx.Colour(252, 250, 255))

        bp = wx.BoxSizer(wx.VERTICAL)

        console_hdr = wx.BoxSizer(wx.HORIZONTAL)
        lbl_console = wx.StaticText(bottom_panel, label="Wizard Console")
        lf = lbl_console.GetFont()
        lf.SetPointSize(9)
        lf.SetWeight(wx.FONTWEIGHT_BOLD)
        lbl_console.SetFont(lf)
        lbl_console.SetForegroundColour(wx.Colour(44, 31, 72))
        console_hdr.Add(lbl_console, 0, wx.ALL | wx.ALIGN_CENTER_VERTICAL, 6)
        console_hdr.AddStretchSpacer(1)

        self.progress_label = wx.StaticText(bottom_panel, label="")
        self.progress_label.SetForegroundColour(wx.Colour(94, 64, 150))
        self.lbl_console_status = self.progress_label  # backward-compatible alias
        console_hdr.Add(self.progress_label, 0, wx.RIGHT | wx.ALIGN_CENTER_VERTICAL, 8)

        self.progress_gauge = wx.Gauge(bottom_panel, range=100, size=(220, 12), style=wx.GA_HORIZONTAL)
        self.progress_gauge.SetValue(0)
        console_hdr.Add(self.progress_gauge, 0, wx.RIGHT | wx.ALIGN_CENTER_VERTICAL, 8)

        bp.Add(console_hdr, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 4)
        bp.Add(wx.StaticLine(bottom_panel), 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.BOTTOM, 4)

        nb = wx.Notebook(bottom_panel)

        # Logs tab
        p_logs = wx.Panel(nb)
        v_logs = wx.BoxSizer(wx.VERTICAL)
        self.txt_logs = wx.TextCtrl(p_logs, value="", style=wx.TE_MULTILINE | wx.TE_READONLY | wx.HSCROLL)
        self.txt_console = self.txt_logs  # backward-compatible alias
        v_logs.Add(self.txt_logs, 1, wx.EXPAND | wx.ALL, 8)
        p_logs.SetSizer(v_logs)
        nb.AddPage(p_logs, "Logs")

        # Issues tab
        p_issues = wx.Panel(nb)
        v_issues = wx.BoxSizer(wx.VERTICAL)
        self.txt_issues = wx.TextCtrl(p_issues, value="No issues yet.", style=wx.TE_MULTILINE | wx.TE_READONLY | wx.HSCROLL)
        v_issues.Add(self.txt_issues, 1, wx.EXPAND | wx.ALL, 8)
        p_issues.SetSizer(v_issues)
        nb.AddPage(p_issues, "Issues")

        # Summary tab
        p_summary = wx.Panel(nb)
        v_sum = wx.BoxSizer(wx.VERTICAL)
        self.txt_summary = wx.TextCtrl(p_summary, value="Load a dataset to see summary.", style=wx.TE_MULTILINE | wx.TE_READONLY | wx.HSCROLL)
        v_sum.Add(self.txt_summary, 1, wx.EXPAND | wx.ALL, 8)
        p_summary.SetSizer(v_sum)
        nb.AddPage(p_summary, "Summary")

        bp.Add(nb, 1, wx.EXPAND | wx.ALL, 6)
        bottom_panel.SetSizer(bp)

        splitter.SplitHorizontally(top_panel, bottom_panel, sashPosition=520)
        main.Add(splitter, 1, wx.EXPAND | wx.ALL, 4)

        # Menubar
        mb = wx.MenuBar()
        m_file = wx.Menu(); m_file.Append(wx.ID_EXIT, "&Quit\tCtrl+Q"); mb.Append(m_file, "&File")
        self.Bind(wx.EVT_MENU, lambda e: self.Close(), id=wx.ID_EXIT)

        m_settings = wx.Menu(); OPEN_SETTINGS_ID = wx.NewIdRef()
        m_settings.Append(OPEN_SETTINGS_ID, "&Preferences...\tCtrl+,"); mb.Append(m_settings, "&Settings")
        self.Bind(wx.EVT_MENU, self.open_settings, id=OPEN_SETTINGS_ID)
        self.SetMenuBar(mb)

        self.SetSizer(main)

        # Periodic connection checks (non-blocking)
        self._conn_state = {}
        self._conn_timer = wx.Timer(self)
        self.Bind(wx.EVT_TIMER, self._on_conn_timer, self._conn_timer)
        self._conn_timer.Start(5000)
        wx.CallAfter(self.refresh_connection_status)

    # Knowledge helpers
    def _get_prioritized_knowledge(self):
        paths = []
        if self.kernel and os.path.exists(self.kernel.path):
            paths.append(self.kernel.path)
        for p in self.knowledge_files:
            if p != self.kernel.path:
                paths.append(p)
        return paths

    def _update_knowledge_label_and_env(self):
        names = ", ".join(os.path.basename(p) for p in self._get_prioritized_knowledge()) or "(none)"
        self.knowledge_lbl.SetLabel(names)
        prio = self._get_prioritized_knowledge()
        os.environ["SIDECAR_KNOWLEDGE_FILES"] = os.pathsep.join(prio)
        os.environ["SIDECAR_KNOWLEDGE_FIRST"] = "1"
        os.environ["SIDECAR_KERNEL_FIRST"] = "1"

    def _ensure_kernel_in_knowledge(self):
        try:
            if self.kernel and os.path.exists(self.kernel.path):
                if self.kernel.path not in self.knowledge_files:
                    self.knowledge_files.append(self.kernel.path)
                self._update_knowledge_label_and_env()
                self.kernel.log("kernel_loaded_as_knowledge", path=self.kernel.path)
        except Exception:
            pass

    # KPI
    def _reset_kpis_for_new_dataset(self, hdr, data):
        self.metrics.update({
            "rows": len(data), "cols": len(hdr),
            "null_pct": None, "uniqueness": None, "dq_score": None,
            "validity": None, "completeness": None, "anomalies": None,
        })
        self._render_kpis()
        self.kernel.set_last_dataset(columns=hdr, rows_count=len(data))
        self.kernel.log("dataset_loaded", rows=len(data), cols=len(hdr))

    def _render_kpis(self):
        self.card_rows.SetValue(self.metrics["rows"] if self.metrics["rows"] is not None else "—")
        self.card_cols.SetValue(self.metrics["cols"] if self.metrics["cols"] is not None else "—")
        self.card_nulls.SetValue(f"{self.metrics['null_pct']:.1f}%" if self.metrics["null_pct"] is not None else "—")
        self.card_unique.SetValue(f"{self.metrics['uniqueness']:.1f}%" if self.metrics["uniqueness"] is not None else "—")
        self.card_quality.SetValue(f"{self.metrics['dq_score']:.1f}" if self.metrics["dq_score"] is not None else "—")
        self.card_validity.SetValue(f"{self.metrics['validity']:.1f}%" if self.metrics["validity"] is not None else "—")
        self.card_complete.SetValue(f"{self.metrics['completeness']:.1f}%" if self.metrics["completeness"] is not None else "—")
        self.card_anoms.SetValue(str(self.metrics["anomalies"]) if self.metrics["anomalies"] is not None else "—")
        self.kernel.set_kpis(self.metrics)


    # ──────────────────────────────────────────────────────────────────────────
    # Wizard Console (right pane)
    # ──────────────────────────────────────────────────────────────────────────
    def _console_ts(self) -> str:
        try:
            return datetime.now().strftime("%H:%M:%S")
        except Exception:
            return ""

    def console_log(self, message: str):
        """Append a line to the Wizard Console log (safe to call from anywhere)."""
        try:
            target = getattr(self, "txt_console", None) or getattr(self, "txt_logs", None)
            if target is None:
                return
            line = f"[{self._console_ts()}] {message}\n"
            target.AppendText(line)
        except Exception:
            pass

    def console_set_status(self, message: str):
        try:
            target = getattr(self, "lbl_console_status", None) or getattr(self, "progress_label", None)
            if target:
                target.SetLabel(message)
        except Exception:
            pass

    def _console_log(self, message: str):
        """Backward-compatible wrapper for legacy calls."""
        self.console_log(message)
    # Utils
    @staticmethod
    def _as_df(rows, cols):
        df = pd.DataFrame(rows, columns=cols)
        return df.map(lambda x: None if (x is None or (isinstance(x, str) and x.strip() == "")) else x)

    def _compute_profile_metrics(self, df: pd.DataFrame):
        total_cells = df.shape[0] * max(1, df.shape[1])
        nulls = int(df.isna().sum().sum())
        null_pct = (nulls / total_cells) * 100.0 if total_cells else 0.0
        uniqs = []
        for c in df.columns:
            s = df[c].dropna()
            n = len(s)
            uniqs.append((s.nunique() / n * 100.0) if n else 0.0)
        uniq_pct = sum(uniqs) / len(uniqs) if uniqs else 0.0
        return null_pct, uniq_pct

    def _compile_rules(self):
        compiled = {}
        for k, v in (self.quality_rules or {}).items():
            if hasattr(v, "pattern"):
                compiled[k] = v
            else:
                try: compiled[k] = re.compile(str(v))
                except Exception: compiled[k] = re.compile(".*")
        return compiled

    def _compute_quality_metrics(self, df: pd.DataFrame):
        total_cells = df.shape[0] * max(1, df.shape[1])
        nulls = int(df.isna().sum().sum())
        completeness = (1.0 - (nulls / total_cells)) * 100.0 if total_cells else 0.0

        rules = self._compile_rules()
        checked = 0
        valid = 0

        # Regex rules can be sensitive to non-string types; normalize values safely.
        for col, rx in (rules or {}).items():
            if col not in df.columns:
                continue
            # ensure rx is compiled
            if not hasattr(rx, "search"):
                try:
                    rx = re.compile(str(rx))
                except Exception:
                    rx = re.compile(".*")

            for raw in df[col].values:
                checked += 1
                if raw is None:
                    s = ""
                else:
                    # treat NaN as empty
                    try:
                        import pandas as _pd
                        if isinstance(raw, float) and _pd.isna(raw):
                            s = ""
                        else:
                            s = str(raw)
                    except Exception:
                        s = str(raw)

                try:
                    if rx.fullmatch(s) or rx.search(s):
                        valid += 1
                except TypeError:
                    # last resort: cast again
                    try:
                        if rx.search(str(s)):
                            valid += 1
                    except Exception:
                        pass

        validity = (valid / checked) * 100.0 if checked else None

        if self.metrics["uniqueness"] is None or self.metrics["null_pct"] is None:
            null_pct, uniq_pct = self._compute_profile_metrics(df)
            self.metrics["null_pct"] = null_pct
            self.metrics["uniqueness"] = uniq_pct

        components = [self.metrics["uniqueness"], completeness]
        if validity is not None:
            components.append(validity)
        dq_score = sum(components) / len(components) if components else 0.0
        return completeness, validity, dq_score

    @staticmethod
    def _coerce_hdr_data(obj):
        if isinstance(obj, tuple) and len(obj) == 2:
            hdr, data = obj
            if isinstance(hdr, pd.DataFrame):
                df = hdr; return list(df.columns), df.values.tolist()
            if isinstance(hdr, (list, tuple)):
                return list(hdr), list(data)
        if isinstance(obj, pd.DataFrame):
            df = obj; return list(df.columns), df.values.tolist()
        return ["message"], [["Quality complete."]]

    # Knowledge & rules
    def on_load_knowledge(self, _evt=None):
        dlg = wx.FileDialog(self, "Load knowledge files", wildcard="Text|*.txt;*.csv;*.tsv|All|*.*",
                            style=wx.FD_OPEN | wx.FD_MULTIPLE | wx.FD_FILE_MUST_EXIST)
        if dlg.ShowModal() != wx.ID_OK: return
        files = dlg.GetPaths(); dlg.Destroy()

        new_list = []
        if self.kernel and os.path.exists(self.kernel.path):
            new_list.append(self.kernel.path)
        new_list.extend(files)
        seen = set()
        self.knowledge_files = [x for x in new_list if not (x in seen or seen.add(x))]
        self._update_knowledge_label_and_env()
        self.kernel.log("load_knowledge_files",
                        count=len(self._get_prioritized_knowledge()),
                        files=[os.path.basename(p) for p in self._get_prioritized_knowledge()])

    def _load_text_file(self, path): return open(path, "r", encoding="utf-8", errors="ignore").read()

    # Upload menu (File or URI/S3)
    def on_upload_menu(self, evt=None):
        menu = wx.Menu()
        from_file = menu.Append(wx.ID_ANY, "From File…")
        from_uri  = menu.Append(wx.ID_ANY, "From URI / S3…")
        self.Bind(wx.EVT_MENU, lambda e: self.on_load_file(), from_file)
        self.Bind(wx.EVT_MENU, lambda e: self.on_load_uri(),  from_uri)
        self.PopupMenu(menu)
        menu.Destroy()

    def on_load_uri(self, _evt=None):
        dlg = wx.TextEntryDialog(self, "Enter URI (supports http(s):// and s3:// when configured):",
                                 "Load from URI / S3")
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy(); return
        uri = dlg.GetValue().strip()
        dlg.Destroy()
        if not uri:
            return
        try:
            text = download_text_from_uri(uri)
            hdr, data = detect_and_split_data(text)
            self.headers, self.raw_data = hdr, data
            self._display(hdr, data)
            self._reset_kpis_for_new_dataset(hdr, data)
            self.kernel.log("load_uri", uri=uri, rows=len(data), cols=len(hdr))
            self.console_set_status("Dataset loaded")
            self.console_log(f"Loaded URI: {uri} (rows={len(data)}, cols={len(hdr)})")
            try:
                self.txt_summary.SetValue(f"Rows: {len(data)}\nColumns: {len(hdr)}\n\nFields:\n- " + "\n- ".join(map(str, hdr)))
            except Exception:
                pass
        except Exception as e:
            wx.MessageBox(f"Could not read from URI:\n{e}", "Load URI", wx.OK | wx.ICON_ERROR)

    # local file loader
    def on_load_file(self, _evt=None):
        dlg = wx.FileDialog(self, "Open data file", wildcard="Data|*.csv;*.tsv;*.txt|All|*.*",
                            style=wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        if dlg.ShowModal() != wx.ID_OK: return
        path = dlg.GetPath(); dlg.Destroy()
        try:
            text = self._load_text_file(path)
            hdr, data = detect_and_split_data(text)
        except Exception as e:
            wx.MessageBox(f"Could not read file: {e}", "Error", wx.OK | wx.ICON_ERROR); return
        self.headers, self.raw_data = hdr, data
        self._display(hdr, data); self._reset_kpis_for_new_dataset(hdr, data)
        self.kernel.log("load_file", path=path, rows=len(data), cols=len(hdr))
        self.console_set_status("Dataset loaded")
        self.console_log(f"Loaded file: {path} (rows={len(data)}, cols={len(hdr)})")
        try:
            self.txt_summary.SetValue(f"Rows: {len(data)}\nColumns: {len(hdr)}\n\nFields:\n- " + "\n- ".join(map(str, hdr)))
        except Exception:
            pass

    def on_rules(self, _evt=None):
        if not self.headers:
            wx.MessageBox("Load data first so fields are available.", "Quality Rules",
                          wx.OK | wx.ICON.WARNING); return
        try:
            dlg = QualityRuleDialog(self, list(self.headers), dict(self.quality_rules))
            if dlg.ShowModal() == wx.ID_OK:
                self.quality_rules = getattr(dlg, "current_rules", self.quality_rules)
                self.kernel.log("rules_updated", rules=self.quality_rules)
            dlg.Destroy()
        except Exception as e:
            wx.MessageBox(f"Could not open Quality Rule Assignment:\n{e}",
                          "Quality Rules", wx.OK | wx.ICON_ERROR)

    # ──────────────────────────────────────────────────────────────────────────
    # Transformations (NEW)
    # ──────────────────────────────────────────────────────────────────────────
    
    def _snapshot_dataset(self):
        """Return a lightweight snapshot of the current dataset for undo/redo."""
        return {
            "headers": list(self.headers or []),
            "raw_data": [list(r) for r in (self.raw_data or [])],
            "current_process": self.current_process,
            "metrics": dict(self.metrics or {}),
        }
    
    def _restore_dataset(self, snap: dict):
        self.headers = list(snap.get("headers") or [])
        self.raw_data = [list(r) for r in (snap.get("raw_data") or [])]
        self.current_process = snap.get("current_process") or ""
        # metrics will be recalculated/reset for safety
        self._display(self.headers, self.raw_data)
        self._reset_kpis_for_new_dataset(self.headers, self.raw_data)
        self._show_catalog_toolbar(self.current_process == "Catalog")
    
    def _push_undo(self, reason: str = ""):
        try:
            self._undo_stack.append(self._snapshot_dataset())
            # keep undo bounded
            if len(self._undo_stack) > 30:
                self._undo_stack = self._undo_stack[-30:]
            self._redo_stack = []
            if reason:
                self._console_log(f"Undo checkpoint: {reason}")
        except Exception:
            pass
    
    def on_undo(self, _evt=None):
        if not getattr(self, "_undo_stack", None):
            wx.MessageBox("Nothing to undo.", "Undo", wx.OK | wx.ICON_INFORMATION)
            return
        try:
            cur = self._snapshot_dataset()
            snap = self._undo_stack.pop()
            self._redo_stack.append(cur)
            self._restore_dataset(snap)
            self._console_log("Undo applied.")
        except Exception as e:
            wx.MessageBox(f"Undo failed: {e}", "Undo", wx.OK | wx.ICON_ERROR)
    
    def on_redo(self, _evt=None):
        if not getattr(self, "_redo_stack", None):
            wx.MessageBox("Nothing to redo.", "Redo", wx.OK | wx.ICON_INFORMATION)
            return
        try:
            cur = self._snapshot_dataset()
            snap = self._redo_stack.pop()
            self._undo_stack.append(cur)
            self._restore_dataset(snap)
            self._console_log("Redo applied.")
        except Exception as e:
            wx.MessageBox(f"Redo failed: {e}", "Redo", wx.OK | wx.ICON_ERROR)
    
    def on_transform_menu(self, _evt=None):
        if (not self.headers) and self.grid.GetNumberCols() == 0:
            wx.MessageBox("Load data first so there is something to transform.", "Transform",
                          wx.OK | wx.ICON_WARNING)
            return
    
        # Use current view columns
        cols = list(self.headers or [])
        if not cols and self.grid.GetNumberCols() > 0:
            cols = [self.grid.GetColLabelValue(i) for i in range(self.grid.GetNumberCols())]
    
        dlg = TransformDialog(self, columns=cols)
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy()
            return
        spec = dlg.get_params()
        dlg.Destroy()
    
        try:
            self._push_undo(reason=f"Transform: {spec.get('operation')}")
            self._apply_transformation(spec)
            self._console_log(f"Transform complete: {spec.get('operation')}")
        except Exception as e:
            import traceback
            wx.MessageBox(f"Transform failed:\n\n{e}\n\n{traceback.format_exc()}",
                          "Transform", wx.OK | wx.ICON_ERROR)
            self.kernel.log("transform_failed", error=str(e), spec=spec)
    
    def _apply_transformation(self, spec: dict):
        op = (spec.get("operation") or "").strip()
        df = pd.DataFrame(self.raw_data, columns=self.headers)
    
        def to_str(x):
            return "" if x is None else str(x)
    
        if op == "Trim whitespace (all text)":
            for c in df.columns:
                if df[c].dtype == object:
                    df[c] = df[c].map(lambda v: to_str(v).strip())
            self.kernel.log("transform_trim", cols=len(df.columns))
    
        elif op == "Normalize column names (snake_case)":
            new_cols = []
            for c in df.columns:
                s = str(c).strip()
                s = re.sub(r"\s+", "_", s)
                s = re.sub(r"[^A-Za-z0-9_]+", "_", s)
                s = re.sub(r"_+", "_", s).strip("_")
                new_cols.append(s or "col")
            df.columns = new_cols
            self.kernel.log("transform_cols_snake_case")
    
        elif op == "Drop duplicate rows":
            before = len(df)
            df = df.drop_duplicates()
            self.kernel.log("transform_drop_duplicates", before=before, after=len(df))
    
        elif op == "Fill nulls in column":
            col = spec.get("column") or ""
            fill_val = spec.get("value")
            if col not in df.columns:
                raise ValueError(f"Column not found: {col}")
            before_nulls = int(df[col].isna().sum())
            df[col] = df[col].where(~df[col].isna(), other=fill_val)
            self.kernel.log("transform_fill_nulls", column=col, before_nulls=before_nulls)
    
        elif op == "Find & replace (regex)":
            col = spec.get("column") or ""
            pattern = spec.get("pattern") or ""
            repl = spec.get("replacement") or ""
            if col not in df.columns:
                raise ValueError(f"Column not found: {col}")
            rx = re.compile(pattern)
            df[col] = df[col].map(lambda v: rx.sub(repl, to_str(v)))
            self.kernel.log("transform_regex_replace", column=col)
    
        elif op == "Cast column to numeric":
            col = spec.get("column") or ""
            if col not in df.columns:
                raise ValueError(f"Column not found: {col}")
            df[col] = pd.to_numeric(df[col], errors="coerce")
            self.kernel.log("transform_cast_numeric", column=col)
    
        elif op == "Parse column as date":
            col = spec.get("column") or ""
            fmt = (spec.get("date_format") or "").strip() or None
            if col not in df.columns:
                raise ValueError(f"Column not found: {col}")
            if fmt:
                df[col] = pd.to_datetime(df[col], errors="coerce", format=fmt).dt.date.astype(str)
            else:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date.astype(str)
            self.kernel.log("transform_parse_date", column=col, fmt=fmt or "auto")
    
        elif op == "Mask PII (email/phone)":
            cols = spec.get("columns") or []
            if not cols:
                cols = [c for c in df.columns if any(k in c.lower() for k in ["email","phone","mobile","cell"])]
            for col in cols:
                if col not in df.columns:
                    continue
                low = col.lower()
                if "email" in low:
                    def mask_email(v):
                        s = to_str(v).strip()
                        if "@" not in s: 
                            return s
                        name, dom = s.split("@", 1)
                        if len(name) <= 2:
                            m = "*" * len(name)
                        else:
                            m = name[0] + ("*" * (len(name)-2)) + name[-1]
                        return m + "@" + dom
                    df[col] = df[col].map(mask_email)
                else:
                    def mask_phone(v):
                        s = re.sub(r"\D+", "", to_str(v))
                        if len(s) < 4:
                            return to_str(v)
                        return "*" * max(0, len(s)-4) + s[-4:]
                    df[col] = df[col].map(mask_phone)
            self.kernel.log("transform_mask_pii", columns=cols)
    
        else:
            raise ValueError(f"Unsupported transformation: {op}")
    
        self.headers = list(df.columns)
        # keep as list-of-lists, stringify for grid stability
        self.raw_data = df.astype(object).where(pd.notna(df), None).values.tolist()
        self.current_process = "Transform"
        self._show_catalog_toolbar(False)
        self._display(self.headers, self.raw_data)
        self._reset_kpis_for_new_dataset(self.headers, self.raw_data)

    # Settings & Buddy
    def open_settings(self, _evt=None):
        try:
            dlg = SettingsWindow(self); self.kernel.log("open_settings")
            if hasattr(dlg, "ShowModal"):
                dlg.ShowModal(); dlg.Destroy()
            else: dlg.Show()
        except Exception as e:
            wx.MessageBox(f"Could not open Settings:\n{e}", "Settings", wx.OK | wx.ICON_ERROR)

    def on_little_buddy(self, _evt=None):
        try:
            dlg = DataBuddyDialog(self)
            prio = self._get_prioritized_knowledge()
            os.environ["SIDECAR_KNOWLEDGE_FILES"] = os.pathsep.join(prio)
            os.environ["SIDECAR_KNOWLEDGE_FIRST"] = "1"
            os.environ["SIDECAR_KERNEL_FIRST"] = "1"
            if hasattr(dlg, "set_kernel"): dlg.set_kernel(self.kernel)
            elif hasattr(dlg, "kernel"):   setattr(dlg, "kernel", self.kernel)
            elif hasattr(dlg, "kernel_path"): setattr(dlg, "kernel_path", self.kernel.path)
            if hasattr(dlg, "set_knowledge_files"): dlg.set_knowledge_files(list(prio))
            else:
                setattr(dlg, "knowledge_files", list(prio))
                setattr(dlg, "priority_sources", list(prio))
                setattr(dlg, "knowledge_first", True)
            self.kernel.log("little_buddy_opened",
                            kernel_path=self.kernel.path,
                            knowledge_files=[os.path.basename(p) for p in prio])
            dlg.ShowModal(); dlg.Destroy()
        except Exception as e:
            wx.MessageBox(f"Little Buddy failed to open:\n{e}", "Little Buddy", wx.OK | wx.ICON_ERROR)

    
    # ──────────────────────────────────────────────────────────────────────────
    # Connection status + progress helpers (NEW)
    # ──────────────────────────────────────────────────────────────────────────

    def _on_conn_timer(self, _evt=None):
        # Refresh in the UI thread; actual network checks are done in worker threads.
        self.refresh_connection_status()

    def refresh_connection_status(self):
        """Best-effort connection checks. Never hard-fail the app if a connector isn't installed."""
        def set_label(name: str, ok: bool, msg: str):
            lbl = getattr(self, "_conn_labels", {}).get(name)
            if not lbl:
                return
            # soft green/red-ish using text (we keep header background dark)
            dot = "●"
            lbl.SetLabel(f"{dot} {name}: {msg}")
            lbl.SetForegroundColour(wx.Colour(140, 255, 170) if ok else wx.Colour(255, 180, 180))

        def update_state(key, ok, msg):
            prev = self._conn_state.get(key)
            cur = (bool(ok), str(msg))
            self._conn_state[key] = cur
            if prev != cur:
                self.console_log(f"Connection {key}: {msg}" if ok else f"Connection {key}: {msg}")

        # AWS (configured vs live)
        def check_aws():
            ok = False
            msg = "not configured"
            if os.environ.get("AWS_PROFILE") or (os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY")):
                msg = "configured"
                ok = True
            # Try a fast STS call if boto3 is available (optional)
            try:
                import boto3  # type: ignore
                try:
                    sts = boto3.client("sts", region_name=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
                    sts.get_caller_identity()
                    ok = True
                    msg = "live"
                except Exception:
                    # keep configured
                    pass
            except Exception:
                pass
            wx.CallAfter(set_label, "AWS", ok, msg)
            wx.CallAfter(update_state, "AWS", ok, msg)

        # Snowflake
        def check_snowflake():
            ok = False
            msg = "not configured"
            if os.environ.get("SNOWFLAKE_ACCOUNT") and os.environ.get("SNOWFLAKE_USER"):
                ok = True
                msg = "configured"
            try:
                import snowflake.connector  # type: ignore
                # Only attempt connect if password/token present to avoid prompting
                if os.environ.get("SNOWFLAKE_PASSWORD") or os.environ.get("SNOWFLAKE_OAUTH_TOKEN"):
                    try:
                        kwargs = dict(
                            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
                            user=os.environ.get("SNOWFLAKE_USER"),
                            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE") or None,
                            database=os.environ.get("SNOWFLAKE_DATABASE") or None,
                            schema=os.environ.get("SNOWFLAKE_SCHEMA") or None,
                            login_timeout=5,
                            network_timeout=5,
                        )
                        if os.environ.get("SNOWFLAKE_OAUTH_TOKEN"):
                            kwargs["authenticator"] = "oauth"
                            kwargs["token"] = os.environ.get("SNOWFLAKE_OAUTH_TOKEN")
                        else:
                            kwargs["password"] = os.environ.get("SNOWFLAKE_PASSWORD")
                        conn = snowflake.connector.connect(**{k:v for k,v in kwargs.items() if v})
                        cur = conn.cursor()
                        cur.execute("select 1")
                        cur.close()
                        conn.close()
                        ok = True
                        msg = "live"
                    except Exception:
                        # keep configured
                        pass
            except Exception:
                pass
            wx.CallAfter(set_label, "Snowflake", ok, msg)
            wx.CallAfter(update_state, "Snowflake", ok, msg)

        # dbt (installed vs configured)
        def check_dbt():
            ok = False
            msg = "not installed"
            try:
                # Use subprocess (fast)
                cp = subprocess.run(["dbt", "--version"], capture_output=True, text=True, timeout=5, shell=False)
                if cp.returncode == 0:
                    ok = True
                    msg = "installed"
            except Exception:
                pass
            wx.CallAfter(set_label, "dbt", ok, msg)
            wx.CallAfter(update_state, "dbt", ok, msg)

        # Fabric (best-effort: configured)
        def check_fabric():
            ok = False
            msg = "not configured"
            if os.environ.get("FABRIC_TENANT_ID") or os.environ.get("POWERBI_TENANT_ID") or os.environ.get("AZURE_TENANT_ID"):
                ok = True
                msg = "configured"
            wx.CallAfter(set_label, "Fabric", ok, msg)
            wx.CallAfter(update_state, "Fabric", ok, msg)

        for fn in (check_aws, check_snowflake, check_dbt, check_fabric):
            threading.Thread(target=fn, daemon=True).start()

    def _set_progress(self, pct: int | None = None, msg: str = ""):
        try:
            if hasattr(self, "progress_label") and self.progress_label:
                self.progress_label.SetLabel(msg or "")
            if hasattr(self, "progress_gauge") and self.progress_gauge:
                if pct is None:
                    self.progress_gauge.Pulse()
                else:
                    self.progress_gauge.SetValue(max(0, min(100, int(pct))))
        except Exception:
            pass

    def _clear_progress(self):
        self._set_progress(0, "")

# Synthetic data (unchanged)
    @staticmethod
    def _most_common_format(strings, default_mask="DDD-DDD-DDDD"):
        def mask_one(s): return re.sub(r"\d", "D", s)
        masks = [mask_one(s) for s in strings if isinstance(s, str)]
        return Counter(masks).most_common(1)[0][0] if masks else default_mask

    @staticmethod
    def _sample_with_weights(values):
        if not values: return lambda *_: None
        counts = Counter(values); vals, weights = zip(*counts.items())
        total = float(sum(weights)); probs = [w/total for w in weights]
        def pick(_row=None):
            r = random.random(); acc = 0.0
            for v, p in zip(vals, probs):
                acc += p
                if r <= acc: return v
            return vals[-1]
        return pick

    def _build_generators(self, src_df: pd.DataFrame, fields):
        gens = {}
        # simple realistic name pools
        first_names = ["Olivia","Liam","Emma","Noah","Ava","Oliver","Sophia","Elijah","Isabella","James",
                       "Amelia","William","Mia","Benjamin","Charlotte","Lucas","Harper","Henry","Evelyn","Alexander"]
        last_names = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
                      "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin"]
        for col in fields:
            lower = col.lower()
            series = src_df[col] if col in src_df.columns else pd.Series([], dtype=object)
            col_vals = [v for v in series.tolist() if (v is not None and str(v).strip() != "")]
            col_strs = [str(v) for v in col_vals]
            if "email" in lower:
                domains = [s.split("@",1)[1].lower() for s in col_strs if "@" in s]
                dom = self._sample_with_weights(domains or ["gmail.com","yahoo.com","outlook.com","example.com"])
                pick = self._sample_with_weights(col_vals) if col_vals else None
                gens[col] = (lambda _row, p=pick, d=dom: (p() if p and random.random()<0.7 else f"user{random.randint(1000,9999)}@{d()}"))
                continue
            if any(k in lower for k in ["phone","mobile","cell","telephone"]):
                mask = self._most_common_format([s for s in col_strs if re.search(r"\d", s)])
                gens[col] = lambda _row, m=mask: "".join(str(random.randint(0,9)) if ch=="D" else ch for ch in m); continue
            if "first" in lower and "name" in lower:
                gens[col] = lambda _row, pool=first_names: random.choice(pool); continue
            if "last" in lower and "name" in lower:
                gens[col] = lambda _row, pool=last_names: random.choice(pool); continue
            if "date" in lower or "dob" in lower:
                dmax=datetime.today(); dmin=dmax-timedelta(days=3650); delta=(dmax-dmin).days or 365
                gens[col]=lambda _row, a=dmin, d=delta: (a+timedelta(days=random.randint(0, max(1,d)))).strftime("%Y-%m-%d"); continue
            uniq = set(col_vals)
            if uniq and len(uniq) <= 50:
                gens[col] = self._sample_with_weights(col_vals); continue
            if col_vals:
                pick = self._sample_with_weights(col_vals); gens[col]=lambda _r, p=pick: p()
            else:
                letters="abcdefghijklmnopqrstuvwxyz"
                gens[col]=lambda _r: "".join(random.choice(letters) for _ in range(random.randint(5,10)))
        return gens

    def on_generate_synth(self, _evt=None):
        if not self.headers:
            wx.MessageBox("Load data first to choose fields.", "No data", wx.OK | wx.ICON_WARNING)
            return
        src_df = pd.DataFrame(self.raw_data, columns=self.headers)
        try:
            dlg = SyntheticDataDialog(self, sample_df=src_df)
        except TypeError:
            dlg = SyntheticDataDialog(self, src_df)
        if hasattr(dlg, "ShowModal"):
            if dlg.ShowModal() != wx.ID_OK:
                dlg.Destroy(); return
        try:
            df = dlg.get_dataframe()
            if df is None or df.empty:
                n_rows = 100
                fields = list(self.headers)
                gens = self._build_generators(src_df, fields)
                out_rows = []
                for _ in range(int(n_rows)):
                    row_map = {}
                    for f in fields:
                        g = gens.get(f)
                        val = g(row_map) if callable(g) else None
                        row_map[f] = "" if val is None else val
                    out_rows.append([row_map[f] for f in fields])
                df = pd.DataFrame(out_rows, columns=fields)
        except Exception as e:
            wx.MessageBox(f"Synthetic data error: {e}", "Error", wx.OK | wx.ICON_ERROR)
            if hasattr(dlg, "Destroy"): dlg.Destroy()
            return
        if hasattr(dlg, "Destroy"): dlg.Destroy()
        hdr = list(df.columns); data = df.values.tolist()
        self.headers = hdr; self.raw_data = data
        self._display(hdr, data); self._reset_kpis_for_new_dataset(hdr, data)
        self.kernel.log("synthetic_generated", rows=len(data), cols=len(hdr), fields=hdr)

    # MDM helpers and action
    @staticmethod
    def _find_col(cols, *cands):
        cl = {c.lower(): c for c in cols}
        for cand in cands:
            for c in cl:
                if cand in c:
                    return cl[c]
        return None

    @staticmethod
    def _norm_email(x): return str(x).strip().lower() if x is not None else None
    @staticmethod
    def _norm_phone(x):
        if x is None: return None
        digits = re.sub(r"\D+", "", str(x))
        if len(digits) >= 10: return digits[-10:]
        return digits or None
    @staticmethod
    def _norm_name(x):  return re.sub(r"[^a-z]", "", str(x).lower()) if x is not None else None
    @staticmethod
    def _norm_text(x):  return re.sub(r"\s+", " ", str(x).strip().lower()) if x is not None else None

    @staticmethod
    def _sim(a, b):
        if not a or not b: return 0.0
        return SequenceMatcher(None, a, b).ratio()

    def _block_key(self, row, cols):
        e = row.get(cols.get("email"))
        if e: return f"e:{self._norm_email(e)}"
        p = row.get(cols.get("phone"))
        if p: return f"p:{self._norm_phone(p)}"
        fi = (row.get(cols.get("first")) or "")[:1].lower()
        li = (row.get(cols.get("last")) or "")[:1].lower()
        zipc = str(row.get(cols.get("zip")) or "")[:3]
        city = str(row.get(cols.get("city")) or "")[:3].lower()
        return f"n:{fi}{li}|{zipc or city}"

    def _score_pair(self, a, b, cols, use_email, use_phone, use_name, use_addr):
        parts=[]; weights=[]
        if use_email and cols.get("email"):
            ea=self._norm_email(a.get(cols["email"])); eb=self._norm_email(b.get(cols["email"]))
            if ea and eb: parts.append(1.0 if ea==eb else self._sim(ea,eb)); weights.append(0.5)
        if use_phone and cols.get("phone"):
            pa=self._norm_phone(a.get(cols["phone"])); pb=self._norm_phone(b.get(cols["phone"]))
            if pa and pb: parts.append(1.0 if pa==pb else self._sim(pa,pb)); weights.append(0.5)
        if use_name and (cols.get("first") or cols.get("last")):
            fa=self._norm_name(a.get(cols.get("first"))); fb=self._norm_name(b.get(cols.get("first")))
            la=self._norm_name(a.get(cols.get("last")));  lb=self._norm_name(b.get(cols.get("last")))
            if fa and fb: parts.append(self._sim(fa,fb)); weights.append(0.25)
            if la and lb: parts.append(self._sim(la,lb)); weights.append(0.3)
        if use_addr and (cols.get("addr") or cols.get("city")):
            aa=self._norm_text(a.get(cols.get("addr"))); ab=self._norm_text(b.get(cols.get("addr")))
            ca=self._norm_text(a.get(cols.get("city"))); cb=self._norm_text(b.get(cols.get("city")))
            sa=self._norm_text(a.get(cols.get("state"))); sb=self._norm_text(b.get(cols.get("state")))
            za=self._norm_text(a.get(cols.get("zip")));   zb=self._norm_text(b.get(cols.get("zip")))
            chunk=[]
            if aa and ab: chunk.append(self._sim(aa,ab))
            if ca and cb: chunk.append(self._sim(ca,cb))
            if sa and sb: chunk.append(self._sim(sa,sb))
            if za and zb: chunk.append(1.0 if za==zb else self._sim(za,zb))
            if chunk: parts.append(sum(chunk)/len(chunk)); weights.append(0.25)
        if not parts: return 0.0
        wsum = sum(weights) or 1.0
        return sum(p*w for p,w in zip(parts,weights))/wsum

    def _run_mdm(self, dataframes, use_email=True, use_phone=True, use_name=True, use_addr=True, threshold=0.85):
        datasets=[]; union_cols=set()
        for df in dataframes:
            cols=list(df.columns)
            colmap={
                "email": self._find_col(cols,"email"),
                "phone": self._find_col(cols,"phone","mobile","cell","telephone"),
                "first": self._find_col(cols,"first name","firstname","given"),
                "last":  self._find_col(cols,"last name","lastname","surname","family"),
                "addr":  self._find_col(cols,"address","street"),
                "city":  self._find_col(cols,"city"),
                "state": self._find_col(cols,"state","province","region"),
                "zip":   self._find_col(cols,"zip","postal"),
            }
            union_cols.update(cols)
            datasets.append((df.reset_index(drop=True), colmap))

        records=[]; offset=0
        for df,colmap in datasets:
            for i in range(len(df)):
                records.append((offset+i, df.iloc[i].to_dict(), colmap))
            offset += len(df)

        parent={}
        def find(x):
            parent.setdefault(x,x)
            if parent[x]!=x: parent[x]=find(parent[x])
            return parent[x]
        def union(a,b):
            ra,rb = find(a),find(b)
            if ra!=rb: parent[rb]=ra

        blocks=defaultdict(list)
        for rec_id,row,cmap in records:
            key=self._block_key(row,cmap)
            blocks[(key, tuple(sorted(cmap.items())) )].append((rec_id,row,cmap))

        for _, members in blocks.items():
            n=len(members)
            if n<=1: continue
            for i in range(n):
                for j in range(i+1,n):
                    id_a,row_a,cmap_a = members[i]
                    id_b,row_b,cmap_b = members[j]
                    cols={k: cmap_a.get(k) or cmap_b.get(k) for k in ("email","phone","first","last","addr","city","state","zip")}
                    score=self._score_pair(row_a,row_b,cols,use_email,use_phone,use_name,use_addr)
                    if score>=threshold: union(id_a,id_b)

        clusters=defaultdict(list)
        for rec_id,row,cmap in records:
            clusters[find(rec_id)].append((row,cmap))

        def best_value(values):
            vals=[v for v in values if (v is not None and str(v).strip()!="")]
            if not vals: return ""
            parsed=[]
            for v in vals:
                s=str(v)
                for fmt in ("%Y-%m-%d","%m/%d/%Y","%d/%m/%Y","%Y/%m/%d"):
                    try:
                        parsed.append(datetime.strptime(s,fmt)); break
                    except: pass
            if parsed and len(parsed)>=len(vals)*0.6:
                return max(parsed).strftime("%Y-%m-%d")
            nums=pd.to_numeric(pd.Series(vals).astype(str).str.replace(",",""), errors="coerce").dropna()
            if len(nums)>=len(vals)*0.6:
                med=float(nums.median()); return str(int(med)) if med.is_integer() else f"{med:.2f}"
            counts=Counter([str(v).strip() for v in vals])
            top,freq=counts.most_common(1)[0]
            ties=[k for k,c in counts.items() if c==freq]
            return ties[0] if len(ties)==1 else max(ties, key=len)

        all_cols=list(sorted(union_cols, key=lambda x: x.lower()))
        golden=[]
        for cluster_rows in clusters.values():
            merged={col: best_value([r.get(col) for r,_ in cluster_rows]) for col in all_cols}
            golden.append(merged)
        return pd.DataFrame(golden, columns=all_cols)

    def on_mdm(self, _evt=None):
        if not self.headers:
            wx.MessageBox("Load a base dataset first (or generate synthetic data).",
                          "MDM", wx.OK | wx.ICON_WARNING); return

        dlg = MDMDialog(self)
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy(); return
        params = dlg.get_params(); dlg.Destroy()

        dataframes=[]
        if params["include_current"]:
            dataframes.append(pd.DataFrame(self.raw_data, columns=self.headers))
        try:
            for src in params["sources"]:
                if src["type"]=="file":
                    text = self._load_text_file(src["value"])
                    hdr,data = detect_and_split_data(text)
                else:
                    text = download_text_from_uri(src["value"])
                    hdr,data = detect_and_split_data(text)
                dataframes.append(pd.DataFrame(data, columns=hdr))
        except Exception as e:
            wx.MessageBox(f"Failed to load a source:\n{e}", "MDM", wx.OK | wx.ICON_ERROR); return

        if len(dataframes) < 2:
            wx.MessageBox("Please add at least one additional dataset.", "MDM",
                          wx.OK | wx.ICON_WARNING); return

        try:
            golden = self._run_mdm(
                dataframes,
                use_email=params["use_email"],
                use_phone=params["use_phone"],
                use_name=params["use_name"],
                use_addr=params["use_addr"],
                threshold=params["threshold"],
            )
        except Exception as e:
            import traceback
            wx.MessageBox(f"MDM failed:\n{e}\n\n{traceback.format_exc()}",
                          "MDM", wx.OK | wx.ICON_ERROR); return

        hdr = list(golden.columns); data = golden.astype(str).values.tolist()
        self.headers, self.raw_data = hdr, data
        self._display(hdr, data); self._reset_kpis_for_new_dataset(hdr, data)
        self.current_process = "MDM"
        self._show_catalog_toolbar(False)
        self.kernel.log("mdm_completed", golden_rows=len(data), golden_cols=len(hdr), params=params)

    # Catalog metadata persistence helpers
    def _load_catalog_meta(self):
        try:
            return dict(self.kernel.data.get("state", {}).get("catalog_meta", {}))
        except Exception:
            return {}

    def _save_catalog_meta(self, meta: dict):
        try:
            self.kernel.data.setdefault("state", {})["catalog_meta"] = dict(meta)
            self.kernel._save()
        except Exception:
            pass

    def _apply_catalog_meta_to_table(self, hdr, data):
        # Ensure SLA column exists (insert before 'Example' when possible)
        if "SLA" not in hdr:
            insert_at = hdr.index("Example") if "Example" in hdr else len(hdr)
            hdr = list(hdr[:insert_at]) + ["SLA"] + list(hdr[insert_at:])
            for r in range(len(data)):
                data[r] = list(data[r][:insert_at]) + ["" ] + list(data[r][insert_at:])

        col_idx = {name: i for i, name in enumerate(hdr)}
        meta = self._load_catalog_meta()

        if "Field" in col_idx:
            f_idx = col_idx["Field"]
            for r in range(len(data)):
                row = list(data[r])
                field_name = str(row[f_idx]).strip()
                if not field_name:
                    data[r] = row
                    continue
                saved = meta.get(fld := field_name, {})
                for key in ("Friendly Name", "Description", "Data Type", "Nullable", "SLA"):
                    if key in col_idx and key in saved:
                        row[col_idx[key]] = saved[key]
                data[r] = row

        return hdr, data

    # Catalog toolbar show/hide
    def _show_catalog_toolbar(self, show: bool):
        if show:
            self.catalog_toolbar_panel.Show()
        else:
            self.catalog_toolbar_panel.Hide()
        self.Layout()

    def _snapshot_grid_to_meta(self):
        hdr = [self.grid.GetColLabelValue(i) for i in range(self.grid.GetNumberCols())]
        try:
            f_idx = hdr.index("Field")
        except ValueError:
            return
        editable = {"Friendly Name", "Description", "Data Type", "Nullable", "SLA"}
        col_idx = {name: i for i, name in enumerate(hdr)}
        meta = self._load_catalog_meta()

        for r in range(self.grid.GetNumberRows()):
            field_name = self.grid.GetCellValue(r, f_idx).strip()
            if not field_name:
                continue
            meta.setdefault(field_name, {})
            for name in editable:
                if name in col_idx:
                    meta[field_name][name] = self.grid.GetCellValue(r, col_idx[name])

        self._save_catalog_meta(meta)

    def on_catalog_save(self, _evt=None):
        if self.current_process != "Catalog":
            return
        self._snapshot_grid_to_meta()
        wx.MessageBox("Catalog edits saved.", "Catalog", wx.OK | wx.ICON_INFORMATION)

    def on_catalog_reset(self, _evt=None):
        if self.current_process != "Catalog":
            return
        hdr = [self.grid.GetColLabelValue(i) for i in range(self.grid.GetNumberCols())]
        try:
            f_idx = hdr.index("Field")
        except ValueError:
            return
        meta = self._load_catalog_meta()
        for r in range(self.grid.GetNumberRows()):
            field_name = self.grid.GetCellValue(r, f_idx).strip()
            if field_name in meta:
                del meta[field_name]
        self._save_catalog_meta(meta)
        self.do_analysis_process("Catalog")

    # Analyses
    def do_analysis_process(self, proc_name: str):
        if not self.headers:
            wx.MessageBox("Load data first.", "No data", wx.OK | wx.ICON_WARNING); return

        self.current_process = proc_name
        self._set_progress(None, f"Running: {proc_name}")
        df = self._as_df(self.raw_data, self.headers)
        self.console_set_status(f"Running: {proc_name}")
        self.console_log(f"Run analysis: {proc_name}")

        if proc_name == "Profile":
            try:
                out = profile_analysis(df)
                hdr, data = self._coerce_hdr_data(out)
            except Exception:
                desc = pd.DataFrame({
                    "Field": df.columns,
                    "Null %": [f"{df[c].isna().mean()*100:.1f}%" for c in df.columns],
                    "Unique": [df[c].nunique() for c in df.columns],
                })
                hdr, data = list(desc.columns), desc.values.tolist()
            null_pct, uniq_pct = self._compute_profile_metrics(df)
            self.metrics["null_pct"] = null_pct
            self.metrics["uniqueness"] = uniq_pct
            self._render_kpis()
            self.grid.EnableEditing(False)
            self._show_catalog_toolbar(False)
            self.kernel.log("run_profile", null_pct=null_pct, uniqueness=uniq_pct)

        elif proc_name == "Quality":
            try:
                out = quality_analysis(df, self.quality_rules)
                hdr, data = self._coerce_hdr_data(out)
            except Exception:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                rows = []
                for c in df.columns:
                    comp = 100.0 - df[c].isna().mean()*100.0
                    uniq = df[c].nunique(dropna=True)
                    num = pd.to_numeric(df[c], errors="coerce")
                    validity = 100.0 if num.notna().mean() > 0.8 else None
                    qs = comp if validity is None else (comp + validity)/2.0
                    rows.append([c, len(df), f"{comp:.1f}", uniq,
                                 f"{validity:.1f}" if validity is not None else "—",
                                 f"{qs:.1f}", now])
                hdr = ["Field", "Total", "Completeness (%)", "Unique Values",
                       "Validity (%)", "Quality Score (%)", "Analysis Date"]
                data = rows
            completeness, validity, dq = self._compute_quality_metrics(df)
            self.metrics["completeness"] = completeness
            self.metrics["validity"] = validity
            self.metrics["dq_score"] = dq
            self._render_kpis()
            self.grid.EnableEditing(False)
            self._show_catalog_toolbar(False)
            self.kernel.log("run_quality", completeness=completeness, validity=validity, dq_score=dq)

        elif proc_name == "Detect Anomalies":
            try:
                work, count = self._detect_anomalies(df)
                hdr, data = list(work.columns), work.values.tolist()
            except Exception:
                hdr, data = list(df.columns), df.values.tolist(); count = 0
            self.metrics["anomalies"] = count
            self._render_kpis()
            self.grid.EnableEditing(False)
            self._show_catalog_toolbar(False)
            self.kernel.log("run_detect_anomalies", anomalies=count)

        elif proc_name == "Catalog":
            try:
                out = catalog_analysis(df)
                hdr, data = self._coerce_hdr_data(out)
            except Exception:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                rows = []
                for c in df.columns:
                    sample = next((str(v) for v in df[c].dropna().head(1).tolist()), "")
                    dtype = "Numeric" if pd.to_numeric(df[c], errors="coerce").notna().mean() > 0.8 else "Text"
                    nullable = "Yes" if df[c].isna().mean() > 0 else "No"
                    friendly = c.replace("_", " ").title()
                    desc = f"{friendly} for each record."
                    rows.append([c, friendly, desc, dtype, nullable, sample, now])
                hdr = ["Field", "Friendly Name", "Description", "Data Type", "Nullable", "Example", "Analysis Date"]
                data = rows

            hdr, data = self._apply_catalog_meta_to_table(hdr, data)

            self.kernel.log("run_catalog", columns=len(hdr))
            self.grid.EnableEditing(True)
            self._show_catalog_toolbar(True)

        elif proc_name == "Compliance":
            try:
                out = compliance_analysis(df)
                hdr, data = self._coerce_hdr_data(out)
            except Exception:
                hdr = ["message"]; data = [["Compliance check complete."]]
            self.grid.EnableEditing(False)
            self._show_catalog_toolbar(False)
            self.kernel.log("run_compliance")

        else:
            hdr, data = ["message"], [[f"Unknown process: {proc_name}"]]
            self.grid.EnableEditing(False)
            self._show_catalog_toolbar(False)

        self._display(hdr, data)
        self._clear_progress()

    # Robust anomaly detector
    def _detect_anomalies(self, df: pd.DataFrame):
        work = df.copy()

        def parse_number(x):
            if x is None: return None
            s = str(x).strip()
            if s == "": return None
            neg = False
            if s.startswith("(") and s.endswith(")"):
                neg = True; s = s[1:-1]
            is_percent = s.endswith("%")
            s = s.replace("$","").replace(",","").replace("%","").strip()
            if re.fullmatch(r"[-+]?\d*\.?\d+", s):
                v = float(s); v = -v if neg else v
                if is_percent: v = v / 100.0
                return v
            return None

        numeric_cols=[]
        for c in work.columns:
            col_str = work[c].astype(str)
            dash_ratio = col_str.str.contains(r"[-()]+").mean()
            digit_median = col_str.str.findall(r"\d").map(len).median() if len(col_str) else 0
            phone_like = dash_ratio > 0.5 and digit_median >= 9
            vals = work[c].map(parse_number)
            ratio = vals.notna().mean()
            if ratio >= 0.60 and not phone_like:
                numeric_cols.append((c, vals.astype(float)))

        flags = pd.Series(False, index=work.index)
        reasons = [[] for _ in range(len(work))]
        pos_map = {idx: i for i, idx in enumerate(work.index)}

        for cname, x in numeric_cols:
            s = x.dropna()
            if s.size < 5: continue
            mu = s.mean(); sd = s.std(ddof=0)
            q1 = s.quantile(0.25); q3 = s.quantile(0.75); iqr = q3-q1
            lo = q1 - 1.5*iqr if iqr else None; hi = q3 + 1.5*iqr if iqr else None
            p01 = s.quantile(0.01) if len(s)>=50 else None
            p99 = s.quantile(0.99) if len(s)>=50 else None
            mostly_nonneg = (s.ge(0).mean() >= 0.95)
            mostly_nonzero = (s.ne(0).mean() >= 0.95)

            zhits = pd.Series(False, index=x.index)
            if sd and sd != 0:
                z = (x - mu).abs() / sd
                zhits = z > 3.0
            iqr_hits = pd.Series(False, index=x.index)
            if lo is not None and hi is not None:
                iqr_hits = (x < lo) | (x > hi)
            q_hits = pd.Series(False, index=x.index)
            if p01 is not None and p99 is not None:
                q_hits = (x < p01) | (x > p99)
            neg_hits = pd.Series(False, index=x.index)
            if mostly_nonneg: neg_hits = x < 0
            zero_hits = pd.Series(False, index=x.index)
            if mostly_nonzero: zero_hits = x == 0

            hits = (zhits.fillna(False) | iqr_hits.fillna(False) |
                    q_hits.fillna(False) | neg_hits.fillna(False) | zero_hits.fillna(False))
            flags = flags | hits.fillna(False)
            for idx, is_hit in hits.fillna(False).items():
                if is_hit:
                    bits=[]
                    if bool(zhits.get(idx, False)): bits.append("z>3")
                    if bool(iqr_hits.get(idx, False)): bits.append("IQR")
                    if bool(q_hits.get(idx, False)): bits.append("P1/P99")
                    if bool(neg_hits.get(idx, False)): bits.append("neg")
                    if bool(zero_hits.get(idx, False)): bits.append("zero")
                    reasons[pos_map[idx]].append(f"{cname} {'/'.join(bits)}")

        work["__anomaly__"] = ["; ".join(r) if r else "" for r in reasons]
        return work, int(flags.sum())


    # ──────────────────────────────────────────────────────────────────────────
    # NEW: dbt bundle generation (models + schema.yml + optional regex tests)
    # ──────────────────────────────────────────────────────────────────────────

    def _default_dbt_project_path(self) -> str:
        # repo root is one level above /app
        try:
            root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        except Exception:
            root = os.getcwd()
        # common layout: <repo>/dbt/sidecar_dbt
        cand = os.path.join(root, "dbt", "sidecar_dbt")
        return cand if os.path.exists(os.path.join(cand, "dbt_project.yml")) else root

    def _write_text(self, path: str, content: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def _dbt_safe_name(self, s: str, default: str = "model") -> str:
        s = re.sub(r"[^A-Za-z0-9_]+", "_", (s or "").strip())
        s = re.sub(r"_+", "_", s).strip("_")
        return s or default

    def on_generate_dbt_bundle(self, _evt=None):
        # must have some dataset / fields
        if (not self.headers) and self.grid.GetNumberCols() == 0:
            wx.MessageBox("Load data first so we have fields to map.", "dbt Bundle",
                          wx.OK | wx.ICON_WARNING)
            return

        dlg = DbtBundleDialog(self, default_project_path=self._default_dbt_project_path())
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy()
            return
        params = dlg.get_params()
        dlg.Destroy()

        project_dir = params["project_dir"]
        if not os.path.exists(os.path.join(project_dir, "dbt_project.yml")):
            wx.MessageBox(
                "That folder doesn't look like a dbt project (dbt_project.yml not found).\n\n"
                f"Folder:\n{project_dir}",
                "dbt Bundle",
                wx.OK | wx.ICON_ERROR
            )
            return

        try:
            mappings = self._extract_mapping_from_current_view()
            now = datetime.utcnow().isoformat() + "Z"

            # Try user's generator first (if present)
            if callable(generate_dbt_models):
                try:
                    generate_dbt_models(mappings=mappings, params=params, project_dir=project_dir, quality_rules=self.quality_rules)
                    wx.MessageBox("dbt bundle generated via app.dbt_generator.", "dbt Bundle", wx.OK | wx.ICON_INFORMATION)
                    self.kernel.log("dbt_bundle_generated", via="dbt_generator", project_dir=project_dir, model=params["model_name"])
                    return
                except TypeError:
                    # support older signature styles
                    try:
                        generate_dbt_models(mappings, project_dir)
                        wx.MessageBox("dbt bundle generated via app.dbt_generator.", "dbt Bundle", wx.OK | wx.ICON_INFORMATION)
                        self.kernel.log("dbt_bundle_generated", via="dbt_generator_legacy", project_dir=project_dir, model=params["model_name"])
                        return
                    except Exception:
                        pass
                except Exception:
                    pass

            # Fallback: generate a minimal staging model + schema.yml (Snowflake-friendly SQL)
            model_name = self._dbt_safe_name(params["model_name"], default="stg_data_buddy")
            src_relation = params["source_relation"].strip()
            materialized = params["materialized"]

            models_dir = os.path.join(project_dir, "models", "sidecar")
            macros_dir = os.path.join(project_dir, "macros", "sidecar")
            out_dir = os.path.join(project_dir, "target", "sidecar_bundle_out")
            os.makedirs(models_dir, exist_ok=True)
            os.makedirs(macros_dir, exist_ok=True)
            os.makedirs(out_dir, exist_ok=True)

            # Build SELECT list
            select_lines = []
            for m in mappings:
                src = (m.get("source_field") or "").strip()
                tgt = (m.get("target_field") or src).strip()
                tgt = self._dbt_safe_name(tgt, default=src or "col")
                # for snowflake casting, prefer catalog_data_type, else target_type
                dtype_hint = (m.get("catalog_data_type") or m.get("target_type") or "").strip()
                sf_type = self._guess_snowflake_type_from_catalog(dtype_hint) if dtype_hint else "VARCHAR"
                if src:
                    select_lines.append(f'    TRY_CAST({{ adapter.quote("{src}") }} AS {sf_type}) AS {tgt}')
            if not select_lines:
                select_lines = ["    *"]

            select_sql = ",\n".join(select_lines)

            sql = f"""{{{{ config(materialized='{materialized}') }}}}

WITH src AS (
  SELECT * FROM {src_relation}
)
SELECT
{select_sql}
FROM src
"""
            self._write_text(os.path.join(models_dir, f"{model_name}.sql"), sql)

            # Build schema.yml
            # Add not_null test when Nullable indicates not nullable, and regex tests when quality_rules has a pattern for the field.
            cols_yml = []
            need_regex_macro = False
            rules = self._compile_rules()
            for m in mappings:
                src = (m.get("source_field") or "").strip()
                tgt = (m.get("target_field") or src).strip()
                tgt = self._dbt_safe_name(tgt, default=src or "col")
                desc = (m.get("description") or "").strip()
                nullable = str(m.get("nullable") or "").strip().lower()
                tests = []
                if nullable in ("no", "n", "false", "not null", "non-null", "nonnullable"):
                    tests.append("not_null")
                if params.get("include_regex_tests", True):
                    rx = rules.get(src) or rules.get(tgt)
                    if rx is not None:
                        need_regex_macro = True
                        # store regex pattern string if possible
                        pattern_str = getattr(rx, "pattern", None) or str(rx)
                        # escape backslashes for yaml string
                        pattern_str = pattern_str.replace('\\', '\\\\')
                        tests.append({"regex_match": {"regex": pattern_str}})
                col_entry = {
                    "name": tgt,
                    "description": desc or f"Generated from {src}",
                }
                if tests:
                    col_entry["tests"] = tests
                cols_yml.append(col_entry)
            # Write schema.yml (manual YAML to avoid external deps)
            def y(s):  # basic YAML string esc
                return (s or "").replace("\\", "\\\\").replace('"', '\\\"')

            lines = []
            lines.append("version: 2")
            lines.append("")
            lines.append("models:")
            lines.append(f"  - name: {model_name}")
            lines.append("    description: \"Generated by Data Wizard (Sidecar) dbt bundle generator.\"")
            lines.append("    columns:")
            for col in cols_yml:
                lines.append(f"      - name: {col['name']}")
                lines.append(f"        description: \"{y(col.get('description',''))}\"")
                tests = col.get("tests") or []
                if tests:
                    lines.append("        tests:")
                    for t in tests:
                        if isinstance(t, str):
                            lines.append(f"          - {t}")
                        elif isinstance(t, dict) and "regex_match" in t:
                            rx = t["regex_match"].get("regex","")
                            lines.append("          - regex_match:")
                            lines.append(f"              regex: \"{y(rx)}\"")
            schema_yml = "\n".join(lines) + "\n"
            self._write_text(os.path.join(models_dir, "schema.yml"), schema_yml)


            # Macro for regex test (Snowflake REGEXP_LIKE)
            if need_regex_macro:
                macro = """{% test regex_match(model, column_name, regex) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and not regexp_like({{ column_name }}, regex)
{% endtest %}
"""
                self._write_text(os.path.join(macros_dir, "regex_match.sql"), macro)

            # Human README
            readme = f"""Data Wizard — dbt Bundle Output
==========================

Generated at (UTC): {now}

Created:
- models/sidecar/{model_name}.sql
- models/sidecar/schema.yml
- macros/sidecar/regex_match.sql (only if regex tests were enabled and rules existed)

How to run:
  cd {project_dir}
  dbt deps   (optional)
  dbt run -s {model_name}
  dbt test -s {model_name}

Source relation:
  {src_relation}

Notes:
- Column casts use TRY_CAST for safety.
- not_null tests are added when Nullable is set to "No" in Catalog.
- regex tests are generated from your Quality Rule Assignment (best-effort).
"""
            self._write_text(os.path.join(out_dir, "README.txt"), readme)

            self.kernel.log("dbt_bundle_generated", via="fallback_writer", project_dir=project_dir, model=model_name, mappings=len(mappings))
            wx.MessageBox(
                "dbt bundle generated successfully.\n\n"
                f"Model: {model_name}\n"
                f"Project: {project_dir}\n\n"
                "Run:\n  dbt run -s " + model_name + "\n  dbt test -s " + model_name,
                "dbt Bundle",
                wx.OK | wx.ICON_INFORMATION
            )

        except Exception as e:
            import traceback
            wx.MessageBox(
                f"Failed to generate dbt bundle:\n\n{e}\n\n{traceback.format_exc()}",
                "dbt Bundle",
                wx.OK | wx.ICON_ERROR
            )
            self.kernel.log("dbt_bundle_failed", error=str(e))

    # ──────────────────────────────────────────────────────────────────────────
    # NEW: Config File generation (AWS Pipeline configs + mappings)
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_filename(name: str, default="pipeline"):
        s = re.sub(r"[^a-zA-Z0-9._-]+", "_", (name or "").strip())
        s = s.strip("._-")
        return s or default

    @staticmethod
    def _guess_glue_type_from_catalog(data_type: str):
        """
        Very simple mapping. You can expand this later.
        Returns AWS Glue / Spark-ish type strings.
        """
        dt = (data_type or "").strip().lower()
        if not dt:
            return "string"
        if any(k in dt for k in ["int", "integer", "bigint", "smallint", "tinyint"]):
            return "bigint" if "big" in dt else "int"
        if any(k in dt for k in ["decimal", "numeric"]):
            return "decimal(18,4)"
        if any(k in dt for k in ["float", "double", "real"]):
            return "double"
        if any(k in dt for k in ["bool", "boolean"]):
            return "boolean"
        if any(k in dt for k in ["date"]):
            return "date"
        if any(k in dt for k in ["time", "timestamp", "datetime"]):
            return "timestamp"
        return "string"

    def _extract_mapping_from_current_view(self):
        """
        Primary rule:
        - If currently in Catalog view (grid has Field/Friendly Name/Data Type/etc), use those to build mappings.
        - Otherwise map current dataset headers 1:1 as string.
        """
        grid_cols = [self.grid.GetColLabelValue(i) for i in range(self.grid.GetNumberCols())]
        grid_rows = self.grid.GetNumberRows()

        def col_index(name):
            try:
                return grid_cols.index(name)
            except ValueError:
                return None

        # Catalog-style mapping
        if "Field" in grid_cols and any(x in grid_cols for x in ["Data Type", "Nullable", "Description", "Friendly Name"]):
            idx_field = col_index("Field")
            idx_friendly = col_index("Friendly Name")
            idx_desc = col_index("Description")
            idx_dtype = col_index("Data Type")
            idx_nullable = col_index("Nullable")
            idx_sla = col_index("SLA")

            mappings = []
            for r in range(grid_rows):
                field = (self.grid.GetCellValue(r, idx_field) if idx_field is not None else "").strip()
                if not field:
                    continue
                friendly = (self.grid.GetCellValue(r, idx_friendly) if idx_friendly is not None else "").strip()
                desc = (self.grid.GetCellValue(r, idx_desc) if idx_desc is not None else "").strip()
                dtype = (self.grid.GetCellValue(r, idx_dtype) if idx_dtype is not None else "").strip()
                nullable = (self.grid.GetCellValue(r, idx_nullable) if idx_nullable is not None else "").strip()
                sla = (self.grid.GetCellValue(r, idx_sla) if idx_sla is not None else "").strip()

                target_field = field
                if friendly:
                    # optional: transform friendly name into a safe column name
                    target_field = re.sub(r"\s+", "_", friendly.strip())
                    target_field = re.sub(r"[^a-zA-Z0-9_]+", "", target_field).strip("_")
                    if not target_field:
                        target_field = field

                glue_type = self._guess_glue_type_from_catalog(dtype)

                mappings.append({
                    "source_field": field,
                    "target_field": target_field,
                    "target_type": glue_type,
                    "catalog_data_type": dtype or "",
                    "nullable": nullable or "",
                    "sla": sla or "",
                    "description": desc or "",
                })

            if mappings:
                return mappings

        # Default mapping (dataset)
        base_cols = list(self.headers or [])
        if not base_cols and self.grid.GetNumberCols() > 0:
            base_cols = [self.grid.GetColLabelValue(i) for i in range(self.grid.GetNumberCols())]

        return [{
            "source_field": c,
            "target_field": c,
            "target_type": "string",
            "catalog_data_type": "",
            "nullable": "",
            "sla": "",
            "description": "",
        } for c in base_cols]

    def on_generate_config_files(self, _evt=None):
        # must have some dataset / fields
        if (not self.headers) and self.grid.GetNumberCols() == 0:
            wx.MessageBox("Load data first so we have fields to map.", "Config File",
                          wx.OK | wx.ICON_WARNING)
            return

        # Collect pipeline parameters from the user
        dlg = ConfigFileDialog(self)
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy()
            return
        params = dlg.get_params()
        dlg.Destroy()

        # choose output folder
        dd = wx.DirDialog(self, "Choose output folder for AWS pipeline config files",
                          style=wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
        if dd.ShowModal() != wx.ID_OK:
            dd.Destroy()
            return
        out_dir = dd.GetPath()
        dd.Destroy()

        try:
            mappings = self._extract_mapping_from_current_view()
            now = datetime.utcnow().isoformat() + "Z"

            # Derive a few helpful things
            pipeline_name = params.get("pipeline_name") or "data-wizard-pipeline"
            safe_name = self._safe_filename(pipeline_name, default="pipeline")
            out_prefix = os.path.join(out_dir, safe_name)
            os.makedirs(out_prefix, exist_ok=True)

            # Save params (what the user typed)
            pipeline_params = {
                "generated_at": now,
                "generated_by": "Data Wizard",
                "pipeline_name": pipeline_name,
                "aws_region": params.get("aws_region", "us-east-1"),
                "source": {
                    "type": params.get("source_type", "s3"),
                    "uri": params.get("source_uri", ""),
                    "format": params.get("source_format", "csv"),
                    "has_header": bool(params.get("source_has_header", True)),
                    "delimiter": params.get("source_delimiter", ","),
                },
                "target": {
                    "s3_uri": params.get("target_s3_uri", ""),
                    "format": params.get("target_format", "parquet"),
                    "compression": params.get("target_compression", "snappy"),
                    "partition_keys": params.get("partition_keys", []),
                },
                "glue": {
                    "database": params.get("glue_database", ""),
                    "table": params.get("glue_table", ""),
                    "crawler_name": params.get("glue_crawler_name", ""),
                    "job_name": params.get("glue_job_name", ""),
                    "iam_role_arn": params.get("iam_role_arn", ""),
                    "workflow_name": params.get("glue_workflow_name", ""),
                    "enable_crawler": bool(params.get("enable_crawler", True)),
                    "enable_job": bool(params.get("enable_job", True)),
                },
                "orchestration": {
                    "type": params.get("orchestration_type", "glue_workflow"),
                    "schedule_cron": params.get("schedule_cron", ""),
                },
                "notes": params.get("notes", ""),
            }

            # Build a pipeline config that many IaC tools can ingest (generic JSON)
            pipeline_config = {
                "schema_version": "1.0",
                "generated_at": now,
                "app": {
                    "name": "Data Wizard",
                    "kernel_path": getattr(self.kernel, "path", ""),
                },
                "pipeline": {
                    "name": pipeline_name,
                    "type": "aws_glue_etl",
                    "region": pipeline_params["aws_region"],
                },
                "connections": {
                    "source": pipeline_params["source"],
                    "target": pipeline_params["target"],
                },
                "resources": {
                    "glue": pipeline_params["glue"],
                    "orchestration": pipeline_params["orchestration"],
                },
                "mappings": {
                    "count": len(mappings),
                    "mapping_file_json": "field_mappings.json",
                    "mapping_file_csv": "field_mappings.csv",
                },
            }

            # Save mapping JSON + CSV
            mapping_json_path = os.path.join(out_prefix, "field_mappings.json")
            with open(mapping_json_path, "w", encoding="utf-8") as f:
                json.dump({"generated_at": now, "mappings": mappings}, f, ensure_ascii=False, indent=2)

            mapping_csv_path = os.path.join(out_prefix, "field_mappings.csv")
            pd.DataFrame(mappings).to_csv(mapping_csv_path, index=False)

            params_path = os.path.join(out_prefix, "pipeline_params.json")
            with open(params_path, "w", encoding="utf-8") as f:
                json.dump(pipeline_params, f, ensure_ascii=False, indent=2)

            config_path = os.path.join(out_prefix, "pipeline_config.json")
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(pipeline_config, f, ensure_ascii=False, indent=2)

            readme_path = os.path.join(out_prefix, "README.txt")
            with open(readme_path, "w", encoding="utf-8") as f:
                f.write(
                    "Data Wizard — AWS Pipeline Config Output\n"
                    "=====================================\n\n"
                    f"Generated at (UTC): {now}\n\n"
                    "Files:\n"
                    "- pipeline_config.json   (high-level pipeline definition)\n"
                    "- pipeline_params.json   (user-entered parameters)\n"
                    "- field_mappings.json    (source-to-target mapping objects)\n"
                    "- field_mappings.csv     (mapping in spreadsheet-friendly form)\n\n"
                    "How it maps fields:\n"
                    "- If you ran Catalog and the grid has Field/Friendly Name/Data Type/etc, it uses those.\n"
                    "- Otherwise it maps the current dataset headers 1:1 as string types.\n\n"
                    "Next steps (typical):\n"
                    "- Use these files to parameterize Glue Crawler/Job creation, a Glue Workflow, or Step Functions.\n"
                    "- If you want, we can add a 'Generate Glue Job Script' option next.\n"
                )

            self.kernel.log(
                "config_files_generated",
                out_dir=out_prefix,
                pipeline_name=pipeline_name,
                mappings=len(mappings),
                source_uri=pipeline_params["source"].get("uri", ""),
                target_s3=pipeline_params["target"].get("s3_uri", ""),
            )

            wx.MessageBox(
                "Config files generated successfully.\n\n"
                f"Folder:\n{out_prefix}\n\n"
                "Created:\n"
                "- pipeline_config.json\n"
                "- pipeline_params.json\n"
                "- field_mappings.json\n"
                "- field_mappings.csv\n"
                "- README.txt",
                "Config File",
                wx.OK | wx.ICON_INFORMATION
            )

        except Exception as e:
            import traceback
            wx.MessageBox(
                f"Failed to generate config files:\n\n{e}\n\n{traceback.format_exc()}",
                "Config File",
                wx.OK | wx.ICON_ERROR
            )
            self.kernel.log("config_files_failed", error=str(e))

    # ──────────────────────────────────────────────────────────────────────────
    # NEW: Snowflake Bundle generation (SQL + mappings + params)
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _guess_snowflake_type_from_catalog(data_type: str):
        """Map catalog-ish data types to Snowflake types (simple, extendable)."""
        dt = (data_type or "").strip().lower()
        if not dt:
            return "VARCHAR"
        if any(k in dt for k in ["bigint"]):
            return "NUMBER(38,0)"
        if any(k in dt for k in ["int", "integer", "smallint", "tinyint"]):
            return "NUMBER(38,0)"
        if any(k in dt for k in ["decimal", "numeric"]):
            return "NUMBER(38,4)"
        if any(k in dt for k in ["float", "double", "real"]):
            return "FLOAT"
        if any(k in dt for k in ["bool", "boolean"]):
            return "BOOLEAN"
        if "timestamp" in dt or "datetime" in dt:
            return "TIMESTAMP_NTZ"
        if "date" in dt:
            return "DATE"
        if "time" in dt:
            return "TIME"
        if any(k in dt for k in ["variant", "json"]):
            return "VARIANT"
        return "VARCHAR"

    def on_generate_snowflake_bundle(self, _evt=None):
        """Generate a Snowflake-ready bundle: SQL scripts + mappings + params."""
        # must have some dataset / fields
        if (not self.headers) and self.grid.GetNumberCols() == 0:
            wx.MessageBox("Load data first so we have fields to map.", "Snowflake Bundle",
                          wx.OK | wx.ICON_WARNING)
            return

        dlg = SnowflakeBundleDialog(self)
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy()
            return
        params = dlg.get_params()
        dlg.Destroy()

        # choose output folder
        dd = wx.DirDialog(self, "Choose output folder for the Snowflake bundle",
                          style=wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
        if dd.ShowModal() != wx.ID_OK:
            dd.Destroy()
            return
        out_dir = dd.GetPath()
        dd.Destroy()

        try:
            mappings = self._extract_mapping_from_current_view()
            now = datetime.utcnow().isoformat() + "Z"

            bundle_name = params.get("bundle_name") or "data_wizard_snowflake_bundle"
            safe_name = self._safe_filename(bundle_name, default="snowflake_bundle")
            out_prefix = os.path.join(out_dir, safe_name)
            os.makedirs(out_prefix, exist_ok=True)

            # save params
            params_path = os.path.join(out_prefix, "snowflake_params.json")
            with open(params_path, "w", encoding="utf-8") as f:
                json.dump({"generated_at": now, **params}, f, ensure_ascii=False, indent=2)

            # mapping outputs (reuse same mapping files for consistency)
            mapping_json_path = os.path.join(out_prefix, "field_mappings.json")
            with open(mapping_json_path, "w", encoding="utf-8") as f:
                json.dump({"generated_at": now, "mappings": mappings}, f, ensure_ascii=False, indent=2)

            mapping_csv_path = os.path.join(out_prefix, "field_mappings.csv")
            pd.DataFrame(mappings).to_csv(mapping_csv_path, index=False)

            # Build DDL from mapping
            cols_sql = []
            for m in mappings:
                col = m.get("target_field") or m.get("source_field") or "COL"
                col = re.sub(r"[^A-Za-z0-9_]+", "_", str(col)).strip("_") or "COL"
                dtype = self._guess_snowflake_type_from_catalog(m.get("catalog_data_type") or "")
                # if catalog dtype empty, fall back to mapping target_type
                if (not m.get("catalog_data_type")) and m.get("target_type"):
                    dtype = self._guess_snowflake_type_from_catalog(m.get("target_type"))
                nullable = str(m.get("nullable") or "").strip().lower()
                null_sql = "" if nullable in ("", "yes", "y", "true", "nullable") else " NOT NULL"
                cols_sql.append(f'  "{col.upper()}" {dtype}{null_sql}')
            if not cols_sql:
                cols_sql = ['  "COL" VARCHAR']

            db = (params.get("database") or "").strip() or "DATA_WIZARD"
            schema = (params.get("schema") or "").strip() or "PUBLIC"
            role = (params.get("role") or "").strip() or "DATA_WIZARD_ROLE"
            wh = (params.get("warehouse") or "").strip() or "DATA_WIZARD_WH"
            table = (params.get("target_table") or "").strip() or "INGEST_TABLE"
            stage = (params.get("stage") or "").strip() or "DATA_WIZARD_STAGE"
            file_format = (params.get("file_format") or "").strip() or "DATA_WIZARD_FF"

            src_type = params.get("source_type", "s3")
            src_url = (params.get("source_url") or "").strip()
            src_pattern = (params.get("pattern") or "").strip()

            # SQL scripts
            setup_sql = f"""-- 00_setup.sql
-- Generated by Data Wizard at {now}
-- Optional: adjust security objects to your standards

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS {wh}
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Database / Schema
CREATE DATABASE IF NOT EXISTS {db};
CREATE SCHEMA IF NOT EXISTS {db}.{schema};

-- Role (optional)
CREATE ROLE IF NOT EXISTS {role};

-- Grants (basic)
GRANT USAGE ON WAREHOUSE {wh} TO ROLE {role};
GRANT USAGE ON DATABASE {db} TO ROLE {role};
GRANT USAGE ON SCHEMA {db}.{schema} TO ROLE {role};
"""

            ff_sql = f"""-- 01_file_format.sql
USE DATABASE {db};
USE SCHEMA {schema};

CREATE FILE FORMAT IF NOT EXISTS {file_format}
  TYPE = {params.get('format_type','CSV')}
  FIELD_DELIMITER = '{params.get('field_delimiter',',')}'
  SKIP_HEADER = {int(bool(params.get('skip_header', True)))}
  FIELD_OPTIONALLY_ENCLOSED_BY = '{params.get('enclosed_by','"')}'
  NULL_IF = ({params.get('null_if','').strip() and "'"+params.get('null_if').strip()+"'" or "''"});
"""

            stage_sql_lines = [
                f"-- 02_stage.sql",
                f"USE DATABASE {db};",
                f"USE SCHEMA {schema};",
                "",
            ]
            if src_type == "s3":
                storage_integration = (params.get("storage_integration") or "").strip()
                if storage_integration:
                    stage_sql_lines.append(
                        f"CREATE STAGE IF NOT EXISTS {stage} URL='{src_url}' STORAGE_INTEGRATION={storage_integration} FILE_FORMAT={file_format};"
                    )
                else:
                    stage_sql_lines.append(
                        f"-- If you use STORAGE_INTEGRATION, set it in the dialog to avoid embedding keys in SQL."
                    )
                    stage_sql_lines.append(
                        f"CREATE STAGE IF NOT EXISTS {stage} URL='{src_url}' FILE_FORMAT={file_format};"
                    )
            elif src_type == "azure":
                storage_integration = (params.get("storage_integration") or "").strip()
                stage_sql_lines.append(
                    f"CREATE STAGE IF NOT EXISTS {stage} URL='{src_url}' STORAGE_INTEGRATION={storage_integration} FILE_FORMAT={file_format};"
                )
            elif src_type == "gcs":
                storage_integration = (params.get("storage_integration") or "").strip()
                stage_sql_lines.append(
                    f"CREATE STAGE IF NOT EXISTS {stage} URL='{src_url}' STORAGE_INTEGRATION={storage_integration} FILE_FORMAT={file_format};"
                )
            else:
                stage_sql_lines.append("-- Unsupported source type for stage generation. Use an existing stage.")

            stage_sql = "\n".join(stage_sql_lines) + "\n"

            cols_join = ",\n".join(cols_sql)

            table_sql = f"""-- 03_table.sql
USE DATABASE {db};
USE SCHEMA {schema};

CREATE TABLE IF NOT EXISTS {table} (
{cols_join}
);
"""

            # COPY INTO statement
            pattern_clause = f" PATTERN='{src_pattern}'" if src_pattern else ""
            copy_sql = f"""-- 04_copy_into.sql
USE DATABASE {db};
USE SCHEMA {schema};

-- Load from stage into table
COPY INTO {table}
FROM @{stage}
FILE_FORMAT = (FORMAT_NAME = {file_format}){pattern_clause}
ON_ERROR = '{params.get('on_error','CONTINUE')}';
"""

            stream_task_sql = f"""-- 05_stream_task.sql
-- Optional: continuous ingest pattern (edit to your standards)
-- Requires Snowpipe / event notifications if you want auto-ingest.

-- Example only:
-- CREATE OR REPLACE STREAM {table}_STREAM ON TABLE {table};
-- CREATE OR REPLACE TASK {table}_TASK WAREHOUSE={wh} SCHEDULE='USING CRON {params.get('schedule_cron','0 * * * * UTC')}'
-- AS
--   -- your transformation / merge here
--   SELECT 1;
-- ALTER TASK {table}_TASK RESUME;
"""

            scripts = [
                ("00_setup.sql", setup_sql),
                ("01_file_format.sql", ff_sql),
                ("02_stage.sql", stage_sql),
                ("03_table.sql", table_sql),
                ("04_copy_into.sql", copy_sql),
                ("05_stream_task.sql", stream_task_sql),
            ]
            for name, content in scripts:
                with open(os.path.join(out_prefix, name), "w", encoding="utf-8") as f:
                    f.write(content)

            readme = f"""Data Wizard — Snowflake Bundle
===========================

Generated at (UTC): {now}

This folder contains a starter Snowflake deployment bundle:
- snowflake_params.json   (what you entered)
- field_mappings.json/csv (mapping from Data Buddy)
- 00_setup.sql            (warehouse/db/schema/role + basic grants)
- 01_file_format.sql      (file format)
- 02_stage.sql            (external stage)
- 03_table.sql            (target table)
- 04_copy_into.sql        (COPY INTO load)
- 05_stream_task.sql      (optional patterns)

Recommended run order:
  1) 00_setup.sql
  2) 01_file_format.sql
  3) 02_stage.sql
  4) 03_table.sql
  5) 04_copy_into.sql

Notes:
- For S3/Azure/GCS, using STORAGE_INTEGRATION is recommended (avoid embedding keys).
- Column types are best-effort from Catalog 'Data Type'. Extend mapping rules anytime.
"""
            with open(os.path.join(out_prefix, "README.txt"), "w", encoding="utf-8") as f:
                f.write(readme)

            self.kernel.log(
                "snowflake_bundle_generated",
                out_dir=out_prefix,
                database=db,
                schema=schema,
                table=table,
                stage=stage,
                mappings=len(mappings),
            )

            wx.MessageBox(
                "Snowflake bundle generated successfully.\n\n"
                f"Folder:\n{out_prefix}\n\n"
                "Created:\n"
                "- snowflake_params.json\n"
                "- field_mappings.json\n"
                "- field_mappings.csv\n"
                "- 00_setup.sql .. 05_stream_task.sql\n"
                "- README.txt",
                "Snowflake Bundle",
                wx.OK | wx.ICON_INFORMATION
            )

        except Exception as e:
            import traceback
            wx.MessageBox(
                f"Failed to generate Snowflake bundle:\n\n{e}\n\n{traceback.format_exc()}",
                "Snowflake Bundle",
                wx.OK | wx.ICON_ERROR
            )
            self.kernel.log("snowflake_bundle_failed", error=str(e))



    # ──────────────────────────────────────────────────────────────────────────
    # NEW: Microsoft Fabric Bundle generation (Lakehouse/Pipeline placeholders + mappings)
    # ──────────────────────────────────────────────────────────────────────────

    def on_generate_fabric_bundle(self, _evt=None):
        """
        Generates a Fabric starter bundle from your current mapping:
        - fabric_params.json
        - field_mappings.json / field_mappings.csv
        - 00_lakehouse.sql (optional DDL idea)
        - 01_dataflow_gen2_notes.txt (how to wire)
        - 02_pipeline_notes.txt (how to run)
        - README.txt
        """
        if (not self.headers) and self.grid.GetNumberCols() == 0:
            wx.MessageBox("Load data first so we have fields to map.", "Fabric Bundle",
                          wx.OK | wx.ICON_WARNING)
            return

        dlg = FabricBundleDialog(self)
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy()
            return
        params = dlg.get_params()
        dlg.Destroy()

        dd = wx.DirDialog(self, "Choose output folder for the Fabric bundle",
                          style=wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
        if dd.ShowModal() != wx.ID_OK:
            dd.Destroy()
            return
        out_dir = dd.GetPath()
        dd.Destroy()

        try:
            mappings = self._extract_mapping_from_current_view()
            now = datetime.utcnow().isoformat() + "Z"

            bundle_name = params.get("bundle_name") or "data_wizard_fabric_bundle"
            safe_name = self._safe_filename(bundle_name, default="fabric_bundle")
            out_prefix = os.path.join(out_dir, safe_name)
            os.makedirs(out_prefix, exist_ok=True)

            # save params + mappings
            with open(os.path.join(out_prefix, "fabric_params.json"), "w", encoding="utf-8") as f:
                json.dump({"generated_at": now, **params}, f, ensure_ascii=False, indent=2)

            with open(os.path.join(out_prefix, "field_mappings.json"), "w", encoding="utf-8") as f:
                json.dump({"generated_at": now, "mappings": mappings}, f, ensure_ascii=False, indent=2)
            pd.DataFrame(mappings).to_csv(os.path.join(out_prefix, "field_mappings.csv"), index=False)

            # Optional: a Lakehouse table DDL idea (Spark SQL style)
            lakehouse = (params.get("lakehouse_name") or "data_wizard_lakehouse").strip() or "data_wizard_lakehouse"
            table = (params.get("table_name") or "stg_ingest").strip() or "stg_ingest"

            cols = []
            for m in mappings:
                col = (m.get("target_field") or m.get("source_field") or "col").strip()
                col = re.sub(r"[^A-Za-z0-9_]+", "_", col).strip("_") or "col"
                dtype = (m.get("target_type") or "string").strip().lower()
                # Fabric Lakehouse uses Spark types; reuse glue mapping heuristics
                spark_type = self._guess_glue_type_from_catalog(m.get("catalog_data_type") or dtype)
                cols.append(f"  {col} {spark_type}")
            if not cols:
                cols = ["  col string"]

            ddl = (
                f"-- 00_lakehouse.sql\n"
                f"-- Generated by Data Wizard at {now}\n\n"
                f"-- Run in a Fabric Lakehouse SQL endpoint (or Spark SQL notebook as needed).\n"
                f"CREATE TABLE IF NOT EXISTS {table} (\n" + ",\n".join(cols) + "\n);\n"
            )
            with open(os.path.join(out_prefix, "00_lakehouse.sql"), "w", encoding="utf-8") as f:
                f.write(ddl)

            # Notes: Dataflow Gen2 + Pipeline wiring
            df_notes = (
                "01_dataflow_gen2_notes.txt\n"
                "===========================\n\n"
                "Goal: land raw files into OneLake/Lakehouse and create a staging table.\n\n"
                "Typical pattern:\n"
                "1) Create a Lakehouse in Fabric.\n"
                "2) Use Dataflow Gen2 to ingest from your source (S3 / ADLS / SharePoint / etc).\n"
                "3) Map/rename columns using field_mappings.csv.\n"
                "4) Output to Lakehouse table.\n\n"
                "Use the mapping files in this bundle to keep names/types consistent.\n"
            )
            with open(os.path.join(out_prefix, "01_dataflow_gen2_notes.txt"), "w", encoding="utf-8") as f:
                f.write(df_notes)

            pipe_notes = (
                "02_pipeline_notes.txt\n"
                "======================\n\n"
                "Goal: orchestrate ingestion + transformation.\n\n"
                "Typical pattern:\n"
                "- Fabric Pipeline activity: refresh Dataflow Gen2\n"
                "- Notebook activity: run transformations (Spark)\n"
                "- Optional: dbt (external) to build semantic models in Snowflake/Fabric Warehouse\n\n"
                "If you want, we can add a 'Run Fabric Pipeline via REST' button next.\n"
            )
            with open(os.path.join(out_prefix, "02_pipeline_notes.txt"), "w", encoding="utf-8") as f:
                f.write(pipe_notes)

            readme = (
                "Data Wizard — Fabric Bundle\n"
                "===========================\n\n"
                f"Generated at (UTC): {now}\n\n"
                "Files:\n"
                "- fabric_params.json\n"
                "- field_mappings.json\n"
                "- field_mappings.csv\n"
                "- 00_lakehouse.sql\n"
                "- 01_dataflow_gen2_notes.txt\n"
                "- 02_pipeline_notes.txt\n\n"
                "Next:\n"
                "- Use Dataflow Gen2 to ingest and map fields.\n"
                "- Use the SQL/Notebook placeholders to create tables/transforms.\n"
            )
            with open(os.path.join(out_prefix, "README.txt"), "w", encoding="utf-8") as f:
                f.write(readme)

            self.kernel.log("fabric_bundle_generated", out_dir=out_prefix, lakehouse=lakehouse, table=table, mappings=len(mappings))
            wx.MessageBox(
                "Fabric bundle generated successfully.\n\n"
                f"Folder:\n{out_prefix}\n\n"
                "Created:\n"
                "- fabric_params.json\n"
                "- field_mappings.json\n"
                "- field_mappings.csv\n"
                "- 00_lakehouse.sql\n"
                "- 01_dataflow_gen2_notes.txt\n"
                "- 02_pipeline_notes.txt\n"
                "- README.txt",
                "Fabric Bundle",
                wx.OK | wx.ICON_INFORMATION
            )
        except Exception as e:
            import traceback
            wx.MessageBox(f"Failed to generate Fabric bundle:\n\n{e}\n\n{traceback.format_exc()}",
                          "Fabric Bundle", wx.OK | wx.ICON_ERROR)
            self.kernel.log("fabric_bundle_failed", error=str(e))

    # ──────────────────────────────────────────────────────────────────────────
    # NEW: Microsoft Purview export (catalog meta + quality rules + mappings)
    # ──────────────────────────────────────────────────────────────────────────


    # ──────────────────────────────────────────────────────────────────────────
    # NEW: Purview Export (metadata export for Microsoft Purview bulk import / reference)
    # ──────────────────────────────────────────────────────────────────────────

    def on_purview_export(self, _evt=None):
        """Export current catalog/mapping metadata into Purview-friendly CSV + JSON.

        Notes:
        - This does NOT call Purview APIs (no credentials needed).
        - Output can be used as a starting point for bulk entity creation, documentation, or import tooling.
        """
        # must have some dataset / fields
        if (not self.headers) and self.grid.GetNumberCols() == 0:
            wx.MessageBox("Load data first so we have fields to export.", "Purview Export",
                          wx.OK | wx.ICON_WARNING)
            return

        # Collect a couple identifiers for qualifiedName shaping
        with wx.TextEntryDialog(self, "Enter a Source System / Collection name (ex: CRM, ERP, S3_Lake):",
                                "Purview Export", value="DATA_WIZARD") as d:
            if d.ShowModal() != wx.ID_OK:
                return
            source_system = (d.GetValue() or "").strip() or "DATA_WIZARD"

        with wx.TextEntryDialog(self, "Enter a Dataset/Table name (ex: customers, orders):",
                                "Purview Export", value="dataset") as d:
            if d.ShowModal() != wx.ID_OK:
                return
            dataset_name = (d.GetValue() or "").strip() or "dataset"

        # choose output folder
        dd = wx.DirDialog(self, "Choose output folder for Purview export",
                          style=wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
        if dd.ShowModal() != wx.ID_OK:
            dd.Destroy()
            return
        out_dir = dd.GetPath()
        dd.Destroy()

        try:
            now = datetime.utcnow().isoformat() + "Z"
            mappings = self._extract_mapping_from_current_view()

            # Compile regex rules (best-effort) for reference
            rules = self._compile_rules()

            rows = []
            for m in mappings:
                src = (m.get("source_field") or "").strip()
                tgt = (m.get("target_field") or src).strip()
                desc = (m.get("description") or "").strip()
                nullable = (m.get("nullable") or "").strip()
                sla = (m.get("sla") or "").strip()

                dtype_hint = (m.get("catalog_data_type") or m.get("target_type") or "").strip()
                # Purview is typeName-based; keep a friendly dtype column for humans/tools
                dtype_out = dtype_hint or "string"

                rx = rules.get(src) or rules.get(tgt)
                rx_pat = getattr(rx, "pattern", "") if rx is not None else ""

                # Very simple qualifiedName scheme you can change later
                # (Purview requires unique qualifiedName per asset)
                qn = f"{source_system}::{dataset_name}::{tgt or src}"

                rows.append({
                    "typeName": "DataSetColumn",
                    "qualifiedName": qn,
                    "name": tgt or src,
                    "sourceField": src,
                    "description": desc,
                    "dataType": dtype_out,
                    "nullable": nullable,
                    "sla": sla,
                    "qualityRuleRegex": rx_pat,
                    "generatedAtUtc": now,
                })

            safe_source = self._safe_filename(source_system, default="source")
            safe_dataset = self._safe_filename(dataset_name, default="dataset")
            out_prefix = os.path.join(out_dir, f"purview_{safe_source}_{safe_dataset}")
            os.makedirs(out_prefix, exist_ok=True)

            csv_path = os.path.join(out_prefix, "purview_columns.csv")
            pd.DataFrame(rows).to_csv(csv_path, index=False)

            json_path = os.path.join(out_prefix, "purview_export.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump({
                    "generated_at": now,
                    "app": "Data Wizard — Sidecar Application",
                    "source_system": source_system,
                    "dataset": dataset_name,
                    "rows": len(rows),
                    "columns": rows,
                }, f, ensure_ascii=False, indent=2)

            readme_path = os.path.join(out_prefix, "README.txt")
            with open(readme_path, "w", encoding="utf-8") as f:
                f.write(
                    "Data Wizard — Purview Export\n"
                    "===========================\n\n"
                    f"Generated at (UTC): {now}\n\n"
                    "Files:\n"
                    "- purview_columns.csv   (tabular export of column metadata)\n"
                    "- purview_export.json   (same data as JSON)\n\n"
                    "Notes:\n"
                    "- This is a metadata export for Microsoft Purview-style workflows.\n"
                    "- qualifiedName format used here is: <SourceSystem>::<Dataset>::<Column>\n"
                    "- Update the qualifiedName convention to match your organization's Purview naming standard.\n"
                    "- qualityRuleRegex is best-effort from Rule Assignment (if rules were set).\n"
                )

            self.kernel.log(
                "purview_export_generated",
                out_dir=out_prefix,
                source_system=source_system,
                dataset=dataset_name,
                rows=len(rows),
            )

            wx.MessageBox(
                "Purview export generated successfully.\n\n"
                f"Folder:\n{out_prefix}\n\n"
                "Created:\n"
                "- purview_columns.csv\n"
                "- purview_export.json\n"
                "- README.txt",
                "Purview Export",
                wx.OK | wx.ICON_INFORMATION
            )

        except Exception as e:
            import traceback
            wx.MessageBox(
                f"Failed to generate Purview export:\n\n{e}\n\n{traceback.format_exc()}",
                "Purview Export",
                wx.OK | wx.ICON_ERROR
            )
            self.kernel.log("purview_export_failed", error=str(e))
    def on_generate_purview_export(self, _evt=None):
        """
        Exports Sidecar metadata in a Purview-friendly package (best-effort):
        - purview_export.json  (catalog meta + rules + mappings)
        - purview_catalog.csv  (Field, Friendly Name, Description, Data Type, Nullable, SLA)
        - purview_rules.csv    (Field, Rule)
        - field_mappings.csv   (reuse)
        - README.txt
        """
        if (not self.headers) and self.grid.GetNumberCols() == 0 and not self._load_catalog_meta():
            wx.MessageBox("Load data or run Catalog first so we have metadata to export.", "Purview Export",
                          wx.OK | wx.ICON_WARNING)
            return

        dlg = PurviewExportDialog(self)
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy()
            return
        params = dlg.get_params()
        dlg.Destroy()

        dd = wx.DirDialog(self, "Choose output folder for the Purview export",
                          style=wx.DD_DEFAULT_STYLE | wx.DD_DIR_MUST_EXIST)
        if dd.ShowModal() != wx.ID_OK:
            dd.Destroy()
            return
        out_dir = dd.GetPath()
        dd.Destroy()

        try:
            now = datetime.utcnow().isoformat() + "Z"
            export_name = params.get("export_name") or "data_wizard_purview_export"
            safe_name = self._safe_filename(export_name, default="purview_export")
            out_prefix = os.path.join(out_dir, safe_name)
            os.makedirs(out_prefix, exist_ok=True)

            mappings = self._extract_mapping_from_current_view() if (self.headers or self.grid.GetNumberCols() > 0) else []
            meta = self._load_catalog_meta()
            rules = {k: (getattr(v, "pattern", None) or str(v)) for k, v in (self.quality_rules or {}).items()}

            # Build a flattened catalog table from meta (and/or mappings)
            rows = []
            # prefer meta keys; if empty, fall back to mappings
            if meta:
                for field, vals in meta.items():
                    rows.append({
                        "Field": field,
                        "Friendly Name": vals.get("Friendly Name", ""),
                        "Description": vals.get("Description", ""),
                        "Data Type": vals.get("Data Type", ""),
                        "Nullable": vals.get("Nullable", ""),
                        "SLA": vals.get("SLA", ""),
                    })
            else:
                for m in mappings:
                    rows.append({
                        "Field": m.get("source_field",""),
                        "Friendly Name": m.get("target_field",""),
                        "Description": m.get("description",""),
                        "Data Type": m.get("catalog_data_type","") or m.get("target_type",""),
                        "Nullable": m.get("nullable",""),
                        "SLA": m.get("sla",""),
                    })

            pd.DataFrame(rows).to_csv(os.path.join(out_prefix, "purview_catalog.csv"), index=False)

            rules_rows = [{"Field": k, "Rule": v} for k, v in rules.items()]
            pd.DataFrame(rules_rows).to_csv(os.path.join(out_prefix, "purview_rules.csv"), index=False)

            if mappings:
                pd.DataFrame(mappings).to_csv(os.path.join(out_prefix, "field_mappings.csv"), index=False)

            export_obj = {
                "generated_at": now,
                "generated_by": "Data Wizard",
                "export": params,
                "catalog_meta": meta,
                "quality_rules": rules,
                "mappings": mappings,
            }
            with open(os.path.join(out_prefix, "purview_export.json"), "w", encoding="utf-8") as f:
                json.dump(export_obj, f, ensure_ascii=False, indent=2)

            readme = (
                "Data Wizard — Purview Export\n"
                "============================\n\n"
                f"Generated at (UTC): {now}\n\n"
                "This is a best-effort package to help you load metadata into Microsoft Purview.\n\n"
                "Files:\n"
                "- purview_export.json  (full export)\n"
                "- purview_catalog.csv  (field-level metadata)\n"
                "- purview_rules.csv    (quality rules as regex / text)\n"
                "- field_mappings.csv   (if available)\n\n"
                "Next:\n"
                "- Use Purview Data Map bulk update patterns or automation to apply descriptions/classifications.\n"
                "- If you want, we can add: Purview CSV template formats (entities/attributes) and REST API push.\n"
            )
            with open(os.path.join(out_prefix, "README.txt"), "w", encoding="utf-8") as f:
                f.write(readme)

            self.kernel.log("purview_export_generated", out_dir=out_prefix, fields=len(rows), rules=len(rules))
            wx.MessageBox(
                "Purview export generated successfully.\n\n"
                f"Folder:\n{out_prefix}\n\n"
                "Created:\n"
                "- purview_export.json\n"
                "- purview_catalog.csv\n"
                "- purview_rules.csv\n"
                "- (optional) field_mappings.csv\n"
                "- README.txt",
                "Purview Export",
                wx.OK | wx.ICON_INFORMATION
            )
        except Exception as e:
            import traceback
            wx.MessageBox(f"Failed to generate Purview export:\n\n{e}\n\n{traceback.format_exc()}",
                          "Purview Export", wx.OK | wx.ICON_ERROR)
            self.kernel.log("purview_export_failed", error=str(e))

    # ──────────────────────────────────────────────────────────────────────────
    # DBT integration (NEW)
    # ──────────────────────────────────────────────────────────────────────────

    def _default_dbt_project_dir(self) -> str:
        """Best-effort path to the dbt project within this repo."""
        try:
            root = Path(__file__).resolve().parents[1]  # repo root (../)
        except Exception:
            root = Path(os.getcwd())
        return str(root / "dbt" / "sidecar_dbt")

    def on_dbt_menu(self, _evt=None):
        """DBT actions menu."""
        menu = wx.Menu()
        m_generate = menu.Append(wx.ID_ANY, "Generate dbt models from current mapping…")
        m_run      = menu.Append(wx.ID_ANY, "dbt run")
        m_test     = menu.Append(wx.ID_ANY, "dbt test")
        m_build    = menu.Append(wx.ID_ANY, "dbt build")
        menu.AppendSeparator()
        m_open     = menu.Append(wx.ID_ANY, "Open dbt project folder")
        self.Bind(wx.EVT_MENU, lambda e: self.on_dbt_generate_models(), m_generate)
        self.Bind(wx.EVT_MENU, lambda e: self.on_dbt_run_cmd("run"), m_run)
        self.Bind(wx.EVT_MENU, lambda e: self.on_dbt_run_cmd("test"), m_test)
        self.Bind(wx.EVT_MENU, lambda e: self.on_dbt_run_cmd("build"), m_build)
        self.Bind(wx.EVT_MENU, lambda e: self.on_dbt_open_folder(), m_open)
        self.PopupMenu(menu)
        menu.Destroy()

    def on_dbt_open_folder(self, _evt=None):
        p = self._default_dbt_project_dir()
        try:
            if os.path.isdir(p):
                os.startfile(p)  # Windows
            else:
                wx.MessageBox(f"dbt project folder not found:\n{p}", "DBT", wx.OK | wx.ICON_WARNING)
        except Exception as e:
            wx.MessageBox(f"Could not open folder:\n{e}", "DBT", wx.OK | wx.ICON_ERROR)

        def on_dbt_generate_models(self, _evt=None):
            """Generate dbt models + tests based on current mapping and (optionally) quality rules.

            NOTE: app.dbt_generator is optional. If it's not installed, we fall back to a built-in writer
            that creates a simple staging model + schema.yml (+ optional regex macro).
            """
            if (not self.headers) and self.grid.GetNumberCols() == 0:
                wx.MessageBox("Load data first so we have fields to map.", "DBT", wx.OK | wx.ICON_WARNING)
                return

            dlg = DbtGenerateDialog(self, default_project_dir=self._default_dbt_project_dir())
            if dlg.ShowModal() != wx.ID_OK:
                dlg.Destroy()
                return
            opts = dlg.get_params()
            dlg.Destroy()

            project_dir = opts.get("project_dir") or self._default_dbt_project_dir()
            if not os.path.exists(os.path.join(project_dir, "dbt_project.yml")):
                wx.MessageBox(
                    "That folder doesn't look like a dbt project (dbt_project.yml not found).\n\n"
                    f"Folder:\n{project_dir}",
                    "DBT",
                    wx.OK | wx.ICON_ERROR
                )
                return

            try:
                mappings = self._extract_mapping_from_current_view()
                # Quality rules may contain compiled regex OR raw strings
                rules = self._compile_rules()

                # If the optional external generator exists, use it.
                if callable(generate_dbt_models):
                    out = generate_dbt_models(
                        dbt_project_dir=project_dir,
                        dataset_name=opts.get("dataset_name") or "sidecar",
                        source_database=opts.get("source_database") or "",
                        source_schema=opts.get("source_schema") or "",
                        source_table=opts.get("source_table") or "",
                        target_schema=opts.get("target_schema") or "",
                        mappings=mappings,
                        quality_rules=dict(self.quality_rules or {}),
                        materialization=opts.get("materialization") or "view",
                    )
                    self.kernel.log("dbt_models_generated", via="dbt_generator", project_dir=project_dir, **(out or {}))
                    wx.MessageBox(
                        "dbt files generated successfully.\n\n"
                        f"Project: {project_dir}\n"
                        f"Models folder: {(out or {}).get('models_dir','models')}",
                        "DBT",
                        wx.OK | wx.ICON_INFORMATION
                    )
                    return

                # ── Fallback generator (no app.dbt_generator installed) ───────────────────
                now = datetime.utcnow().isoformat() + "Z"
                dataset = self._dbt_safe_name(opts.get("dataset_name") or "sidecar", default="sidecar")
                src_db = (opts.get("source_database") or "").strip()
                src_schema = (opts.get("source_schema") or "").strip()
                src_table = (opts.get("source_table") or "").strip() or "RAW_TABLE"
                tgt_schema = (opts.get("target_schema") or "").strip() or src_schema or "PUBLIC"
                materialized = (opts.get("materialization") or "view").strip().lower()

                model_name = self._dbt_safe_name(f"stg_{dataset}_{src_table}", default="stg_data_wizard")
                models_dir = os.path.join(project_dir, "models", "data_wizard")
                macros_dir = os.path.join(project_dir, "macros", "data_wizard")
                out_dir = os.path.join(project_dir, "target", "data_wizard_generate_out")
                os.makedirs(models_dir, exist_ok=True)
                os.makedirs(macros_dir, exist_ok=True)
                os.makedirs(out_dir, exist_ok=True)

                # Source relation: best-effort fully-qualified (works for Snowflake)
                # If you prefer dbt sources, set Source Relation in the Bundle dialog and use "dbt Bundle".
                if src_db and src_schema and src_table:
                    src_relation = f'{src_db}.{src_schema}.{src_table}'
                elif src_schema and src_table:
                    src_relation = f'{src_schema}.{src_table}'
                else:
                    src_relation = src_table

                # Build SELECT list (Snowflake-friendly TRY_CAST)
                select_lines = []
                for m in mappings:
                    src = (m.get("source_field") or "").strip()
                    if not src:
                        continue
                    tgt = (m.get("target_field") or src).strip()
                    tgt = self._dbt_safe_name(tgt, default=src or "col")
                    dtype_hint = (m.get("catalog_data_type") or m.get("target_type") or "").strip()
                    sf_type = self._guess_snowflake_type_from_catalog(dtype_hint) if dtype_hint else "VARCHAR"
                    select_lines.append(f'  TRY_CAST({{{{ adapter.quote("{src}") }}}} AS {sf_type}) AS {tgt}')
                if not select_lines:
                    select_sql = "  *"
                else:
                    select_sql = ",\n".join(select_lines)

                sql = (
                    """{{{{ config(materialized='{materialized}', schema='{tgt_schema}') }}}}

    WITH src AS (
      SELECT * FROM {src_relation}
    )
    SELECT
    {select_sql}
    FROM src
    """.format(
                        materialized=materialized,
                        tgt_schema=tgt_schema,
                        src_relation=src_relation,
                        select_sql=select_sql,
                    )
                )
                self._write_text(os.path.join(models_dir, f"{model_name}.sql"), sql)

                # Build schema.yml (tests)
                cols_yml = []
                need_regex_macro = False
                for m in mappings:
                    src = (m.get("source_field") or "").strip()
                    if not src:
                        continue
                    tgt = (m.get("target_field") or src).strip()
                    tgt = self._dbt_safe_name(tgt, default=src or "col")
                    desc = (m.get("description") or "").strip()
                    nullable = str(m.get("nullable") or "").strip().lower()

                    tests = []
                    # Catalog-driven not_null
                    if nullable in ("no", "n", "false", "not null", "non-null", "nonnullable"):
                        tests.append("not_null")

                    # Heuristic: id fields should be not_null + unique
                    if tgt.lower() in ("id",) or tgt.lower().endswith("_id"):
                        if "not_null" not in tests:
                            tests.append("not_null")
                        tests.append("unique")

                    # Regex from Quality Rules (best-effort)
                    rx = rules.get(src) or rules.get(tgt)
                    if rx is not None:
                        need_regex_macro = True
                        pattern_str = getattr(rx, "pattern", None) or str(rx)
                        # YAML escape
                        pattern_str = pattern_str.replace('\\', '\\\\')
                        tests.append({"regex_match": {"regex": pattern_str}})

                    entry = {
                        "name": tgt,
                        "description": desc or f"Generated from {src}",
                    }
                    if tests:
                        entry["tests"] = tests
                    cols_yml.append(entry)

                def y(s):
                    return (s or "").replace("\\", "\\\\").replace('"', '\\"')

                lines = []
                lines.append("version: 2")
                lines.append("")
                lines.append("models:")
                lines.append(f"  - name: {model_name}")
                lines.append(f"    description: \"Generated by Data Wizard (fallback dbt generator).\"")
                lines.append("    columns:")
                for col in cols_yml:
                    lines.append(f"      - name: {col['name']}")
                    lines.append(f"        description: \"{y(col.get('description',''))}\"")
                    tests = col.get("tests") or []
                    if tests:
                        lines.append("        tests:")
                        for t in tests:
                            if isinstance(t, str):
                                lines.append(f"          - {t}")
                            elif isinstance(t, dict) and "regex_match" in t:
                                rxv = t["regex_match"].get("regex", "")
                                lines.append("          - regex_match:")
                                lines.append(f"              regex: \"{y(rxv)}\"")
                schema_yml = "\n".join(lines) + "\n"
                self._write_text(os.path.join(models_dir, "schema.yml"), schema_yml)

                if need_regex_macro:
                    macro = """{% test regex_match(model, column_name, regex) %}
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and not regexp_like({{ column_name }}, regex)
    {% endtest %}
    """
                    self._write_text(os.path.join(macros_dir, "regex_match.sql"), macro)

                readme = f"""Data Wizard — dbt Generate Output (fallback)
    ========================================

    Generated at (UTC): {now}

    Created:
    - models/data_wizard/{model_name}.sql
    - models/data_wizard/schema.yml
    - macros/data_wizard/regex_match.sql (only if regex rules existed)

    How to run:
      cd {project_dir}
      dbt run -s {model_name}
      dbt test -s {model_name}

    Source relation (best-effort):
      {src_relation}

    Notes:
    - This fallback does not create sources.yml. If you want sources-based models, use 'dbt Bundle'.
    """
                self._write_text(os.path.join(out_dir, "README.txt"), readme)

                self.kernel.log(
                    "dbt_models_generated",
                    via="fallback_writer",
                    project_dir=project_dir,
                    model=model_name,
                    models_dir=models_dir,
                    macros_dir=macros_dir,
                )
                wx.MessageBox(
                    "dbt files generated (fallback writer).\n\n"
                    f"Project: {project_dir}\n"
                    f"Model: {model_name}\n\n"
                    "Next: DBT -> dbt run / dbt test",
                    "DBT",
                    wx.OK | wx.ICON_INFORMATION
                )

            except Exception as e:
                import traceback
                wx.MessageBox(f"DBT generate failed:\n\n{e}\n\n{traceback.format_exc()}",
                              "DBT", wx.OK | wx.ICON_ERROR)
                self.kernel.log("dbt_models_generate_failed", error=str(e))
    def on_dbt_run_cmd(self, cmd: str, _evt=None):
        """Run a dbt command (run/test/build) and stream logs in a dialog."""
        project_dir = self._default_dbt_project_dir()

        dlg = DbtRunDialog(self, default_project_dir=project_dir, default_cmd=cmd)
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy()
            return
        opts = dlg.get_params()
        dlg.Destroy()

        project_dir = opts.get("project_dir") or project_dir
        cmd = opts.get("cmd") or cmd
        target = opts.get("target") or None
        profiles_dir = opts.get("profiles_dir") or None

        self.kernel.log("dbt_command_started", cmd=cmd, project_dir=project_dir, target=target)

        # Run in background thread
        def worker():
            from app.dbt_runner import run_dbt_capture
            rc, output = run_dbt_capture(project_dir=project_dir, cmd=cmd, target=target, profiles_dir=profiles_dir)
            def done():
                title = "DBT Success" if rc == 0 else "DBT Failed"
                icon = wx.ICON_INFORMATION if rc == 0 else wx.ICON_ERROR
                wx.MessageBox(f"{title}.\n\nExit code: {rc}\n\nSee the log window for details.",
                              "DBT", wx.OK | icon)
            wx.CallAfter(self._show_dbt_log, f"dbt {cmd}", output, rc)
            wx.CallAfter(done)
            self.kernel.log("dbt_command_finished", cmd=cmd, rc=rc)
        threading.Thread(target=worker, daemon=True).start()

    def _show_dbt_log(self, title: str, text: str, rc: int):
        dlg = DbtLogDialog(self, title=f"{title} (exit {rc})", text=text)
        dlg.ShowModal()
        dlg.Destroy()

    # Tasks / export / upload
    def on_run_tasks(self, _evt=None):
        dlg = wx.FileDialog(self, "Open Tasks File",
                            wildcard="Tasks (*.json;*.txt)|*.json;*.txt|All|*.*",
                            style=wx.FD_OPEN | wx.FD_FILE_MUST_EXIST)
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy(); return
        path = dlg.GetPath(); dlg.Destroy()

        try:
            tasks = self._load_tasks_from_file(path)
        except Exception as e:
            wx.MessageBox(f"Could not read tasks file:\n{e}", "Tasks", wx.OK | wx.ICON_ERROR); return
        self.kernel.log("tasks_started", path=path, steps=len(tasks))
        threading.Thread(target=self._run_tasks_worker, args=(tasks,), daemon=True).start()

    def _load_tasks_from_file(self, path: str):
        text = open(path, "r", encoding="utf-8", errors="ignore").read().strip()
        if not text: return []
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                obj = obj.get("tasks") or obj.get("steps") or obj.get("actions") or []
            if not isinstance(obj, list):
                raise ValueError("JSON must be a list of task objects")
            out = []
            for it in obj:
                if not isinstance(it, dict) or "action" not in it:
                    raise ValueError("Each JSON task must be an object with 'action'")
                t = {k: v for k, v in it.items()}
                t["action"] = str(t["action"]).strip()
                out.append(t)
            return out
        except Exception:
            pass

        tasks=[]
        for line in text.splitlines():
            line=line.strip()
            if not line or line.startswith("#"): continue
            parts=line.split(maxsplit=1)
            action=parts[0]; arg=parts[1] if len(parts)==2 else None
            t={"action": action}
            if arg:
                if action.lower() in ("loadfile","exportcsv","exporttxt"):
                    t["path"]=arg
                elif action.lower() in ("loads3","loaduri"):
                    t["uri"]=arg
                else:
                    t["arg"]=arg
            tasks.append(t)
        return tasks

    def _run_tasks_worker(self, tasks):
        ran = 0
        for i, t in enumerate(tasks, 1):
            try:
                act = (t.get("action") or "").strip().lower()
                if act == "loadfile":
                    p = t.get("path") or t.get("file")
                    if not p: raise ValueError("LoadFile requires 'path'")
                    text = self._load_text_file(p)
                    self.headers, self.raw_data = detect_and_split_data(text)
                    wx.CallAfter(self._display, self.headers, self.raw_data)
                    wx.CallAfter(self._reset_kpis_for_new_dataset, self.headers, self.raw_data)

                elif act in ("loads3", "loaduri"):
                    uri = t.get("uri") or t.get("path")
                    if not uri: raise ValueError("LoadS3/LoadURI requires 'uri'")
                    text = download_text_from_uri(uri)
                    self.headers, self.raw_data = detect_and_split_data(text)
                    wx.CallAfter(self._display, self.headers, self.raw_data)
                    wx.CallAfter(self._reset_kpis_for_new_dataset, self.headers, self.raw_data)

                elif act in ("profile", "quality", "catalog", "compliance", "detectanomalies"):
                    name = {"detectanomalies": "Detect Anomalies"}.get(act, act.capitalize())
                    wx.CallAfter(self.do_analysis_process, name)

                elif act == "exportcsv":
                    p = t.get("path")
                    if not p: raise ValueError("ExportCSV requires 'path'")
                    wx.CallAfter(self._export_to_path, p, ",")

                elif act == "exporttxt":
                    p = t.get("path")
                    if not p: raise ValueError("ExportTXT requires 'path'")
                    wx.CallAfter(self._export_to_path, p, "\t")

                elif act == "uploads3":
                    wx.CallAfter(self.on_upload_s3, None)

                elif act == "sleep":
                    import time
                    time.sleep(float(t.get("seconds", 1)))

                else:
                    raise ValueError(f"Unknown action: {t.get('action')}")

                ran += 1
            except Exception as e:
                wx.CallAfter(wx.MessageBox, f"Tasks stopped at step {i}:\n{t}\n\n{e}",
                             "Tasks", wx.OK | wx.ICON_ERROR)
                self.kernel.log("tasks_failed", step=i, action=t.get("action"), error=str(e))
                return

        self.kernel.log("tasks_completed", steps=ran)
        wx.CallAfter(wx.MessageBox, f"Tasks completed. {ran} step(s) executed.",
                     "Tasks", wx.OK | wx.ICON_INFORMATION)

    def _export_to_path(self, path: str, sep: str):
        try:
            hdr = [self.grid.GetColLabelValue(i) for i in range(self.grid.GetNumberCols())]
            data = [[self.grid.GetCellValue(r, c) for c in range(len(hdr))]
                    for r in range(self.grid.GetNumberRows())]
            pd.DataFrame(data, columns=hdr).to_csv(path, index=False, sep=sep)
            self.kernel.log("export_to_path", path=path, sep=sep, rows=len(data), cols=len(hdr))
        except Exception as e:
            wx.MessageBox(f"Export failed: {e}", "Export", wx.OK | wx.ICON_ERROR)

    # Export menu (CSV/TSV file, S3, or HTTP PUT)
    def on_export_menu(self, evt=None):
        if self.grid.GetNumberCols() == 0:
            wx.MessageBox("There is nothing to export yet.", "Export", wx.OK | wx.ICON_INFORMATION)
            return
        menu = wx.Menu()
        m_csv = menu.Append(wx.ID_ANY, "Save as CSV…")
        m_tsv = menu.Append(wx.ID_ANY, "Save as TSV…")
        menu.AppendSeparator()
        m_s3  = menu.Append(wx.ID_ANY, "Export to S3…")
        m_uri = menu.Append(wx.ID_ANY, "PUT to URI (HTTP)…")
        self.Bind(wx.EVT_MENU, lambda e: self._export_save_dialog(','), m_csv)
        self.Bind(wx.EVT_MENU, lambda e: self._export_save_dialog('\t'), m_tsv)
        self.Bind(wx.EVT_MENU, lambda e: self.on_upload_s3(), m_s3)
        self.Bind(wx.EVT_MENU, lambda e: self._export_to_uri_http(), m_uri)
        self.PopupMenu(menu)
        menu.Destroy()

    def _export_save_dialog(self, sep=','):
        dlg = wx.FileDialog(self, "Save data",
                            wildcard="CSV (*.csv)|*.csv|TSV (*.tsv)|*.tsv|All|*.*",
                            style=wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT)
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy(); return
        path = dlg.GetPath(); dlg.Destroy()
        if sep == ',' and not path.lower().endswith('.csv'):
            path += '.csv'
        if sep == '\t' and not path.lower().endswith('.tsv'):
            path += '.tsv'
        self._export_to_path(path, sep)

    def _export_to_uri_http(self):
        dlg = wx.TextEntryDialog(self, "Enter HTTP(S) URI to PUT CSV to:",
                                 "Export to URI (HTTP PUT)")
        if dlg.ShowModal() != wx.ID_OK:
            dlg.Destroy(); return
        uri = dlg.GetValue().strip()
        dlg.Destroy()
        if not uri:
            return
        try:
            import io
            hdr = [self.grid.GetColLabelValue(i) for i in range(self.grid.GetNumberCols())]
            data = [[self.grid.GetCellValue(r, c) for c in range(len(hdr))]
                    for r in range(self.grid.GetNumberRows())]
            buf = io.StringIO()
            pd.DataFrame(data, columns=hdr).to_csv(buf, index=False)
            payload = buf.getvalue().encode('utf-8')
            resp = requests.put(uri, data=payload, headers={'Content-Type':'text/csv'})
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            wx.MessageBox("Exported to URI successfully.", "Export", wx.OK | wx.ICON_INFORMATION)
            self.kernel.log("export_uri", uri=uri, rows=len(data), cols=len(hdr))
        except Exception as e:
            wx.MessageBox(f"Export to URI failed:\n{e}", "Export", wx.OK | wx.ICON_ERROR)

    def on_upload_s3(self, _evt=None):
        hdr = [self.grid.GetColLabelValue(i) for i in range(self.grid.GetNumberCols())]
        data = [[self.grid.GetCellValue(r, c) for c in range(len(hdr))] for r in range(self.grid.GetNumberRows())]
        try:
            msg = upload_to_s3(self.current_process or "Unknown", hdr, data)
            wx.MessageBox(msg, "Upload", wx.OK | wx.ICON_INFORMATION)
            self.kernel.log("upload_s3", rows=len(data), cols=len(hdr), process=self.current_process or "Unknown")
        except Exception as e:
            wx.MessageBox(f"Upload failed: {e}", "Upload", wx.OK | wx.ICON_ERROR)

    # Grid events/presentation
    def on_cell_changed(self, evt):
        # Persist Catalog edits
        if self.current_process == "Catalog":
            try:
                row = evt.GetRow()
                col = evt.GetCol()
                hdr = [self.grid.GetColLabelValue(i) for i in range(self.grid.GetNumberCols())]
                col_name = hdr[col]
                if col_name not in {"Friendly Name", "Description", "Data Type", "Nullable", "SLA"}:
                    evt.Skip(); return
                try:
                    f_idx = hdr.index("Field")
                except ValueError:
                    evt.Skip(); return
                field_name = self.grid.GetCellValue(row, f_idx).strip()
                new_val = self.grid.GetCellValue(row, col)
                if not field_name:
                    evt.Skip(); return
                meta = self._load_catalog_meta()
                meta.setdefault(field_name, {})
                meta[field_name][col_name] = new_val
                self._save_catalog_meta(meta)
            finally:
                evt.Skip()
        else:
            evt.Skip()

    def _display(self, hdr, data):
        # allow pd.DataFrame too
        if isinstance(hdr, pd.DataFrame):
            df = hdr; hdr = list(df.columns); data = df.values.tolist()
        if isinstance(hdr, tuple) and len(hdr) == 2:
            hdr, data = hdr

        self.grid.ClearGrid()
        if self.grid.GetNumberRows(): self.grid.DeleteRows(0, self.grid.GetNumberRows())
        if self.grid.GetNumberCols(): self.grid.DeleteCols(0, self.grid.GetNumberCols())

        if not isinstance(hdr, (list, tuple)) or len(hdr) == 0:
            self._render_kpis()
            self._show_catalog_toolbar(False)
            return

        self.grid.AppendCols(len(hdr))
        for i, h in enumerate(hdr): self.grid.SetColLabelValue(i, str(h))
        self.grid.AppendRows(len(data))

        try: anom_idx = hdr.index("__anomaly__")
        except ValueError: anom_idx = -1

        for r, row in enumerate(data):
            row_has_anom = False
            if anom_idx >= 0 and anom_idx < len(row):
                row_has_anom = bool(str(row[anom_idx]).strip())
            for c, val in enumerate(row):
                self.grid.SetCellValue(r, c, "" if val is None else str(val))
                base = wx.Colour(255,255,255) if r%2==0 else wx.Colour(248,246,255)
                if row_has_anom: base = wx.Colour(255,235,238)
                self.grid.SetCellBackgroundColour(r, c, base)

        self.adjust_grid(); self._render_kpis()
        self.grid.EnableEditing(self.current_process == "Catalog")

    def adjust_grid(self):
        cols = self.grid.GetNumberCols()
        if cols == 0: return
        total_w = self.grid.GetClientSize().GetWidth()
        usable = max(0, total_w - self.grid.GetRowLabelSize())
        w = max(80, usable // cols)
        for c in range(cols): self.grid.SetColSize(c, w)

    def on_grid_resize(self, event):
        event.Skip(); wx.CallAfter(self.adjust_grid)



# ──────────────────────────────────────────────────────────────────────────────
# dbt bundle dialog (NEW)
# ──────────────────────────────────────────────────────────────────────────────

class DbtBundleDialog(wx.Dialog):
    def __init__(self, parent, default_project_path=""):
        super().__init__(
            parent,
            title="Generate dbt Bundle",
            style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER
        )
        self.SetMinSize((660, 420))

        outer = wx.BoxSizer(wx.VERTICAL)

        panel = wx.Panel(self)
        v = wx.BoxSizer(wx.VERTICAL)

        hint = wx.StaticText(panel, label=(
            "Generates a starter dbt model + schema.yml from your current grid mappings.\n"
            "Tip: Run Catalog first so Data Type / Nullable / Description / SLA are included.\n"
            "Tip: Quality Rule Assignment regex rules can become dbt tests (optional)."
        ))
        v.Add(hint, 0, wx.ALL, 10)

        grid = wx.FlexGridSizer(0, 2, 8, 10)
        grid.AddGrowableCol(1, 1)

        def add_row(label, ctrl):
            grid.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid.Add(ctrl, 1, wx.EXPAND)

        self.txt_project = wx.TextCtrl(panel, value=default_project_path)
        btn_browse = wx.Button(panel, label="Browse…")

        prj_line = wx.BoxSizer(wx.HORIZONTAL)
        prj_line.Add(self.txt_project, 1, wx.EXPAND | wx.RIGHT, 8)
        prj_line.Add(btn_browse, 0)

        grid.Add(wx.StaticText(panel, label="dbt Project Folder"), 0, wx.ALIGN_CENTER_VERTICAL)
        grid.Add(prj_line, 1, wx.EXPAND)

        self.txt_source_relation = wx.TextCtrl(panel, value='{{ source("raw", "your_table") }}')
        add_row("Source Relation", self.txt_source_relation)

        self.txt_model = wx.TextCtrl(panel, value="stg_data_buddy")
        add_row("Model Name", self.txt_model)

        self.cbo_mat = wx.Choice(panel, choices=["view", "table"])
        self.cbo_mat.SetSelection(0)
        add_row("Materialized", self.cbo_mat)

        self.chk_regex = wx.CheckBox(panel, label="Create regex tests from Quality Rules (best-effort)")
        self.chk_regex.SetValue(True)
        grid.Add(wx.StaticText(panel, label="Quality Tests"), 0, wx.ALIGN_CENTER_VERTICAL)
        grid.Add(self.chk_regex, 0)

        v.Add(grid, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.BOTTOM, 10)

        v.Add(wx.StaticLine(panel), 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.TOP, 8)

        btns = wx.StdDialogButtonSizer()
        ok = wx.Button(panel, wx.ID_OK)
        ca = wx.Button(panel, wx.ID_CANCEL)
        btns.AddButton(ok); btns.AddButton(ca); btns.Realize()
        v.Add(btns, 0, wx.ALIGN_RIGHT | wx.ALL, 10)

        panel.SetSizer(v)
        outer.Add(panel, 1, wx.EXPAND)
        self.SetSizer(outer)
        self.Layout()

        def on_browse(_):
            dd = wx.DirDialog(self, "Select dbt project folder (contains dbt_project.yml)")
            if dd.ShowModal() == wx.ID_OK:
                self.txt_project.SetValue(dd.GetPath())
            dd.Destroy()

        btn_browse.Bind(wx.EVT_BUTTON, on_browse)

    def get_params(self):
        return {
            "project_dir": self.txt_project.GetValue().strip(),
            "source_relation": self.txt_source_relation.GetValue().strip(),
            "model_name": self.txt_model.GetValue().strip(),
            "materialized": self.cbo_mat.GetStringSelection(),
            "include_regex_tests": self.chk_regex.GetValue(),
        }


# ──────────────────────────────────────────────────────────────────────────────
# Config dialog (NEW)
# ──────────────────────────────────────────────────────────────────────────────


# ──────────────────────────────────────────────────────────────────────────────
# DBT dialogs (NEW)
# ──────────────────────────────────────────────────────────────────────────────

class DbtGenerateDialog(wx.Dialog):
    def __init__(self, parent, default_project_dir=""):
        super().__init__(parent, title="Generate dbt Models", style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER)
        self.SetMinSize((700, 520))

        outer = wx.BoxSizer(wx.VERTICAL)
        panel = wx.Panel(self)
        panel.SetBackgroundColour(wx.Colour(252, 250, 255))
        v = wx.BoxSizer(wx.VERTICAL)

        hint = wx.StaticText(panel, label=(
            "This generates dbt models from your current field mappings.\n"
            "Use this when your raw data is landing in Snowflake (or another warehouse) and you want Sidecar to generate the dbt project assets.\n"
            "You can then run dbt run/test/build from within Sidecar."
        ))
        v.Add(hint, 0, wx.ALL, 10)

        grid = wx.FlexGridSizer(0, 2, 10, 12)
        grid.AddGrowableCol(1, 1)

        def add_row(label, ctrl):
            grid.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid.Add(ctrl, 1, wx.EXPAND)

        self.txt_project = wx.TextCtrl(panel, value=default_project_dir)
        self.txt_dataset = wx.TextCtrl(panel, value="sidecar")

        self.txt_src_db = wx.TextCtrl(panel, value="DATAWIZARD_DB")
        self.txt_src_schema = wx.TextCtrl(panel, value="DATAWIZARD_SCHEMA")
        self.txt_src_table = wx.TextCtrl(panel, value="RAW_TABLE")

        self.txt_target_schema = wx.TextCtrl(panel, value="DATAWIZARD_SCHEMA")
        self.cbo_mat = wx.Choice(panel, choices=["view", "table", "incremental"])
        self.cbo_mat.SetSelection(0)

        add_row("dbt Project Folder", self.txt_project)
        add_row("Dataset Name (prefix)", self.txt_dataset)
        add_row("Source Database", self.txt_src_db)
        add_row("Source Schema", self.txt_src_schema)
        add_row("Source Table", self.txt_src_table)
        add_row("Target Schema (dbt models)", self.txt_target_schema)
        add_row("Materialization", self.cbo_mat)

        v.Add(grid, 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 10)

        # folder picker
        h = wx.BoxSizer(wx.HORIZONTAL)
        btn_pick = wx.Button(panel, label="Browse…")
        h.Add(btn_pick, 0, wx.RIGHT, 8)
        h.Add(wx.StaticText(panel, label="Tip: choose your repo's dbt/sidecar_dbt folder."), 0, wx.ALIGN_CENTER_VERTICAL)
        v.Add(h, 0, wx.LEFT | wx.RIGHT | wx.TOP, 10)

        def on_pick(_):
            d = wx.DirDialog(self, "Select dbt project directory", defaultPath=self.txt_project.GetValue() or "")
            if d.ShowModal() == wx.ID_OK:
                self.txt_project.SetValue(d.GetPath())
            d.Destroy()
        btn_pick.Bind(wx.EVT_BUTTON, on_pick)

        panel.SetSizer(v)
        outer.Add(panel, 1, wx.EXPAND | wx.ALL, 10)

        btns = wx.StdDialogButtonSizer()
        ok = wx.Button(self, wx.ID_OK)
        ca = wx.Button(self, wx.ID_CANCEL)
        btns.AddButton(ok); btns.AddButton(ca); btns.Realize()
        outer.Add(btns, 0, wx.ALIGN_RIGHT | wx.ALL, 10)
        self.SetSizer(outer)
        self.Layout()

    def get_params(self):
        return {
            "project_dir": self.txt_project.GetValue().strip(),
            "dataset_name": self.txt_dataset.GetValue().strip(),
            "source_database": self.txt_src_db.GetValue().strip(),
            "source_schema": self.txt_src_schema.GetValue().strip(),
            "source_table": self.txt_src_table.GetValue().strip(),
            "target_schema": self.txt_target_schema.GetValue().strip(),
            "materialization": self.cbo_mat.GetStringSelection(),
        }


class DbtRunDialog(wx.Dialog):
    def __init__(self, parent, default_project_dir="", default_cmd="run"):
        super().__init__(parent, title="Run dbt Command", style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER)
        self.SetMinSize((700, 420))

        outer = wx.BoxSizer(wx.VERTICAL)
        panel = wx.Panel(self)
        v = wx.BoxSizer(wx.VERTICAL)

        hint = wx.StaticText(panel, label=(
            "Runs dbt using your installed dbt-core/dbt-snowflake.\n"
            "If you already configured C:\\Users\\<you>\\.dbt\\profiles.yml, you can leave Profiles Dir blank."
        ))
        v.Add(hint, 0, wx.ALL, 10)

        grid = wx.FlexGridSizer(0, 2, 10, 12)
        grid.AddGrowableCol(1, 1)

        def add_row(label, ctrl):
            grid.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid.Add(ctrl, 1, wx.EXPAND)

        self.txt_project = wx.TextCtrl(panel, value=default_project_dir)
        self.cbo_cmd = wx.Choice(panel, choices=["run", "test", "build", "compile", "docs generate"])
        try:
            self.cbo_cmd.SetStringSelection(default_cmd)
        except Exception:
            self.cbo_cmd.SetSelection(0)
        self.txt_target = wx.TextCtrl(panel, value="dev")
        self.txt_profiles = wx.TextCtrl(panel, value="")  # optional

        add_row("dbt Project Folder", self.txt_project)
        add_row("Command", self.cbo_cmd)
        add_row("Target (profiles.yml)", self.txt_target)
        add_row("Profiles Dir (optional)", self.txt_profiles)

        v.Add(grid, 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 10)

        panel.SetSizer(v)
        outer.Add(panel, 1, wx.EXPAND | wx.ALL, 10)

        btns = wx.StdDialogButtonSizer()
        ok = wx.Button(self, wx.ID_OK)
        ca = wx.Button(self, wx.ID_CANCEL)
        btns.AddButton(ok); btns.AddButton(ca); btns.Realize()
        outer.Add(btns, 0, wx.ALIGN_RIGHT | wx.ALL, 10)

        self.SetSizer(outer)
        self.Layout()

    def get_params(self):
        return {
            "project_dir": self.txt_project.GetValue().strip(),
            "cmd": self.cbo_cmd.GetStringSelection(),
            "target": self.txt_target.GetValue().strip(),
            "profiles_dir": self.txt_profiles.GetValue().strip(),
        }


class DbtLogDialog(wx.Dialog):
    def __init__(self, parent, title="DBT Log", text=""):
        super().__init__(parent, title=title, style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER)
        self.SetMinSize((900, 650))
        v = wx.BoxSizer(wx.VERTICAL)

        self.txt = wx.TextCtrl(self, value=text or "", style=wx.TE_MULTILINE | wx.TE_READONLY | wx.HSCROLL)
        v.Add(self.txt, 1, wx.EXPAND | wx.ALL, 10)

        btns = wx.StdDialogButtonSizer()
        ok = wx.Button(self, wx.ID_OK)
        btns.AddButton(ok); btns.Realize()
        v.Add(btns, 0, wx.ALIGN_RIGHT | wx.ALL, 10)

        self.SetSizer(v)
        self.Layout()


class TransformDialog(wx.Dialog):
    def __init__(self, parent, columns=None):
        super().__init__(parent, title="Transform Data", style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER)
        self.SetMinSize((720, 520))
        self.columns = list(columns or [])

        outer = wx.BoxSizer(wx.VERTICAL)
        panel = wx.Panel(self)
        v = wx.BoxSizer(wx.VERTICAL)

        hint = wx.StaticText(panel, label=(
            "Apply common transformations to the currently loaded dataset.\n"
            "Tip: Use Undo/Redo in Actions to roll changes back and forth."
        ))
        v.Add(hint, 0, wx.ALL, 10)

        grid = wx.FlexGridSizer(0, 2, 10, 12)
        grid.AddGrowableCol(1, 1)

        def add_row(label, ctrl):
            grid.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid.Add(ctrl, 1, wx.EXPAND)

        self.cbo_op = wx.Choice(panel, choices=[
            "Trim whitespace (all text)",
            "Normalize column names (snake_case)",
            "Drop duplicate rows",
            "Fill nulls in column",
            "Find & replace (regex)",
            "Cast column to numeric",
            "Parse column as date",
            "Mask PII (email/phone)",
        ])
        self.cbo_op.SetSelection(0)
        add_row("Operation", self.cbo_op)

        self.cbo_col = wx.Choice(panel, choices=self.columns)
        if self.columns:
            self.cbo_col.SetSelection(0)
        add_row("Column (when needed)", self.cbo_col)

        self.txt_value = wx.TextCtrl(panel, value="")
        add_row("Value (Fill nulls)", self.txt_value)

        self.txt_pattern = wx.TextCtrl(panel, value="")
        add_row("Regex Pattern", self.txt_pattern)

        self.txt_repl = wx.TextCtrl(panel, value="")
        add_row("Replacement", self.txt_repl)

        self.txt_datefmt = wx.TextCtrl(panel, value="")
        add_row("Date Format (optional)", self.txt_datefmt)

        self.lst_cols = wx.CheckListBox(panel, choices=self.columns)
        add_row("Mask columns (optional)", self.lst_cols)

        v.Add(grid, 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 10)

        v.Add(wx.StaticLine(panel), 0, wx.EXPAND | wx.ALL, 10)

        btns = wx.StdDialogButtonSizer()
        ok = wx.Button(panel, wx.ID_OK)
        ca = wx.Button(panel, wx.ID_CANCEL)
        btns.AddButton(ok); btns.AddButton(ca); btns.Realize()
        v.Add(btns, 0, wx.ALIGN_RIGHT | wx.ALL, 10)

        panel.SetSizer(v)
        outer.Add(panel, 1, wx.EXPAND)
        self.SetSizer(outer)
        self.Layout()

    def get_params(self):
        op = self.cbo_op.GetStringSelection()
        col = self.cbo_col.GetStringSelection() if self.cbo_col.GetCount() else ""
        checked_cols = [self.lst_cols.GetString(i) for i in range(self.lst_cols.GetCount()) if self.lst_cols.IsChecked(i)]
        return {
            "operation": op,
            "column": col,
            "value": self.txt_value.GetValue(),
            "pattern": self.txt_pattern.GetValue(),
            "replacement": self.txt_repl.GetValue(),
            "date_format": self.txt_datefmt.GetValue(),
            "columns": checked_cols,
        }


class ConfigFileDialog(wx.Dialog):
    def __init__(self, parent):
        super().__init__(
            parent,
            title="Generate AWS Pipeline Config Files",
            style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER
        )
        self.SetMinSize((660, 520))

        outer = wx.BoxSizer(wx.VERTICAL)

        # Scrollable content so it never gets cut off on smaller screens
        scrolled = wx.ScrolledWindow(self, style=wx.VSCROLL)
        scrolled.SetScrollRate(10, 10)

        panel = scrolled  # use scrolled as the parent for controls
        v = wx.BoxSizer(wx.VERTICAL)


        hint = wx.StaticText(panel, label=(
            "This will generate AWS pipeline configuration and field mappings from your current app/grid inputs.\n"
            "Tip: Run Catalog first to include Data Type, Nullable, Description, SLA in the mapping."
        ))
        v.Add(hint, 0, wx.ALL, 10)

        grid = wx.FlexGridSizer(0, 2, 8, 10)
        grid.AddGrowableCol(1, 1)

        def add_row(label, ctrl):
            grid.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid.Add(ctrl, 1, wx.EXPAND)

        self.txt_pipeline = wx.TextCtrl(panel, value="data-wizard-pipeline")
        self.txt_region = wx.TextCtrl(panel, value=os.environ.get("AWS_REGION", "us-east-1"))
        self.cbo_source_type = wx.Choice(panel, choices=["s3", "http", "local"])
        self.cbo_source_type.SetSelection(0)
        self.txt_source_uri = wx.TextCtrl(panel, value="s3://your-bucket/path/input/")
        self.cbo_source_format = wx.Choice(panel, choices=["csv", "tsv", "json", "parquet"])
        self.cbo_source_format.SetSelection(0)
        self.chk_header = wx.CheckBox(panel, label="Source has header row")
        self.chk_header.SetValue(True)
        self.txt_delim = wx.TextCtrl(panel, value=",")

        self.txt_target_s3 = wx.TextCtrl(panel, value="s3://your-bucket/path/output/")
        self.cbo_target_format = wx.Choice(panel, choices=["parquet", "csv", "json"])
        self.cbo_target_format.SetSelection(0)
        self.cbo_target_compress = wx.Choice(panel, choices=["snappy", "gzip", "none"])
        self.cbo_target_compress.SetSelection(0)
        self.txt_partitions = wx.TextCtrl(panel, value="")  # comma-separated

        self.txt_glue_db = wx.TextCtrl(panel, value="")     # optional
        self.txt_glue_table = wx.TextCtrl(panel, value="")  # optional
        self.txt_glue_crawler = wx.TextCtrl(panel, value="")
        self.txt_glue_job = wx.TextCtrl(panel, value="")
        self.txt_glue_workflow = wx.TextCtrl(panel, value="")
        self.txt_role_arn = wx.TextCtrl(panel, value="")

        self.chk_enable_crawler = wx.CheckBox(panel, label="Enable crawler")
        self.chk_enable_crawler.SetValue(True)
        self.chk_enable_job = wx.CheckBox(panel, label="Enable job")
        self.chk_enable_job.SetValue(True)

        self.cbo_orch = wx.Choice(panel, choices=["glue_workflow", "step_functions", "none"])
        self.cbo_orch.SetSelection(0)
        self.txt_cron = wx.TextCtrl(panel, value="")  # optional

        self.txt_notes = wx.TextCtrl(panel, style=wx.TE_MULTILINE, size=(-1, 80))
        self.txt_notes.SetValue("")

        add_row("Pipeline Name", self.txt_pipeline)
        add_row("AWS Region", self.txt_region)
        add_row("Source Type", self.cbo_source_type)
        add_row("Source URI", self.txt_source_uri)
        add_row("Source Format", self.cbo_source_format)

        # header + delimiter line (two controls on right)
        hdr_line = wx.BoxSizer(wx.HORIZONTAL)
        hdr_line.Add(self.chk_header, 0, wx.RIGHT | wx.ALIGN_CENTER_VERTICAL, 10)
        hdr_line.Add(wx.StaticText(panel, label="Delimiter:"), 0, wx.RIGHT | wx.ALIGN_CENTER_VERTICAL, 6)
        hdr_line.Add(self.txt_delim, 0)
        grid.Add(wx.StaticText(panel, label="CSV/TSV Options"), 0, wx.ALIGN_CENTER_VERTICAL)
        grid.Add(hdr_line, 1, wx.EXPAND)

        add_row("Target S3 URI", self.txt_target_s3)
        add_row("Target Format", self.cbo_target_format)
        add_row("Compression", self.cbo_target_compress)
        add_row("Partition Keys (comma-separated)", self.txt_partitions)

        add_row("Glue Database (optional)", self.txt_glue_db)
        add_row("Glue Table (optional)", self.txt_glue_table)
        add_row("Glue Crawler Name", self.txt_glue_crawler)
        add_row("Glue Job Name", self.txt_glue_job)
        add_row("Glue Workflow Name", self.txt_glue_workflow)
        add_row("IAM Role ARN", self.txt_role_arn)

        flags = wx.BoxSizer(wx.HORIZONTAL)
        flags.Add(self.chk_enable_crawler, 0, wx.RIGHT, 16)
        flags.Add(self.chk_enable_job, 0)
        grid.Add(wx.StaticText(panel, label="Glue Toggles"), 0, wx.ALIGN_CENTER_VERTICAL)
        grid.Add(flags, 1, wx.EXPAND)

        add_row("Orchestration", self.cbo_orch)
        add_row("Schedule Cron (optional)", self.txt_cron)

        v.Add(grid, 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 10)

        v.Add(wx.StaticText(panel, label="Notes (optional)"), 0, wx.LEFT | wx.RIGHT | wx.TOP, 10)
        v.Add(self.txt_notes, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.BOTTOM, 10)

        v.Add(wx.StaticLine(panel), 0, wx.EXPAND | wx.ALL, 8)

        okc = wx.StdDialogButtonSizer()
        ok = wx.Button(self, wx.ID_OK)      # parent = self (non-scrolling)
        ca = wx.Button(self, wx.ID_CANCEL)  # parent = self (non-scrolling)
        okc.AddButton(ok); okc.AddButton(ca); okc.Realize()

        outer.Add(okc, 0, wx.ALIGN_RIGHT | wx.ALL, 10)


        panel.SetSizer(v)
        panel.FitInside()

        outer.Add(scrolled, 1, wx.EXPAND)
        self.SetSizer(outer)
        self.Layout()


    def get_params(self):
        parts = [p.strip() for p in (self.txt_partitions.GetValue() or "").split(",") if p.strip()]
        return {
            "pipeline_name": self.txt_pipeline.GetValue().strip(),
            "aws_region": self.txt_region.GetValue().strip() or "us-east-1",
            "source_type": self.cbo_source_type.GetStringSelection(),
            "source_uri": self.txt_source_uri.GetValue().strip(),
            "source_format": self.cbo_source_format.GetStringSelection(),
            "source_has_header": self.chk_header.GetValue(),
            "source_delimiter": self.txt_delim.GetValue() or ",",
            "target_s3_uri": self.txt_target_s3.GetValue().strip(),
            "target_format": self.cbo_target_format.GetStringSelection(),
            "target_compression": self.cbo_target_compress.GetStringSelection(),
            "partition_keys": parts,
            "glue_database": self.txt_glue_db.GetValue().strip(),
            "glue_table": self.txt_glue_table.GetValue().strip(),
            "glue_crawler_name": self.txt_glue_crawler.GetValue().strip(),
            "glue_job_name": self.txt_glue_job.GetValue().strip(),
            "glue_workflow_name": self.txt_glue_workflow.GetValue().strip(),
            "iam_role_arn": self.txt_role_arn.GetValue().strip(),
            "enable_crawler": self.chk_enable_crawler.GetValue(),
            "enable_job": self.chk_enable_job.GetValue(),
            "orchestration_type": self.cbo_orch.GetStringSelection(),
            "schedule_cron": self.txt_cron.GetValue().strip(),
            "notes": self.txt_notes.GetValue().strip(),
        }


# ──────────────────────────────────────────────────────────────────────────────
# Snowflake bundle dialog (NEW)
# ──────────────────────────────────────────────────────────────────────────────

class SnowflakeBundleDialog(wx.Dialog):
    def __init__(self, parent):
        super().__init__(
            parent,
            title="Generate Snowflake Bundle",
            style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER
        )
        self.SetMinSize((660, 560))

        outer = wx.BoxSizer(wx.VERTICAL)

        scrolled = wx.ScrolledWindow(self, style=wx.VSCROLL)
        scrolled.SetScrollRate(10, 10)
        panel = scrolled

        v = wx.BoxSizer(wx.VERTICAL)

        hint = wx.StaticText(panel, label=(
            "Generates a Snowflake deployment bundle (SQL scripts + mappings) from your current grid.\n"
            "Tip: Run Catalog first so Data Type / Nullable / Description / SLA are included."
        ))
        v.Add(hint, 0, wx.ALL, 10)

        grid = wx.FlexGridSizer(0, 2, 8, 10)
        grid.AddGrowableCol(1, 1)

        def add_row(label, ctrl):
            grid.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid.Add(ctrl, 1, wx.EXPAND)

        # Naming
        self.txt_bundle_name = wx.TextCtrl(panel, value="data_wizard_snowflake_bundle")

        # Core Snowflake objects
        self.txt_database = wx.TextCtrl(panel, value="DATA_WIZARD")
        self.txt_schema = wx.TextCtrl(panel, value="PUBLIC")
        self.txt_warehouse = wx.TextCtrl(panel, value="DATA_WIZARD_WH")
        self.txt_role = wx.TextCtrl(panel, value="DATA_WIZARD_ROLE")

        self.txt_target_table = wx.TextCtrl(panel, value="INGEST_TABLE")
        self.txt_stage = wx.TextCtrl(panel, value="DATA_WIZARD_STAGE")
        self.txt_file_format = wx.TextCtrl(panel, value="DATA_WIZARD_FF")

        # Source / stage config
        self.cbo_source_type = wx.Choice(panel, choices=["s3", "azure", "gcs", "existing_stage"])
        self.cbo_source_type.SetSelection(0)
        self.txt_source_url = wx.TextCtrl(panel, value="s3://your-bucket/path/")
        self.txt_storage_integration = wx.TextCtrl(panel, value="")  # recommended
        self.txt_pattern = wx.TextCtrl(panel, value="")  # optional

        # File format options
        self.cbo_format_type = wx.Choice(panel, choices=["CSV", "JSON", "PARQUET"])
        self.cbo_format_type.SetSelection(0)
        self.txt_field_delim = wx.TextCtrl(panel, value=",")
        self.chk_skip_header = wx.CheckBox(panel, label="Skip header row")
        self.chk_skip_header.SetValue(True)
        self.txt_enclosed_by = wx.TextCtrl(panel, value="\"")
        self.txt_null_if = wx.TextCtrl(panel, value="")

        # COPY options
        self.cbo_on_error = wx.Choice(panel, choices=["CONTINUE", "SKIP_FILE", "SKIP_FILE_1%", "ABORT_STATEMENT"])
        self.cbo_on_error.SetSelection(0)

        # Optional scheduling string for examples
        self.txt_schedule = wx.TextCtrl(panel, value="0 * * * * UTC")

        add_row("Bundle Name", self.txt_bundle_name)
        v.Add(grid, 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 10)

        v.Add(wx.StaticLine(panel), 0, wx.EXPAND | wx.ALL, 8)

        grid2 = wx.FlexGridSizer(0, 2, 8, 10)
        grid2.AddGrowableCol(1, 1)

        def add2(label, ctrl):
            grid2.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid2.Add(ctrl, 1, wx.EXPAND)

        add2("Database", self.txt_database)
        add2("Schema", self.txt_schema)
        add2("Warehouse", self.txt_warehouse)
        add2("Role", self.txt_role)
        add2("Target Table", self.txt_target_table)
        add2("Stage Name", self.txt_stage)
        add2("File Format Name", self.txt_file_format)
        v.Add(grid2, 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 10)

        v.Add(wx.StaticLine(panel), 0, wx.EXPAND | wx.ALL, 8)

        grid3 = wx.FlexGridSizer(0, 2, 8, 10)
        grid3.AddGrowableCol(1, 1)

        def add3(label, ctrl):
            grid3.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid3.Add(ctrl, 1, wx.EXPAND)

        add3("Source Type", self.cbo_source_type)
        add3("Source URL (for stage)", self.txt_source_url)
        add3("STORAGE_INTEGRATION (recommended)", self.txt_storage_integration)
        add3("PATTERN (optional)", self.txt_pattern)
        v.Add(grid3, 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 10)

        v.Add(wx.StaticLine(panel), 0, wx.EXPAND | wx.ALL, 8)

        grid4 = wx.FlexGridSizer(0, 2, 8, 10)
        grid4.AddGrowableCol(1, 1)

        def add4(label, ctrl):
            grid4.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid4.Add(ctrl, 1, wx.EXPAND)

        add4("File Format Type", self.cbo_format_type)
        add4("Field Delimiter (CSV)", self.txt_field_delim)
        grid4.Add(wx.StaticText(panel, label="CSV Header"), 0, wx.ALIGN_CENTER_VERTICAL)
        grid4.Add(self.chk_skip_header, 0, wx.EXPAND)
        add4("Enclosed By (CSV)", self.txt_enclosed_by)
        add4("NULL_IF (optional)", self.txt_null_if)
        v.Add(grid4, 0, wx.EXPAND | wx.LEFT | wx.RIGHT, 10)

        v.Add(wx.StaticLine(panel), 0, wx.EXPAND | wx.ALL, 8)

        grid5 = wx.FlexGridSizer(0, 2, 8, 10)
        grid5.AddGrowableCol(1, 1)

        def add5(label, ctrl):
            grid5.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid5.Add(ctrl, 1, wx.EXPAND)

        add5("COPY ON_ERROR", self.cbo_on_error)
        add5("Schedule (example only)", self.txt_schedule)
        v.Add(grid5, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.BOTTOM, 10)

        panel.SetSizer(v)
        panel.FitInside()

        outer.Add(scrolled, 1, wx.EXPAND)

        btns = wx.StdDialogButtonSizer()
        ok = wx.Button(self, wx.ID_OK)
        ca = wx.Button(self, wx.ID_CANCEL)
        btns.AddButton(ok); btns.AddButton(ca); btns.Realize()
        outer.Add(btns, 0, wx.ALIGN_RIGHT | wx.ALL, 10)

        self.SetSizer(outer)
        self.Layout()

    def get_params(self):
        return {
            "bundle_name": self.txt_bundle_name.GetValue().strip(),
            "database": self.txt_database.GetValue().strip(),
            "schema": self.txt_schema.GetValue().strip(),
            "warehouse": self.txt_warehouse.GetValue().strip(),
            "role": self.txt_role.GetValue().strip(),
            "target_table": self.txt_target_table.GetValue().strip(),
            "stage": self.txt_stage.GetValue().strip(),
            "file_format": self.txt_file_format.GetValue().strip(),
            "source_type": self.cbo_source_type.GetStringSelection(),
            "source_url": self.txt_source_url.GetValue().strip(),
            "storage_integration": self.txt_storage_integration.GetValue().strip(),
            "pattern": self.txt_pattern.GetValue().strip(),
            "format_type": self.cbo_format_type.GetStringSelection(),
            "field_delimiter": self.txt_field_delim.GetValue(),
            "skip_header": self.chk_skip_header.GetValue(),
            "enclosed_by": self.txt_enclosed_by.GetValue(),
            "null_if": self.txt_null_if.GetValue(),
            "on_error": self.cbo_on_error.GetStringSelection(),
            "schedule_cron": self.txt_schedule.GetValue().strip(),
        }

# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Fabric bundle dialog (NEW)
# ──────────────────────────────────────────────────────────────────────────────

class FabricBundleDialog(wx.Dialog):
    def __init__(self, parent):
        super().__init__(parent, title="Generate Fabric Bundle", style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER)
        self.SetMinSize((660, 420))

        outer = wx.BoxSizer(wx.VERTICAL)
        panel = wx.Panel(self)
        v = wx.BoxSizer(wx.VERTICAL)

        hint = wx.StaticText(panel, label=(
            "Generates a Microsoft Fabric starter bundle (Lakehouse/pipeline placeholders + mappings).\n"
            "Tip: Run Catalog first so Data Type / Nullable / Description / SLA are included."
        ))
        v.Add(hint, 0, wx.ALL, 10)

        grid = wx.FlexGridSizer(0, 2, 8, 10)
        grid.AddGrowableCol(1, 1)

        def add_row(label, ctrl):
            grid.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid.Add(ctrl, 1, wx.EXPAND)

        self.txt_bundle = wx.TextCtrl(panel, value="data_wizard_fabric_bundle")
        self.txt_lakehouse = wx.TextCtrl(panel, value="data_wizard_lakehouse")
        self.txt_table = wx.TextCtrl(panel, value="stg_ingest")

        add_row("Bundle Name", self.txt_bundle)
        add_row("Lakehouse Name (reference)", self.txt_lakehouse)
        add_row("Table Name (reference)", self.txt_table)

        v.Add(grid, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.BOTTOM, 10)

        btns = wx.StdDialogButtonSizer()
        ok = wx.Button(panel, wx.ID_OK)
        ca = wx.Button(panel, wx.ID_CANCEL)
        btns.AddButton(ok); btns.AddButton(ca); btns.Realize()
        v.Add(btns, 0, wx.ALIGN_RIGHT | wx.ALL, 10)

        panel.SetSizer(v)
        outer.Add(panel, 1, wx.EXPAND)
        self.SetSizer(outer)
        self.Layout()

    def get_params(self):
        return {
            "bundle_name": self.txt_bundle.GetValue().strip(),
            "lakehouse_name": self.txt_lakehouse.GetValue().strip(),
            "table_name": self.txt_table.GetValue().strip(),
        }


# ──────────────────────────────────────────────────────────────────────────────
# Purview export dialog (NEW)
# ──────────────────────────────────────────────────────────────────────────────

class PurviewExportDialog(wx.Dialog):
    def __init__(self, parent):
        super().__init__(parent, title="Generate Purview Export", style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER)
        self.SetMinSize((660, 360))

        outer = wx.BoxSizer(wx.VERTICAL)
        panel = wx.Panel(self)
        v = wx.BoxSizer(wx.VERTICAL)

        hint = wx.StaticText(panel, label=(
            "Exports catalog metadata + quality rules + mappings into CSV/JSON files.\n"
            "This is a best-effort package to help automate Microsoft Purview loading."
        ))
        v.Add(hint, 0, wx.ALL, 10)

        grid = wx.FlexGridSizer(0, 2, 8, 10)
        grid.AddGrowableCol(1, 1)

        def add_row(label, ctrl):
            grid.Add(wx.StaticText(panel, label=label), 0, wx.ALIGN_CENTER_VERTICAL)
            grid.Add(ctrl, 1, wx.EXPAND)

        self.txt_export = wx.TextCtrl(panel, value="data_wizard_purview_export")
        add_row("Export Name", self.txt_export)

        v.Add(grid, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.BOTTOM, 10)

        btns = wx.StdDialogButtonSizer()
        ok = wx.Button(panel, wx.ID_OK)
        ca = wx.Button(panel, wx.ID_CANCEL)
        btns.AddButton(ok); btns.AddButton(ca); btns.Realize()
        v.Add(btns, 0, wx.ALIGN_RIGHT | wx.ALL, 10)

        panel.SetSizer(v)
        outer.Add(panel, 1, wx.EXPAND)
        self.SetSizer(outer)
        self.Layout()

    def get_params(self):
        return {
            "export_name": self.txt_export.GetValue().strip(),
        }


# MDM dialog used above (kept here to keep file self-contained)
# ──────────────────────────────────────────────────────────────────────────────

class MDMDialog(wx.Dialog):
    def __init__(self, parent):
        super().__init__(parent, title="Master Data Management (MDM)", size=(560, 420))
        panel = wx.Panel(self)
        v = wx.BoxSizer(wx.VERTICAL)

        self.chk_include_current = wx.CheckBox(panel, label="Include current dataset as a source")
        self.chk_include_current.SetValue(True)
        v.Add(self.chk_include_current, 0, wx.ALL, 8)

        v.Add(wx.StaticText(panel, label="Sources to merge (local files or URIs):"), 0, wx.LEFT | wx.TOP, 8)
        self.lst = wx.ListBox(panel, style=wx.LB_EXTENDED)
        v.Add(self.lst, 1, wx.EXPAND | wx.ALL, 8)

        btns = wx.BoxSizer(wx.HORIZONTAL)
        btn_add = wx.Button(panel, label="Add Local…")
        btn_uri = wx.Button(panel, label="Add URI/S3…")
        btn_rm  = wx.Button(panel, label="Remove Selected")
        btns.Add(btn_add, 0, wx.RIGHT, 6)
        btns.Add(btn_uri, 0, wx.RIGHT, 6)
        btns.Add(btn_rm, 0)
        v.Add(btns, 0, wx.LEFT | wx.RIGHT | wx.BOTTOM, 8)

        grid = wx.FlexGridSizer(2,2,6,6); grid.AddGrowableCol(1,1)
        grid.Add(wx.StaticText(panel, label="Match threshold (percent):"), 0, wx.ALIGN_CENTER_VERTICAL)
        self.spn_thresh = wx.SpinCtrl(panel, min=50, max=100, initial=85)
        grid.Add(self.spn_thresh, 0, wx.EXPAND)

        grid.Add(wx.StaticText(panel, label="Fields to match on:"), 0, wx.ALIGN_CENTER_VERTICAL)
        h = wx.BoxSizer(wx.HORIZONTAL)
        self.chk_email = wx.CheckBox(panel, label="Email")
        self.chk_phone = wx.CheckBox(panel, label="Phone")
        self.chk_name  = wx.CheckBox(panel, label="Name")
        self.chk_addr  = wx.CheckBox(panel, label="Address")
        for c in (self.chk_email, self.chk_phone, self.chk_name, self.chk_addr):
            c.SetValue(True); h.Add(c, 0, wx.RIGHT, 8)
        grid.Add(h, 0, wx.EXPAND)
        v.Add(grid, 0, wx.EXPAND | wx.LEFT | wx.RIGHT | wx.BOTTOM, 8)

        v.Add(wx.StaticLine(panel), 0, wx.EXPAND | wx.ALL, 6)
        okc = wx.StdDialogButtonSizer(); ok = wx.Button(panel, wx.ID_OK); ca = wx.Button(panel, wx.ID_CANCEL)
        okc.AddButton(ok); okc.AddButton(ca); okc.Realize()
        v.Add(okc, 0, wx.ALIGN_RIGHT | wx.ALL, 8)

        panel.SetSizer(v)
        self.sources = []
        btn_add.Bind(wx.EVT_BUTTON, self._on_add_file)
        btn_uri.Bind(wx.EVT_BUTTON, self._on_add_uri)
        btn_rm.Bind(wx.EVT_BUTTON, self._on_rm)

    def _on_add_file(self, _):
        dlg = wx.FileDialog(self, "Select data file", wildcard="Data|*.csv;*.tsv;*.txt|All|*.*",
                            style=wx.FD_OPEN | wx.FD_FILE_MUST_EXIST | wx.FD_MULTIPLE)
        if dlg.ShowModal() != wx.ID_OK:
            return
        for p in dlg.GetPaths():
            self.sources.append({"type": "file", "value": p})
            self.lst.Append(f"[FILE] {p}")
        dlg.Destroy()

    def _on_add_uri(self, _):
        with wx.TextEntryDialog(self, "Enter HTTP/HTTPS/S3 URI:", "Add URI/S3") as d:
            if d.ShowModal() != wx.ID_OK:
                return
            uri = d.GetValue().strip()
        if uri:
            self.sources.append({"type": "uri", "value": uri})
            self.lst.Append(f"[URI]  {uri}")

    def _on_rm(self, _):
        for i in reversed(self.lst.GetSelections()):
            self.lst.Delete(i)
            del self.sources[i]

    def get_params(self):
        return {
            "include_current": self.chk_include_current.GetValue(),
            "threshold": self.spn_thresh.GetValue() / 100.0,
            "use_email": self.chk_email.GetValue(),
            "use_phone": self.chk_phone.GetValue(),
            "use_name": self.chk_name.GetValue(),
            "use_addr": self.chk_addr.GetValue(),
            "sources": list(self.sources),
        }


if __name__ == "__main__":
    app = wx.App(False)
    MainWindow()
    app.MainLoop()
