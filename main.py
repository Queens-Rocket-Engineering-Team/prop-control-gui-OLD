"""
Barebones PySide6 GUI skeleton for talking to a FastAPI backend
and tailing a Redis pub/sub channel.

Run locally with:
  uv add PySide6 httpx redis pyqtgraph
  uv run python main.py

Adjust API_BASE and REDIS settings below.
"""
from __future__ import annotations

import json
import sys
import threading
import re
from math import isfinite
from dataclasses import dataclass
from typing import Any, Optional
from pathlib import Path
from datetime import datetime
import csv

import httpx
import pyqtgraph as pg
from PySide6.QtCore import QObject, QThread, QThreadPool, QRunnable, Signal, Slot, Qt, QTimer
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QApplication,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QVBoxLayout,
    QScrollArea,
    QSplitter,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QWidget,
    QDialog,
    QFormLayout,
    QDialogButtonBox,
)

pg.setConfigOptions(useOpenGL=False, antialias=True)


# -----------------------------
# Config
# -----------------------------

@dataclass
class Config:
    api_base: str = "http://192.168.1.100:8000"
    commands_path: str = "/v1/command"
    api_username: str = "noah"
    api_password: str = "stinkylion"
    redis_host: str = "192.168.1.100"
    redis_port: int = 6379
    redis_channel: str = "log"
    redis_username: str = "roclient"
    redis_password: str = "password"


CONFIG = Config()

# Strip ANSI color codes from Redis messages
ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")




class HttpRequestSignals(QObject):
    success = Signal(object)
    error = Signal(str)
    finished = Signal()  # always emitted at the end of work (success or error)


class HttpRequestWorker(QRunnable):
    def __init__(self, method: str,
                 url: str,
                 *,
                 json_body: Optional[dict] = None,
                 timeout: float = 5.0,
                 auth: Optional[tuple[str, str]] = None):
        super().__init__()
        self.method = method.upper()
        self.url = url
        self.json_body = json_body
        self.timeout = timeout
        self.auth = auth
        self.signals = HttpRequestSignals()

    @Slot()
    def run(self) -> None:
        try:
            with httpx.Client(timeout=self.timeout, auth=self.auth) as client:
                if self.method == "GET":
                    resp = client.get(self.url)
                elif self.method == "POST":
                    resp = client.post(self.url, json=self.json_body)
                else:
                    resp = client.request(self.method, self.url, json=self.json_body)

            resp.raise_for_status()
            try:
                payload: Any = resp.json()
            except Exception:
                payload = resp.text
            self.signals.success.emit(payload)
        except Exception as e:
            self.signals.error.emit(str(e))
        finally:
            # ensure cleanup hooks in the GUI always fire
            self.signals.finished.emit()


class RedisTailer(QThread):
    message = Signal(str)
    status = Signal(str)
    error = Signal(str)

    def __init__(self, host: str, port: int, channel: str, username: str, password: str, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.host = host
        self.port = port
        self.channel = channel
        self.username = username
        self.password = password
        self._stop_flag = threading.Event()
        self._pubsub = None

    def run(self) -> None:
        try:
            import redis
            client = redis.Redis(
                host=self.host, port=self.port,
                username=self.username, password=self.password,
                decode_responses=True,
            )
            self._pubsub = client.pubsub(ignore_subscribe_messages=True)
            self._pubsub.subscribe(self.channel)
            self.status.emit(f"Subscribed to redis://{self.host}:{self.port} channel '{self.channel}'")

            count = 0
            while not self._stop_flag.is_set():
                item = self._pubsub.get_message(timeout=1.0)  # seconds
                if not item:
                    continue
                if item.get("type") == "message":
                    data = item.get("data")
                    if not isinstance(data, str):
                        try:
                            data = json.dumps(data)
                        except Exception:
                            data = str(data)
                    # optional: strip ANSI here if you've filled in the regex
                    data = ANSI_RE.sub("", data)
                    # emit per-line to be safe
                    for line in str(data).splitlines():
                        line = line.strip()
                        if line:
                            self.message.emit(line)

                count += 1
                if count % 10 == 0:
                    QApplication.processEvents()  # keep GUI responsive

        except Exception as e:
            self.error.emit(str(e))
        finally:
            try:
                if self._pubsub is not None:
                    self._pubsub.close()
            except Exception:
                pass
            self.status.emit("Redis tailer stopped.")


    def stop(self) -> None:
        self._stop_flag.set()

# -----------------------------
# Controls
# -----------------------------
class ControlsPanel(QGroupBox):
    def __init__(self, parent=None):
        super().__init__("Commands", parent)
        layout = QVBoxLayout()
        self.btn_gets = QPushButton("GETS")
        self.btn_stop = QPushButton("STOP")
        self.stream_rate_edit = QLineEdit("100")
        self.stream_rate_edit.setPlaceholderText("Hz")
        self.btn_stream = QPushButton("STREAM")
        self.btn_ping = QPushButton("Ping Backend")

        layout.addWidget(self.btn_gets)
        layout.addWidget(self.btn_stop)

        # Stream controls group
        stream_group = QGroupBox("Stream Controls")
        stream_layout = QVBoxLayout()
        freq_layout = QHBoxLayout()
        freq_layout.addWidget(self.stream_rate_edit)
        freq_layout.addWidget(QLabel("Hz"))
        stream_layout.addWidget(self.btn_stream)
        stream_layout.addLayout(freq_layout)
        stream_group.setLayout(stream_layout)

        layout.addWidget(stream_group)
        layout.addWidget(self.btn_ping)
        self.setLayout(layout)

# -----------------------------
# Controls sidebar with per-control Open/Close
# -----------------------------
class ControlsSidebar(QGroupBox):
    controlRequested = Signal(str, str)  # (name, action)

    def __init__(self, parent: Optional[QWidget] = None, controls: Optional[list[str]] = None, general_widget: Optional[QWidget] = None) -> None:
        super().__init__("Controls", parent)
        if controls is None:
            av_controls = [
                "AVFILL", "AVRUN", "AVDUMP", "AVPURGE1", "AVPURGE2", "AVVENT"
            ]
            power_controls = [
                "SAFE24", "IGN"
            ]
        else:
            # If custom controls are provided, split them by name
            av_controls = [c for c in controls if c.startswith("AV")]
            power_controls = [c for c in controls if c in {"SAFE24", "IGN"}]

        self.controlButtons = {}

        v = QVBoxLayout(self)

        # Valves subbox
        valves_box = QGroupBox("VALVES", self)
        valves_layout = QVBoxLayout(valves_box)
        for name in av_controls:
            box = QGroupBox(name, valves_box)
            h = QHBoxLayout(box)
            btn_open = QPushButton("Open", box)
            btn_close = QPushButton("Close", box)
            self.controlButtons[f"{name}_open"] = btn_open
            self.controlButtons[f"{name}_close"] = btn_close
            btn_open.clicked.connect(lambda _=False, n=name: self.controlRequested.emit(n, "OPEN"))
            btn_close.clicked.connect(lambda _=False, n=name: self.controlRequested.emit(n, "CLOSE"))
            h.addWidget(btn_open)
            h.addWidget(btn_close)
            box.setLayout(h)
            valves_layout.addWidget(box)

        # Add "Close All" and "Default Positions" buttons
        btn_close_all = QPushButton("Close All", valves_box)
        btn_default_positions = QPushButton("Default Positions", valves_box)
        valves_layout.addWidget(btn_close_all)
        valves_layout.addWidget(btn_default_positions)

        # Connect buttons to signals
        btn_close_all.clicked.connect(lambda: [self.controlRequested.emit(name, "CLOSE") for name in av_controls])
        btn_default_positions.clicked.connect(self.handleDefaultButton)

        valves_layout.addStretch(1)
        valves_box.setLayout(valves_layout)
        v.addWidget(valves_box)

        # Power subbox
        power_box = QGroupBox("Power", self)
        power_layout = QVBoxLayout(power_box)
        for name in power_controls:
            box = QGroupBox(name, power_box)
            h = QHBoxLayout(box)
            btn_open = QPushButton("Open", box)
            btn_close = QPushButton("Close", box)
            self.controlButtons[f"{name}_open"] = btn_open
            self.controlButtons[f"{name}_close"] = btn_close
            btn_open.clicked.connect(lambda _=False, n=name: self.controlRequested.emit(n, "OPEN"))
            btn_close.clicked.connect(lambda _=False, n=name: self.controlRequested.emit(n, "CLOSE"))
            h.addWidget(btn_open)
            h.addWidget(btn_close)
            box.setLayout(h)
            power_layout.addWidget(box)
        power_layout.addStretch(1)
        power_box.setLayout(power_layout)
        v.addWidget(power_box)

        # Spacer so the General group stays at the bottom
        v.addStretch(1)

    # General controls subbox (at the bottom)
        general_box = QGroupBox("General", self)
        general_layout = QVBoxLayout(general_box)
        if general_widget is not None:
            general_layout.addWidget(general_widget)
        general_box.setLayout(general_layout)
        v.addWidget(general_box)

    def handleDefaultButton(self):
        # Open a dialog to confirm whether the user wants to reset to default. Warn that this will dump any oxidizer to
        # the air
        confirm = QMessageBox.question(
            self,
            "Confirm Reset",
            "This will reset all controls to their default positions and may dump oxidizer to the air. Do you want to continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        if confirm == QMessageBox.StandardButton.Yes:
            self.controlRequested.emit("ALL", "DEFAULT")
        else:
            return


# -----------------------------
# Credentials dialog (username + password)
# -----------------------------
class CredentialsDialog(QDialog):
    def __init__(self, username: str = "", password: str = "", parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("API Credentials")
        form = QFormLayout(self)
        self.user_edit = QLineEdit(username)
        self.pass_edit = QLineEdit(password)
        self.pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("Username", self.user_edit)
        form.addRow("Password", self.pass_edit)
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        form.addRow(buttons)

    def values(self) -> tuple[str, str]:
        return self.user_edit.text().strip(), self.pass_edit.text().strip()

# -----------------------------
# Dynamic graph panel using pyqtgraph
# -----------------------------
class GraphPanel(QWidget):
    def __init__(self, columns: int = 1, max_points: int = 2000, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.columns = 1  # Force single column
        self.max_points = max_points
        self.window_seconds = 30  # Sliding window of X seconds
        self.grid = QGridLayout(self)
        self.grid.setContentsMargins(0, 0, 0, 0)
        self.grid.setHorizontalSpacing(8)
        self.grid.setVerticalSpacing(16)
        self._order: list[str] = []
        self._curves: dict[str, pg.PlotDataItem] = {}
        self._plots: dict[str, pg.PlotWidget] = {}
        self._data: dict[str, tuple[list[float], list[float]]] = {}
        self._last_t: dict[str, float] = {}  # track last timestamp per series to avoid duplicates
        self._readouts: dict[str, QLabel] = {}  # name -> QLabel
        self._titles: dict[str, QLabel] = {}    # name -> QLabel
        self._units: dict[str, str] = {}  # key: full series or base name -> unit
        # Display-only tare offsets per series; raw data in _data remains unchanged
        self._tare_offsets: dict[str, float] = {}

    def ensure_plot(self, name: str) -> pg.PlotDataItem:
        if name in self._curves:
            return self._curves[name]
        row = len(self._order)

        # Vertical box for title, readout, and tare button
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(2)

        # Title label (include unit if known)
        base_name = name.split(":", 1)[-1]
        unit = self._units.get(name) or self._units.get(base_name)
        title_text = f"{name} ({unit})" if unit else name
        title = QLabel(title_text)
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        tfont = title.font()
        tfont.setPointSize(11)
        tfont.setBold(True)
        title.setFont(tfont)
        left_layout.addWidget(title)

        # Live readout label
        readout = QLabel("--")
        readout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        rfont = readout.font()
        rfont.setPointSize(14)
        rfont.setBold(True)
        readout.setFont(rfont)
        left_layout.addWidget(readout)

        # Per-series Tare button (display-only offset)
        tare_btn = QPushButton("Tare")
        tare_btn.setToolTip("Zero this series by subtracting the current value from the display")
        tare_btn.clicked.connect(lambda _=False, n=name: self.tare_series(n))
        left_layout.addWidget(tare_btn)
        left_layout.addStretch(1)

        self._readouts[name] = readout
        self._titles[name] = title

        # Plot widget to the right of the readout
        plot = pg.PlotWidget()
        plot.showGrid(x=True, y=True)
        curve = plot.plot()

        # Place left_widget and plot in the same row
        self.grid.addWidget(left_widget, row, 0, alignment=Qt.AlignmentFlag.AlignTop)
        self.grid.addWidget(plot, row, 1)
        self._order.append(name)
        self._curves[name] = curve
        self._plots[name] = plot
        self._data[name] = ([], [])
        return curve

    def set_unit(self, name: str, unit: str) -> None:
        """Register a unit for a series or base signal name.
        name can be either 'DEVICE:signal' or just 'signal'."""
        if not name:
            return
        unit = (unit or "").strip()
        if not unit:
            return
        self._units[name] = unit
        # Also map base for convenience
        if ":" in name:
            base = name.split(":", 1)[-1]
            self._units.setdefault(base, unit)
        # Refresh existing titles
        for series_name, title_lbl in self._titles.items():
            base = series_name.split(":", 1)[-1]
            u = self._units.get(series_name) or self._units.get(base)
            title_lbl.setText(f"{series_name} ({u})" if u else series_name)

    def add_point(self, name: str, t: float, y: float) -> None:
        if not name:
            return
        # Skip if time not increasing for this series
        last = self._last_t.get(name)
        if last is not None and t <= last:
            return
        self._last_t[name] = t

        curve = self.ensure_plot(name)
        xs, ys = self._data[name]
        xs.append(t)
        ys.append(y)

        # Only plot the last window_seconds, but keep all data in xs/ys
        min_t = t - self.window_seconds
        # Find the first index where xs[idx] >= min_t
        start_idx = 0
        for i, tx in enumerate(xs):
            if tx >= min_t:
                start_idx = i
                break
        else:
            start_idx = len(xs)  # all points are older

        if start_idx > 0:
            del xs[:start_idx]
            del ys[:start_idx]

        plot_xs = xs[:]
        plot_ys = ys[:]

        # Apply display-only tare offset if present
        offset = self._tare_offsets.get(name, 0.0)
        if offset:
            plot_ys = [v - offset for v in plot_ys]

        # Pad the window if not enough data to fill window_seconds
        if plot_xs:
            window_start = plot_xs[0]
            window_end = plot_xs[-1]
            # If less than window_seconds, pad at the left
            if window_end - window_start < self.window_seconds:
                pad_start = window_end - self.window_seconds
                if pad_start < 0:
                    pad_start = 0
                # If first point is after pad_start, pad with NaN
                if plot_xs[0] > pad_start:
                    plot_xs = [pad_start] + plot_xs
                    plot_ys = [float('nan')] + plot_ys
            # Always set x range to window_end - window_seconds to window_end
            plot_widget = self._plots.get(name)
            if plot_widget is not None:
                plot_item = getattr(plot_widget, "getPlotItem", lambda: None)()
                view_box = getattr(plot_item, "getViewBox", lambda: None)()
                if view_box is not None:
                    view_box.setXRange(window_end - self.window_seconds, window_end)
        else:
            # No data yet, set x range to t - window_seconds to t
            plot_widget = self._plots.get(name)
            if plot_widget is not None:
                plot_item = getattr(plot_widget, "getPlotItem", lambda: None)()
                view_box = getattr(plot_item, "getViewBox", lambda: None)()
                if view_box is not None:
                    view_box.setXRange(t - self.window_seconds, t)

        curve.setData(plot_xs, plot_ys)

        # Update live readout (append unit if available)
        if name in self._readouts:
            unit = self._units.get(name) or self._units.get(name.split(":", 1)[-1])
            disp_y = y - self._tare_offsets.get(name, 0.0)
            self._readouts[name].setText(f"{disp_y:.3f} {unit}" if unit else f"{disp_y:.3f}")

    def tare_series(self, name: str) -> None:
        """Set a display-only zero offset for a series to its current value and refresh the plot."""
        if name not in self._data:
            return
        xs, ys = self._data[name]
        if not ys:
            return  # nothing to tare yet
        current = ys[-1]
        self._tare_offsets[name] = float(current)
        # Refresh existing curve with new offset and update readout
        self._refresh_series(name)

    def _refresh_series(self, name: str) -> None:
        """Re-render a series using the current tare offset and window."""
        if name not in self._data:
            return
        xs, ys = self._data[name]
        if not xs:
            # Clear plot and readout
            curve = self._curves.get(name)
            if curve is not None:
                curve.setData([], [])
            if name in self._readouts:
                self._readouts[name].setText("--")
            return

        # Use last timestamp to compute window
        t = xs[-1]
        min_t = t - self.window_seconds
        start_idx = 0
        for i, tx in enumerate(xs):
            if tx >= min_t:
                start_idx = i
                break
        else:
            start_idx = len(xs)

        plot_xs = xs[start_idx:]
        plot_ys = ys[start_idx:]

        offset = self._tare_offsets.get(name, 0.0)
        if offset:
            plot_ys = [v - offset for v in plot_ys]

        if plot_xs:
            window_start = plot_xs[0]
            window_end = plot_xs[-1]
            if window_end - window_start < self.window_seconds:
                pad_start = window_end - self.window_seconds
                if pad_start < 0:
                    pad_start = 0
                if plot_xs[0] > pad_start:
                    plot_xs = [pad_start] + plot_xs
                    plot_ys = [float('nan')] + plot_ys
            plot_widget = self._plots.get(name)
            if plot_widget is not None:
                plot_item = getattr(plot_widget, "getPlotItem", lambda: None)()
                view_box = getattr(plot_item, "getViewBox", lambda: None)()
                if view_box is not None:
                    view_box.setXRange(window_end - self.window_seconds, window_end)
        curve = self._curves.get(name)
        if curve is not None:
            curve.setData(plot_xs, plot_ys)
        # Update readout with offset applied
        if name in self._readouts and ys:
            unit = self._units.get(name) or self._units.get(name.split(":", 1)[-1])
            disp_y = ys[-1] - self._tare_offsets.get(name, 0.0)
            self._readouts[name].setText(f"{disp_y:.3f} {unit}" if unit else f"{disp_y:.3f}")

class DataLogger(QObject):
    """Buffered wide-CSV logger with per-device row assembly and periodic flushes.

    CSV layout: device,t,<sensor1>,<sensor2>,...
    """

    def __init__(self, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._file = None
        self._writer = None
        self._buffer = []  # list of ready-to-write rows
        self.path = None
        self._columns = []  # sensor names
        self._header_written = False
        # Current in-progress rows per device
        self._current_t = {}
        self._current_row = {}

    @property
    def is_active(self) -> bool:
        return self._file is not None

    def start(self, base_dir: Path, start_dt: datetime, columns: Optional[list[str]] = None) -> None:
        base_dir.mkdir(parents=True, exist_ok=True)
        fname = f"PANDA-TEST-{start_dt.strftime('%d%m%y-%H%M%S')}.csv"
        self.path = base_dir / fname
        self._file = open(self.path, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        # Prepare columns, may be empty; if empty, defer header until first data row
        self._columns = sorted(set(columns or []))
        if self._columns:
            header = ["device", "t", *self._columns]
            self._writer.writerow(header)
            self._file.flush()
            self._header_written = True
        else:
            self._header_written = False
        # Reset per-device accumulators
        self._current_t.clear()
        self._current_row.clear()

    def log(self, device: str, t: float, name: str, value: float) -> None:
        if not self._writer:
            return
    # Always record incoming sensor values; columns are handled at finalize time
        prev_t = self._current_t.get(device)
        if prev_t is None:
            self._current_t[device] = t
            self._current_row[device] = {}
        elif t != prev_t:
            # new timestamp -> finalize previous row first
            self._finalize_device_row(device)
            self._current_t[device] = t
            self._current_row[device] = {}
        # record value
        self._current_row[device][name] = value
        # protect against runaway memory
        if len(self._buffer) >= 200:
            self.flush()

    def flush(self) -> None:
        if not self._writer:
            return
        # finalize in-progress rows (so far) and write out
        for device, rowdict in list(self._current_row.items()):
            if rowdict:
                self._finalize_device_row(device)
                # keep the same timestamp active, but clear row accumulation
                self._current_row[device] = {}
        if self._buffer:
            self._writer.writerows(self._buffer)
            self._buffer.clear()
            if self._file:
                self._file.flush()

    def _finalize_device_row(self, device: str) -> None:
        if device not in self._current_t or device not in self._current_row:
            return
        t = self._current_t[device]
        rowdict = self._current_row[device]
        if not rowdict:
            return
        # Write header if still pending
        if not self._header_written:
            if not self._columns:
                self._columns = sorted(rowdict.keys())
            header = ["device", "t", *self._columns]
            if not self._writer:
                return
            self._writer.writerow(header)
            if self._file:
                self._file.flush()
            self._header_written = True
        # Build row in fixed column order
        row = [device, t]
        for col in self._columns:
            row.append(rowdict.get(col, ""))
        self._buffer.append(row)

    def stop(self) -> None:
        try:
            # finalize remaining
            for device in list(self._current_t.keys()):
                self._finalize_device_row(device)
            self.flush()
        finally:
            if self._file:
                self._file.close()
            self._file = None
            self._writer = None
            self.path = None
            self._columns = []
            self._current_t.clear()
            self._current_row.clear()


class MainWindow(QMainWindow):
    def __init__(self, config: Config):
        super().__init__()
        self.config = config
        self.setWindowTitle("Prop Control")

        self.thread_pool = QThreadPool.globalInstance()
        # Avoid saturating the machine with too many concurrent requests
        self._http_workers: set[HttpRequestWorker] = set()  # keep refs to prevent GC/segfaults
        self.redis_thread: Optional[RedisTailer] = None

        self._pending_points = []
        self._plot_timer = QTimer(self)
        self._plot_timer.setInterval(30)  # ms, adjust as needed
        self._plot_timer.timeout.connect(self.flush_pending_points)
        self._plot_timer.start()

        self.deviceConfig = None


        # ----
        # Menu selections
        # ----

        server_menu = self.menuBar().addMenu("Server")
        self.server_ips = ["192.168.1.100"]
        self.default_server_ip = self.server_ips[0]

        for ip in self.server_ips:
            act = QAction(ip, self)
            act.triggered.connect(lambda checked, ip=ip: self.set_server_ip(ip))
            server_menu.addAction(act)

        server_menu.addSeparator()
        act_creds = QAction("Set API Credentials…", self)
        act_creds.triggered.connect(self.set_api_credentials)
        server_menu.addAction(act_creds)

        # --- Logging setup ---
        self._log_dir = Path(__file__).resolve().parent / "data"
        self._test_start_dt: Optional[datetime] = None
        self.logger = DataLogger(self)

        self._log_timer = QTimer(self)
        self._log_timer.setInterval(1000)  # flush once per second
        self._log_timer.timeout.connect(self.logger.flush)

        # ----
        # View menu: Show Log toggle
        # ----
        view_menu = self.menuBar().addMenu("View")
        self.act_show_log = QAction("Show Log", self, checkable=True)
        self.act_show_log.setChecked(True)
        view_menu.addAction(self.act_show_log)


        # ----
        # Controls (moved into sidebar General group)
        # ----
        self.controls_panel = ControlsPanel()
        self.btn_gets = self.controls_panel.btn_gets
        self.btn_stop = self.controls_panel.btn_stop
        self.stream_rate_edit = self.controls_panel.stream_rate_edit
        self.btn_stream = self.controls_panel.btn_stream
        self.btn_ping = self.controls_panel.btn_ping

        self.log = QTextEdit()
        self.log.setReadOnly(True)

        self.act_show_log.toggled.connect(self.toggle_log)
        # Ensure log is visible by default
        self.log.setVisible(True)

        # --- Right side: graphs + log (controls moved to sidebar)
        self.graphs = GraphPanel(columns=2)
        graphs_scroll = QScrollArea()
        graphs_scroll.setWidget(self.graphs)
        graphs_scroll.setWidgetResizable(True)
        graphs_scroll.setMinimumHeight(300)

        # Right stack: splitter between graphs and log
        right_splitter = QSplitter(Qt.Orientation.Vertical)
        right_splitter.addWidget(graphs_scroll)
        right_splitter.addWidget(self.log)
        right_splitter.setSizes([500, 200])

        right_container = QWidget()
        right_v = QVBoxLayout(right_container)
        right_v.addWidget(right_splitter, 1)

        # Left side: controls sidebar + (optional) config panel
        side = QWidget()
        side_v = QVBoxLayout(side)
        self.controls_sidebar = ControlsSidebar(general_widget=self.controls_panel)
        self.controlButtons = self.controls_sidebar.controlButtons
        self.controls_sidebar.controlRequested.connect(self.send_control_command)
        side_v.addWidget(self.controls_sidebar)
        side_v.addStretch(1)
        side.setMinimumWidth(220)
        side.setMaximumWidth(220)

        # Main splitter: side <-> right
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.addWidget(side)
        main_splitter.addWidget(right_container)
        main_splitter.setSizes([360, 800])

        self.setCentralWidget(main_splitter)

        act_quit = QAction("Quit", self)
        act_quit.triggered.connect(self.close)
        self.menuBar().addAction(act_quit)

        self.btn_ping.clicked.connect(self.on_ping)
        self.btn_gets.clicked.connect(self.on_gets)
        self.btn_stream.clicked.connect(self.on_stream)
        self.btn_stop.clicked.connect(self.on_stop)

        self.set_server_ip(self.default_server_ip)
        self.send_config_request()
        self.send_status_request()
        self.statusBar().showMessage("Ready")

    def _collect_sensor_columns(self) -> list[str]:
        """Collect sensor names from deviceConfig for CSV header order.
        Searches common keys per device: sensors, telemetry, signals, readings, measurements.
        Returns sorted unique sensor names (without device prefix)."""
        names: set[str] = set()
        cfg = self.deviceConfig
        if isinstance(cfg, dict):
            container = cfg.get("configs", cfg)
            if isinstance(container, dict):
                for _dev, devDict in container.items():
                    if not isinstance(devDict, dict):
                        continue
                    for key in ("sensors", "telemetry", "signals", "readings", "measurements"):
                        coll = devDict.get(key)
                        if isinstance(coll, dict):
                            for sig in coll.keys():
                                if isinstance(sig, str) and sig:
                                    names.add(sig)
        return sorted(names)

    def build_url(self) -> str:
        base, _ = self._base_and_auth()
        path = self.config.commands_path
        if not path.startswith("/"):
            path = "/" + path
        return base.rstrip("/") + path

    def set_server_ip(self, ip: str):
        self.config.redis_host = ip
        # Update API base as well:
        self.config.api_base = f"http://{ip}:8000"
        self.append_log(f"Server IP set to {ip}")
        self.start_redis()

    def set_api_credentials(self):
        dlg = CredentialsDialog(self.config.api_username, self.config.api_password, self)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        user, pwd = dlg.values()
        if not user or not pwd:
            QMessageBox.warning(self, "API Credentials", "Username and password cannot be empty.")
            return
        self.validate_auth(user, pwd)

    def validate_auth(self, user: str, pwd: str) -> None:
        base = self.config.api_base.strip().rstrip("/") or f"http://{self.config.redis_host}:8000"
        url = base + "/auth"
        self.append_log(f"Validating credentials at {url}")
        worker = HttpRequestWorker("GET", url, auth=(user, pwd))

        def on_ok(_payload: object) -> None:
            # Save only on success
            self.config.api_username = user
            self.config.api_password = pwd
            self.append_log("Auth OK: credentials accepted")
            self.statusBar().showMessage("Auth OK", 3000)

        def on_err(msg: str) -> None:
            self.append_log(f"Auth ERROR: {msg}")
            QMessageBox.warning(self, "Authentication Failed", f"/auth check failed:\n{msg}")

        worker.signals.success.connect(on_ok)
        worker.signals.error.connect(on_err)
        self.thread_pool.start(worker)

    def flush_pending_points(self):
        for series, t, val in self._pending_points:
            self.graphs.add_point(series, t, val)
        self._pending_points.clear()

    def toggle_log(self):
        self.log.setVisible(self.act_show_log.isChecked())

    @Slot()
    def on_ping(self) -> None:
        base, auth = self._base_and_auth()
        url = base.rstrip("/") + "/health"
        self.append_log(f"GET {url}")
        worker = HttpRequestWorker("GET", url, auth=auth)
        worker.signals.success.connect(lambda payload: self.append_log(f"Ping OK: {payload}"))
        worker.signals.error.connect(lambda msg: self.append_log(f"Ping ERROR: {msg}"))
        self.thread_pool.start(worker)


    def setControlButtonsToDefault(self) -> None:
        if not isinstance(self.deviceConfig, dict):
            return

        for devName, devDict in self.deviceConfig.get("configs", {}).items():
                self.append_log(f"Device found: {devName}")
                for control, controlDict in devDict.get("controls", {}).items():
                    control = control.upper()
                    defaultState = controlDict.get("defaultState", None)
                    if defaultState == "OPEN":
                        self.controlButtons[f"{control}_open"].setEnabled(False)
                        self.controlButtons[f"{control}_close"].setEnabled(True)
                    elif defaultState == "CLOSED":
                        self.controlButtons[f"{control}_open"].setEnabled(True)
                        self.controlButtons[f"{control}_close"].setEnabled(False)

    def handleConfigResponse(self, payload: dict) -> None:
        self.append_log(f"Config response: {payload}")
        # Stash config locally as dict
        try:
            if isinstance(payload, str):
                self.deviceConfig = json.loads(payload)
            else:
                self.deviceConfig = payload

            # WE NO LONGER SET TO DEFAULT ON BOOT. Button states are set by status requests using send_status_request()
            # self.setControlButtonsToDefault()
            # Apply any units from the config to graph titles/readouts
            self.apply_units_from_config()

        except Exception as e:
            self.append_log(f"Failed to parse config: {e}")

    def apply_units_from_config(self) -> None:
        """Extract units from the loaded deviceConfig and register them with the graphs.
        Tries a few common shapes:
          configs -> device -> (sensors|telemetry|signals|readings|measurements) -> name -> { unit|units|uom }
          configs -> device -> units{ name: unit }
        """
        cfg = self.deviceConfig
        if not isinstance(cfg, dict):
            return
        container = cfg.get("configs", cfg)
        if not isinstance(container, dict):
            return
        for dev, devDict in container.items():
            if not isinstance(devDict, dict):
                continue
            # Direct units map
            direct_units = devDict.get("units") or devDict.get("units_map")
            if isinstance(direct_units, dict):
                for sig, unit in direct_units.items():
                    if isinstance(sig, str) and isinstance(unit, str) and unit.strip():
                        self.graphs.set_unit(f"{dev}:{sig}", unit.strip())
                        self.graphs.set_unit(sig, unit.strip())
            # Nested collections
            for key in ("sensors", "telemetry", "signals", "readings", "measurements"):
                coll = devDict.get(key)
                if not isinstance(coll, dict):
                    continue
                for sig, meta in coll.items():
                    unit_val: Optional[str] = None
                    if isinstance(meta, dict):
                        unit_val = meta.get("unit") or meta.get("units") or meta.get("uom")
                    if isinstance(unit_val, str) and unit_val.strip():
                        self.graphs.set_unit(f"{dev}:{sig}", unit_val.strip())
                        self.graphs.set_unit(sig, unit_val.strip())

    def send_config_request(self) -> None:
        base, auth = self._base_and_auth()
        url = base.rstrip("/") + "/config"
        self.append_log(f"GET {url}")
        worker = HttpRequestWorker("GET", url, auth=auth)
        worker.signals.success.connect(self.handleConfigResponse)
        worker.signals.error.connect(lambda msg: self.append_log(f"Config ERROR: {msg}"))
        self.thread_pool.start(worker)

    def send_status_request(self) -> None:
        try:
            base, auth = self._base_and_auth()
            url = base.rstrip("/") + "/status"
            self.append_log(f"GET {url}")
            worker = HttpRequestWorker("GET", url, auth=auth)
            worker.signals.success.connect(lambda payload: self.append_log(f"Status OK: {payload}"))
            worker.signals.error.connect(lambda msg: self.append_log(f"Status ERROR: {msg}"))
            self.thread_pool.start(worker)
        except Exception as e:
            self.append_log(f"Failed to send status request: {e}")

    def send_command(self, payload: dict[str, Any]) -> HttpRequestWorker:
        url = self.build_url()
        _, auth = self._base_and_auth()
        self.append_log(f"POST {url} -> {payload}")
        worker = HttpRequestWorker("POST", url, json_body=payload, auth=auth)
        worker.signals.success.connect(lambda resp: self.append_log(f"Command OK: {resp}"))
        worker.signals.error.connect(lambda msg: self.append_log(f"Command ERROR: {msg}"))
        # track worker to prevent premature GC while running
        self._http_workers.add(worker)
        worker.signals.finished.connect(lambda: self._http_workers.discard(worker))
        self.thread_pool.start(worker)
        return worker

    @Slot()
    def start_redis(self) -> None:
        if self.redis_thread and self.redis_thread.isRunning():
            return
        try:
            host = self.config.redis_host
            port = int(self.config.redis_port)
            chan = self.config.redis_channel
            user = self.config.redis_username
            pwd = self.config.redis_password
        except Exception:
            QMessageBox.warning(self, "Redis", "Invalid host/port/channel/user/pass")
            return

        self.redis_thread = RedisTailer(host, port, chan, user, pwd)
        self.redis_thread.message.connect(lambda m: self.append_log(f"[redis] {m}"))
        self.redis_thread.message.connect(self.on_redis_message)
        self.redis_thread.status.connect(self.append_log)
        self.redis_thread.error.connect(lambda e: self.append_log(f"Redis ERROR: {e}"))
        self.redis_thread.start()


    @Slot()
    def stop_redis(self) -> None:
        if self.redis_thread and self.redis_thread.isRunning():
            self.redis_thread.stop()
            self.redis_thread.wait(2000)

    def send_control_command(self, name: str, action: str) -> None:
        name = name.strip()
        if action not in {"OPEN", "CLOSE", "DEFAULT"}:
            QMessageBox.warning(self, "Command", f"Unknown action: {action}")
            return

        if (name, action) == ("ALL", "DEFAULT"):
            # Set each valve to its default state from deviceConfig
            if not isinstance(self.deviceConfig, dict):
                return
            for devName, devDict in self.deviceConfig.get("configs", {}).items():
                for control, controlDict in devDict.get("controls", {}).items():
                    control_upper = control.upper()
                    defaultState = controlDict.get("defaultState", None)
                    if defaultState in {"OPEN", "CLOSED"}:
                        if defaultState == "OPEN":
                            action = "OPEN"
                        elif defaultState == "CLOSED":
                            action = "CLOSE"
                        else:
                            continue
                        self.send_control_command(control_upper, action)
            return

        self.send_command({"command": "CONTROL", "args": [name, action]})

    def on_gets(self) -> None:
        self.btn_gets.setEnabled(False)
        w = self.send_command({"command": "GETS", "args": []})
        w.signals.finished.connect(lambda: self.btn_gets.setEnabled(True))

    def on_stream(self) -> None:
        text = self.stream_rate_edit.text().strip()
        try:
            hz = int(text)
            if hz < 0:
                raise ValueError
        except Exception:
            QMessageBox.warning(self, "STREAM", f"Invalid Hz: {text}")
            return
        self.send_command({"command": "STREAM", "args": [str(hz)]})
        self.stream_rate_edit.setEnabled(False)
        self.btn_stream.setEnabled(False)

        if not self.logger.is_active:
            self._test_start_dt = datetime.now()
            columns = self._collect_sensor_columns()
            self.logger.start(self._log_dir, self._test_start_dt, columns=columns)
            self._log_timer.start()
            self.append_log(f"Logging to {self.logger.path}")

    def on_stop(self) -> None:
        self.send_command({"command": "STOP", "args": []})
        self.btn_stream.setEnabled(True)
        self.stream_rate_edit.setEnabled(True)

        # Stop logging
        if self.logger.is_active:
            self._log_timer.stop()
            p = self.logger.path  # keep for message before stop()
            self.logger.stop()
            if p:
                self.append_log(f"Log saved: {p}")

    def on_redis_message(self, m: str) -> None:
        m = m.split("]", 1)[-1]  # strip any leading timestamp
        m = m.strip()

        # Strict data-line matcher: "DEVICE <t> NAME:VAL" (no ANSI, one line)
        DATA_LOG_RE = re.compile(
            r'^(?P<device>\S+)\s+(?P<t>[+-]?\d+(?:\.\d+)?)\s+(?P<name>[A-Za-z0-9_]+):(?P<val>[+-]?\d+(?:\.\d+)?)$'
        )
        CONTROL_LOG_RE = re.compile(r'^(?P<device>\S+)\s+CONTROL\s+(?P<control>\S+)\s+(?P<action>\S+)$')
        STATUS_LOG_RE = re.compile(r'^(?P<device>\S+):\s+STATUS\s+(?P<status>.+)$')

        # Parse out data values
        dataMatch = DATA_LOG_RE.match(m)
        if dataMatch:
            self.handleDataString(dataMatch)
        controlMatch = CONTROL_LOG_RE.match(m)
        if controlMatch:
            self.handleControlString(controlMatch)
        statusMatch = STATUS_LOG_RE.match(m)
        if statusMatch:
            self.handleStatusString(statusMatch)

        if not dataMatch:
            return  # ignore non-data lines


    def handleDataString(self, data: re.Match) -> None:
        # Process the incoming data string
        try:
            device = data.group("device")
            t = float(data.group("t"))
            name = data.group("name")
            val = float(data.group("val"))
        except Exception:
            return

        if not (isfinite(t) and isfinite(val)):
            return

        # Keep device streams separate so they don't merge
        series = f"{device}:{name}"
        self._pending_points.append((series, t, val))

        if self.logger.is_active:
            self.logger.log(device, t, name, val)

    def handleControlString(self, data: re.Match) -> None:
        # device not used here, but parsed for completeness
        _device = data.group("device")
        control = data.group("control")
        action = data.group("action")

        if action == "opened":
            self.controlButtons[f"{control}_open"].setEnabled(False)
            self.controlButtons[f"{control}_close"].setEnabled(True)
        if action == "closed":
            self.controlButtons[f"{control}_close"].setEnabled(False)
            self.controlButtons[f"{control}_open"].setEnabled(True)

    def handleStatusString(self, data: re.Match) -> None:
        # device not used here, but parsed for completeness
        _device = data.group("device")
        status = data.group("status")

        ctlStatusDict = json.loads(status).get("controls", {})

        for control, status in ctlStatusDict.items():
            control = control.upper()
            if status == "OPEN":
                self.controlButtons[f"{control}_open"].setEnabled(False)
                self.controlButtons[f"{control}_close"].setEnabled(True)
            elif status == "CLOSED":
                self.controlButtons[f"{control}_close"].setEnabled(False)
                self.controlButtons[f"{control}_open"].setEnabled(True)


    def append_log(self, line: str) -> None:
        self.log.append(line)
        self.statusBar().showMessage(line, 3000)

    def closeEvent(self, event) -> None:
        try:
            self.stop_redis()
            if self.logger.is_active:
                self._log_timer.stop()
                self.logger.stop()
        finally:
            super().closeEvent(event)

    def _base_and_auth(self) -> tuple[str, Optional[tuple[str, str]]]:
        from urllib.parse import urlsplit, urlunsplit

        raw = self.config.api_base.strip()
        parts = urlsplit(raw)
        user = self.config.api_username.strip() or (parts.username or "")
        pwd = self.config.api_password.strip() or (parts.password or "")
        host = parts.hostname or ""
        netloc = host + (f":{parts.port}" if parts.port else "")
        base = urlunsplit((parts.scheme or "http", netloc, parts.path, parts.query, parts.fragment))
        auth = (user, pwd) if user and pwd else None
        return base, auth


def main() -> int:
    app = QApplication(sys.argv)
    win = MainWindow(CONFIG)
    win.showMaximized()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
