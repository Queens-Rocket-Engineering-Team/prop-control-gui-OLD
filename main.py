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
        self.grid = QGridLayout(self)
        self.grid.setContentsMargins(0, 0, 0, 0)
        self.grid.setHorizontalSpacing(8)
        self.grid.setVerticalSpacing(8)
        self._order = []
        self._curves = {}
        self._data = {}
        self._last_t = {}  # track last timestamp per series to avoid duplicates

    def ensure_plot(self, name: str):
        if name in self._curves:
            return self._curves[name]
        row = len(self._order)
        col = 0  # Always column 0 for vertical stacking
        plot = pg.PlotWidget()
        plot.setTitle(name)
        plot.showGrid(x=True, y=True)
        curve = plot.plot()
        self.grid.addWidget(plot, row, col)
        self._order.append(name)
        self._curves[name] = curve
        self._data[name] = ([], [])
        return curve

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
        if len(xs) > self.max_points:
            drop = len(xs) - self.max_points
            del xs[:drop]
            del ys[:drop]
        curve.setData(xs, ys)

class MainWindow(QMainWindow):
    def __init__(self, config: Config):
        super().__init__()
        self.config = config
        self.setWindowTitle("Prop Control")
        self.showMaximized()

        self.thread_pool = QThreadPool.globalInstance()
        # Avoid saturating the machine with too many concurrent requests
        try:
            self.thread_pool.setMaxThreadCount(4)
        except Exception:
            pass
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

        except Exception as e:
            self.append_log(f"Failed to parse config: {e}")

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

    def on_stop(self) -> None:
        self.send_command({"command": "STOP", "args": []})
        self.btn_stream.setEnabled(True)
        self.stream_rate_edit.setEnabled(True)

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
    win.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
