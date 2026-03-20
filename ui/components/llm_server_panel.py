"""Shared panel for downloading and hosting a local GGUF model.

This widget is embedded by both ``AIHostConfigDialog`` (self-host group) and
``LocalLLMSettingsDialog`` (host-a-model tab) to eliminate duplicated server
configuration UI.
"""

import os
from typing import Optional, List

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QFormLayout,
    QLabel,
    QLineEdit,
    QComboBox,
    QPushButton,
    QCheckBox,
    QSpinBox,
    QMessageBox,
    QProgressBar,
    QGroupBox,
    QFileDialog,
)

from core.config import ConfigManager
from core.constants import (
    HOSTED_LLM_DEFAULT_PORT,
    HOSTED_LLM_CONTEXT_SIZE,
    HOSTED_LLM_GPU_LAYERS,
)
from core.logger import get_logger

logger = get_logger(__name__)


class LLMServerPanel(QWidget):
    """Reusable panel: model selection, download, and server start/stop.

    Parameters
    ----------
    show_auto_start : bool
        When *True* an "Auto-start server when app launches" checkbox is
        shown inside the server-controls group box.
    parent : QWidget | None
        Optional parent widget.
    """

    # Emitted when the server-start thread finishes.
    # (success: bool, message_or_url: str)
    server_started = Signal(bool, str)

    def __init__(self, show_auto_start: bool = False, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._show_auto_start = show_auto_start

        # Thread references kept alive so QThread isn't garbage-collected
        self._download_thread = None  # ModelDownloadThread
        self._server_thread = None    # ServerStartThread

        # Catalog key list parallels combo items so we can resolve back
        self._catalog_keys: List[str] = []

        self._build_ui()

    # ------------------------------------------------------------------
    #  UI construction
    # ------------------------------------------------------------------
    def _build_ui(self):
        from model_manager import (
            get_recommended_models,
            list_available_models,
            is_llama_cpp_available,
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Dependency check banner
        if not is_llama_cpp_available():
            warn = QLabel(
                "\u26a0 llama-cpp-python is not installed.\n"
                "Run:  pip install llama-cpp-python\n"
                "Then restart the application."
            )
            warn.setWordWrap(True)
            warn.setStyleSheet("color: #e8a838; font-weight: bold;")
            layout.addWidget(warn)
            layout.addSpacing(3)

        desc = QLabel(
            "Download a model and let the application host it for you.\n"
            "No separate server setup required."
        )
        desc.setWordWrap(True)
        layout.addWidget(desc)
        layout.addSpacing(5)

        # ---- 1. Select a Model ----
        model_group = QGroupBox("1. Select a Model")
        mg_layout = QVBoxLayout(model_group)

        self.model_combo = QComboBox()
        catalog = get_recommended_models()

        for key, info in catalog.items():
            label = f"{info['name']}  ({info['size_gb']} GB)"
            if info.get("recommended"):
                label += "  \u2605"
            self.model_combo.addItem(label)
            self._catalog_keys.append(key)

        # Already-downloaded models that are not in the catalog
        existing = list_available_models()
        catalog_filenames = {v["filename"] for v in catalog.values()}
        for fname in existing:
            if fname not in catalog_filenames:
                self.model_combo.addItem(f"[downloaded] {fname}")
                self._catalog_keys.append(f"__local__{fname}")

        mg_layout.addWidget(self.model_combo)

        # Browse for a custom .gguf file
        browse_row = QHBoxLayout()
        self.custom_path_input = QLineEdit()
        self.custom_path_input.setPlaceholderText("Or browse for a .gguf file\u2026")
        browse_btn = QPushButton("Browse\u2026")
        browse_btn.setMaximumWidth(90)
        browse_btn.clicked.connect(self.browse_model)
        browse_row.addWidget(self.custom_path_input)
        browse_row.addWidget(browse_btn)
        mg_layout.addLayout(browse_row)

        layout.addWidget(model_group)

        # ---- 2. Download Model ----
        dl_group = QGroupBox("2. Download Model")
        dl_layout = QVBoxLayout(dl_group)

        self.download_btn = QPushButton("Download Selected Model")
        self.download_btn.clicked.connect(self.start_download)
        dl_layout.addWidget(self.download_btn)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        dl_layout.addWidget(self.progress_bar)

        self.progress_label = QLabel("")
        self.progress_label.setVisible(False)
        dl_layout.addWidget(self.progress_label)

        layout.addWidget(dl_group)

        # ---- 3. Start / Stop Server ----
        srv_group = QGroupBox("3. Start / Stop Server")
        sg_layout = QVBoxLayout(srv_group)

        srv_form = QFormLayout()
        hosted_cfg = ConfigManager.get_hosted_llm_config()

        self.port_spin = QSpinBox()
        self.port_spin.setRange(1024, 65535)
        self.port_spin.setValue(hosted_cfg.get("hosted_port", HOSTED_LLM_DEFAULT_PORT))
        self.port_spin.setToolTip(
            "The network port the local LLM server listens on.\n"
            "Change this if another service is already using the default port."
        )
        srv_form.addRow("Port:", self.port_spin)

        self.context_spin = QSpinBox()
        self.context_spin.setRange(512, 131072)
        self.context_spin.setSingleStep(512)
        self.context_spin.setValue(hosted_cfg.get("hosted_context_size", HOSTED_LLM_CONTEXT_SIZE))
        self.context_spin.setToolTip(
            "Maximum number of tokens the model can process in a single prompt/response.\n"
            "Higher values allow longer conversations but use more RAM/VRAM.\n"
            "Common values: 2048, 4096, 8192, 16384, 32768."
        )
        srv_form.addRow("Context Size:", self.context_spin)

        self.gpu_spin = QSpinBox()
        self.gpu_spin.setRange(0, 999)
        self.gpu_spin.setValue(hosted_cfg.get("hosted_gpu_layers", HOSTED_LLM_GPU_LAYERS))
        self.gpu_spin.setToolTip(
            "Number of model layers to offload to the GPU for faster inference.\n"
            "Set to 0 for CPU-only mode. Higher values use more VRAM but run faster.\n"
            "Set to 999 to offload all layers (requires sufficient VRAM)."
        )
        srv_form.addRow("GPU Layers:", self.gpu_spin)

        sg_layout.addLayout(srv_form)

        btn_row = QHBoxLayout()
        self.start_btn = QPushButton("Start Server")
        self.start_btn.clicked.connect(self.start_server)
        btn_row.addWidget(self.start_btn)

        self.stop_btn = QPushButton("Stop Server")
        self.stop_btn.clicked.connect(self.stop_server)
        btn_row.addWidget(self.stop_btn)
        sg_layout.addLayout(btn_row)

        self.status_label = QLabel("")
        sg_layout.addWidget(self.status_label)

        # Optional auto-start checkbox
        self.auto_start_cb = None
        if self._show_auto_start:
            self.auto_start_cb = QCheckBox("Auto-start server when app launches")
            self.auto_start_cb.setChecked(hosted_cfg.get("hosted_auto_start", False))
            sg_layout.addWidget(self.auto_start_cb)

        layout.addWidget(srv_group)

        # Initial status refresh
        self.refresh_server_status()

    # ------------------------------------------------------------------
    #  Model helpers
    # ------------------------------------------------------------------
    def browse_model(self):
        """Open a file dialog to choose a custom .gguf model file."""
        path, _ = QFileDialog.getOpenFileName(
            self, "Select GGUF Model", "", "GGUF Models (*.gguf);;All Files (*)"
        )
        if path:
            self.custom_path_input.setText(path)

    def get_selected_model_info(self) -> Optional[dict]:
        """Return catalog info dict or a pseudo-dict for a local/custom file."""
        from model_manager import get_models_directory, get_recommended_models

        # Custom path takes priority
        custom = self.custom_path_input.text().strip()
        if custom:
            return {
                "filename": os.path.basename(custom),
                "url": None,
                "path": custom,
                "size_gb": 0,
            }

        idx = self.model_combo.currentIndex()
        if idx < 0:
            return None

        key = self._catalog_keys[idx]
        catalog = get_recommended_models()

        if key.startswith("__local__"):
            fname = key.replace("__local__", "")
            return {
                "filename": fname,
                "url": None,
                "path": os.path.join(get_models_directory(), fname),
                "size_gb": 0,
            }
        else:
            info = catalog[key]
            return {
                "filename": info["filename"],
                "url": info["url"],
                "path": os.path.join(get_models_directory(), info["filename"]),
                "size_gb": info["size_gb"],
            }

    # ------------------------------------------------------------------
    #  Download
    # ------------------------------------------------------------------
    def start_download(self):
        """Validate selection and kick off a background download."""
        # Import thread class lazily to avoid circular imports
        from ui.dialogs.llm_host import ModelDownloadThread

        info = self.get_selected_model_info()
        if not info:
            QMessageBox.warning(self, "No Model", "Select a model first.")
            return

        if not info.get("url"):
            if os.path.isfile(info.get("path", "")):
                QMessageBox.information(
                    self,
                    "Already Available",
                    f"Model file already exists:\n{info['path']}",
                )
            else:
                QMessageBox.warning(
                    self,
                    "No URL",
                    "This model has no download URL. Browse for a local .gguf file.",
                )
            return

        from model_manager import check_disk_space

        if not check_disk_space(info["size_gb"] * 1.1):
            QMessageBox.warning(
                self,
                "Disk Space",
                f"Not enough disk space. ~{info['size_gb']:.1f} GB required.",
            )
            return

        self.download_btn.setEnabled(False)
        self.download_btn.setText("Downloading\u2026")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.progress_label.setVisible(True)
        self.progress_label.setText("Starting download\u2026")

        self._download_thread = ModelDownloadThread(info["url"], info["filename"], self)
        self._download_thread.progress.connect(self.on_download_progress)
        self._download_thread.finished.connect(self.on_download_finished)
        self._download_thread.start()

    def on_download_progress(self, pct: float, downloaded: float, total: float):
        self.progress_bar.setValue(int(pct))
        dl_mb = downloaded / (1024 * 1024)
        tot_mb = total / (1024 * 1024)
        self.progress_label.setText(
            f"{dl_mb:.0f} MB / {tot_mb:.0f} MB  ({pct:.1f}%)"
        )

    def on_download_finished(self, success: bool, message: str):
        self.download_btn.setEnabled(True)
        self.download_btn.setText("Download Selected Model")
        self.progress_bar.setVisible(False)
        self.progress_label.setVisible(False)

        if success:
            QMessageBox.information(self, "Download Complete", message)
        else:
            QMessageBox.critical(self, "Download Failed", message)

    # ------------------------------------------------------------------
    #  Server start / stop
    # ------------------------------------------------------------------
    def start_server(self):
        """Validate, then start the hosted LLM server on a background thread."""
        from model_manager import is_llama_cpp_available
        from ui.dialogs.llm_host import ServerStartThread

        if not is_llama_cpp_available():
            QMessageBox.warning(
                self,
                "Missing Dependency",
                "llama-cpp-python is not installed.\n\n"
                "Install it with:\n  pip install llama-cpp-python\n\n"
                "Then restart the application.",
            )
            return

        info = self.get_selected_model_info()
        if not info:
            QMessageBox.warning(self, "No Model", "Select a model first.")
            return

        model_path = info["path"]
        if not os.path.isfile(model_path):
            QMessageBox.warning(
                self,
                "Model Not Found",
                f"Model file not found:\n{model_path}\n\nDownload it first.",
            )
            return

        port = self.port_spin.value()
        context_size = self.context_spin.value()
        gpu_layers = self.gpu_spin.value()

        self.start_btn.setEnabled(False)
        self.start_btn.setText("Starting\u2026")
        self.status_label.setText("Starting server \u2014 this may take a moment\u2026")

        self._server_thread = ServerStartThread(
            model_path, port, gpu_layers, context_size, self
        )
        self._server_thread.finished.connect(self.on_server_started)
        self._server_thread.start()

    def on_server_started(self, success: bool, message: str):
        self.start_btn.setEnabled(True)
        self.start_btn.setText("Start Server")

        if success:
            QMessageBox.information(self, "Server Started", message)
        else:
            QMessageBox.critical(self, "Server Failed", message)

        self.refresh_server_status()

        # Emit signal so owning dialogs can react (e.g. update URL input)
        self.server_started.emit(success, message)

    def stop_server(self):
        """Stop the hosted LLM server."""
        from model_manager import stop_model_server

        ok, msg = stop_model_server()
        if ok:
            QMessageBox.information(self, "Server Stopped", msg)
        else:
            QMessageBox.warning(self, "Error", msg)
        self.refresh_server_status()

    def refresh_server_status(self):
        """Query the model-manager for running-server state and update UI."""
        from model_manager import get_server_status

        status = get_server_status()
        if status["running"]:
            self.status_label.setText(
                f"\u25cf Server running  (PID {status['pid']},  {status['url']})"
            )
            self.status_label.setStyleSheet("color: #4caf50; font-weight: bold;")
            self.start_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
        else:
            self.status_label.setText("\u25cb Server not running")
            self.status_label.setStyleSheet("color: gray;")
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
