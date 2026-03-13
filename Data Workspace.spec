# -*- mode: python ; coding: utf-8 -*-

import os
from PyInstaller.utils.hooks import collect_data_files
from PyInstaller.building.build_main import Analysis, PYZ, EXE, COLLECT

# --- Directories ---
BASE_DIR = os.path.abspath(".")
DIST_DIR = os.path.join(BASE_DIR, "dist")
BUILD_DIR = os.path.join(BASE_DIR, "build")

# --- Absolute paths to resources ---
ICON_FILE = os.path.join(BASE_DIR, "icon.svg")
CSS_DIR = os.path.join(BASE_DIR, "css")

# --- Collect additional data files ---
torch_data = collect_data_files("torch")
transformers_data = collect_data_files("transformers", include_py_files=True)
st_data = collect_data_files("sentence_transformers", include_py_files=True)
llama_cpp_data = collect_data_files("llama_cpp", include_py_files=True)  # new

# --- Datas: copy icon.svg and css folder to root of dist ---
datas = [
    (ICON_FILE, "."),
    (CSS_DIR, "css"),
] + torch_data + transformers_data + st_data + llama_cpp_data

# --- Hidden imports ---
hiddenimports = [
    "PySide6.QtCore",
    "PySide6.QtGui",
    "PySide6.QtWidgets",
    "anthropic",
    "openai",
    "altair",
    "sqlalchemy.dialects.sqlite",
    "sqlalchemy.dialects.mysql",
    "sqlalchemy.dialects.postgresql",
    "sqlalchemy.dialects.mssql",
    "sqlalchemy.dialects.oracle",
    "pymysql",
    "psycopg2",
    "pyodbc",
    "cx_Oracle",
    "oracledb",
    "sentence_transformers.SentenceTransformer",
    "transformers.models.auto.modeling_auto",
    "vl_convert",
    "llama_cpp",  # new
]

# --- Excludes ---
excludes = [
    "tensorflow",
    "keras",
    "tensorboard",
    "tkinter",
]

# --- Analysis ---
a = Analysis(
    ["main.py"],
    pathex=[BASE_DIR],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    excludes=excludes,
    noarchive=False,
)

# --- PYZ ---
pyz = PYZ(a.pure, a.zipped_data)

# --- EXE ---
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Data Workspace",
    console=True,
)

# --- COLLECT ---
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    name="Data Workspace",
    distpath=DIST_DIR,
)