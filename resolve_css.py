from pathlib import Path
import sys
from logger import get_logger

logger = get_logger(__name__)


# HTML/Markdown Conversion
# _CSS_DIR = Path(__file__).resolve().parent / "css" 17/03/26
def get_base_path():
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent


# def _read_css_file(filename: str) -> str: 17/03/26
#     return (_CSS_DIR / filename).read_text(encoding="utf-8").strip()
def _read_css_file(CSS_DIR: Path, filename: str) -> str:
    try:
        path = CSS_DIR / filename
        if not path.exists():
            print(f"[WARNING] CSS file missing: {path}")
            return ""
        return path.read_text(encoding="utf-8").strip()
    except Exception as e:
        print(f"[ERROR] Failed to read CSS {filename}: {e}")
        return ""
