#!/usr/bin/env python3
"""
Run The Referee backend server
"""
import uvicorn
from pathlib import Path

# Collect all files to watch for auto-reload
def get_extra_files():
    extra_files = []
    backend_dir = Path(__file__).parent

    for pattern in ["**/*.py", "**/*.json", "**/*.yaml"]:
        for f in backend_dir.glob(pattern):
            if f.is_file() and "venv" not in str(f) and "__pycache__" not in str(f):
                extra_files.append(str(f))

    return extra_files


if __name__ == "__main__":
    extra_files = get_extra_files()
    print(f"🚀 The Referee API starting...")
    print(f"   Auto-reload: ENABLED")
    print(f"   👁️  Watching {len(extra_files)} files for changes")
    print(f"   📚 Docs: http://localhost:8000/docs")

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_includes=["*.py", "*.json"],
    )
