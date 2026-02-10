from __future__ import annotations

from pathlib import Path

REQUIRED_DIRS = [
    # raw layer (your contract)
    Path("raw/milagros/births"),

    # processed layers (your current outputs)
    Path("processed/silver/milagros"),
    Path("processed/gold/milagros"),

    # optional: logs/artifacts
    Path("artifacts"),
]

def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    for rel in REQUIRED_DIRS:
        p = repo_root / rel
        p.mkdir(parents=True, exist_ok=True)
        # keep empty dirs tracked if you want (optional)
        gitkeep = p / ".gitkeep"
        if not any(p.iterdir()):
            gitkeep.touch(exist_ok=True)

    print("[infra] bootstrapped local directories:")
    for rel in REQUIRED_DIRS:
        print(f"  - {rel}")

if __name__ == "__main__":
    main()