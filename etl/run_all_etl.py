import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Directorio base del proyecto (carpeta raíz)
BASE_DIR = Path(__file__).resolve().parent.parent

# Scripts ETL internos (HU1: 3 fuentes internas)
SCRIPTS = [
    "etl/internal/ingesta_viajes_validados.py",
    "etl/internal/ingesta_densidad.py",
    "etl/internal/ingesta_temperatura.py",
    "etl/external/ingesta_externa.py",
]

# Carpeta y archivo de log
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / f"etl_{datetime.now():%Y%m%d_%H%M%S}.log"


def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}\n"
    print(line, end="")
    with log_file.open("a", encoding="utf-8") as f:
        f.write(line)


def run_script(relative_path: str):
    script_path = BASE_DIR / relative_path
    log(f"Inicio script: {script_path}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
    )

    # Guardamos stdout y stderr en el log
    if result.stdout:
        with log_file.open("a", encoding="utf-8") as f:
            f.write(result.stdout + "\n")

    if result.stderr:
        with log_file.open("a", encoding="utf-8") as f:
            f.write("STDERR:\n" + result.stderr + "\n")

    if result.returncode != 0:
        log(f"[ERROR] Script falló: {script_path} (code={result.returncode})")
    else:
        log(f"[OK] Script completado: {script_path}")


def main():
    log("===== INICIO ETL DIARIO MCP (HU1) =====")
    for script in SCRIPTS:
        run_script(script)
    log("===== FIN ETL DIARIO MCP (HU1) =====")


if __name__ == "__main__":
    main()
