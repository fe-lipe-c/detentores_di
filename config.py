from pathlib import Path
import pandas as pd

DATA_PATH = Path("__file__").parent / "data"
REFERENCE_PATH = DATA_PATH / "reference"

# Create folders if they don't exist.
Path.mkdir(REFERENCE_PATH, parents=True, exist_ok=True)

# Load 'participantes' data.
DI_PARTICIPANTES = pd.read_csv(REFERENCE_PATH / "participantes.csv", sep=";")
DI_PARTICIPANTES = dict(zip(DI_PARTICIPANTES["Código"], DI_PARTICIPANTES["Nome"]))
