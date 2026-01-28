from pathlib import Path
import csv
from typing import List, Dict, Any

REPO_ROOT = Path(__file__).resolve().parents[4]
FACTORY_DIR = REPO_ROOT / 'factory'
INDICE_PATH = FACTORY_DIR / 'data' / '02_curated' / 'facts' / 'fato_indice_ismb.csv'
ANALYTICS_DIR = FACTORY_DIR / 'data' / '04_analytics'


def _read_csv_file(path: Path) -> List[Dict[str, Any]]:
	path = Path(path)
	if not path.exists():
		raise FileNotFoundError(str(path))
	data: List[Dict[str, Any]] = []
	with path.open(newline='', encoding='utf-8') as f:
		reader = csv.DictReader(f)
		for row in reader:

			for k, v in list(row.items()):
				if v is None or v == '':
					row[k] = None
					continue

				if k.lower().startswith('dat') or k.lower().startswith('date'):
					row[k] = v
					continue

				try:
					row[k] = float(v)
				except Exception:
					row[k] = v
			data.append(row)

	return data


def ler_csv(key_or_path: str):
	if key_or_path == 'indice':
		return _read_csv_file(INDICE_PATH)

	p = Path(key_or_path)
	if p.exists():
		return _read_csv_file(p)

	raise FileNotFoundError(key_or_path)


def list_analytics() -> List[str]:
	if not ANALYTICS_DIR.exists():
		return []
	files = [p.name for p in sorted(ANALYTICS_DIR.glob('*.csv'))]
	return files


def read_analytics(filename: str) -> List[Dict[str, Any]]:
	if Path(filename).name != filename:
		raise FileNotFoundError(filename)

	p = ANALYTICS_DIR / filename
	if not p.exists():
		p = p.with_suffix('.csv')
	if not p.exists():
		raise FileNotFoundError(filename)

	return _read_csv_file(p)


def get_indicador(nome: str):
	if not ANALYTICS_DIR.exists():
		raise FileNotFoundError(str(ANALYTICS_DIR))

	for p in ANALYTICS_DIR.glob('*.csv'):
		if p.stem.lower() == nome.lower() or nome.lower() in p.name.lower():
			return _read_csv_file(p)

	raise FileNotFoundError(nome)

