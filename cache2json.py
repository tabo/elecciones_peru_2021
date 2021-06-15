import pathlib
import logging
from collections import defaultdict
import json


class Cache2Json:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.data = {}

    def process(self):
        cache_dir = self.base_dir / "_cache"
        election_dirs = (cd for cd in cache_dir.iterdir() if cd.is_dir())

        for election_dir in election_dirs:
            election_name = election_dir.name
            logging.info(f"Procesando elección {election_name}")
            self.data[election_name] = data = {
                "geo_regions": {},
                "ubigeos": {},
                "mesas": {},
                "locales": defaultdict(dict)
            }
            logging.info("Procesando ubigeos")
            for json_file in (election_dir / "mesas/actas/11/").glob("**/*.json"):
                data["locales"][json_file.parent.stem][json_file.stem] = json.loads(json_file.read_text())
            for json_file in (election_dir / "ecp/ubigeos").glob("*.json"):
                data["geo_regions"][json_file.stem] = json.loads(json_file.read_text())
            logging.info("Procesando locales")
            for json_file in (election_dir / "mesas/locales").glob("*.json"):
                data["ubigeos"][json_file.stem] = json.loads(json_file.read_text())
            logging.info("Procesando detalles")
            for json_file in (election_dir / "mesas/detalle").glob("*.json"):
                data["mesas"][json_file.stem] = json.loads(json_file.read_text())
            logging.info("Procesando actas")
        data_file = self.base_dir / "data.json"
        logging.info(f"Ronderos guardando data en {data_file}")
        data_file.write_text(json.dumps(self.data, sort_keys=True, indent=4))


def main():
    """✏️ hello lapicitos"""
    logging.basicConfig(
        level=logging.DEBUG,
        format="✏️ %(relativeCreated)6d ✏️ %(message)s",
    )
    base_dir = pathlib.Path(__file__).resolve().parent
    c2j = Cache2Json(base_dir=base_dir)
    c2j.process()


if __name__ == "__main__":
    main()
