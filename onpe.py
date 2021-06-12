"""
✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️
✏️
✏️ Resultados de las elecciones presidenciales 2021 en Peru
✏️
✏️ 🙈 🙉 🙊
✏️
✏️ Obtiene resultados de manera local para análisis.
✏️ Prohibido el uso de este software si no eres rondero 🤠
✏️
✏️ -  Gustavo Picón (@tabo)
✏️
✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️
"""
import concurrent.futures
import json
import pathlib
from collections import defaultdict
from urllib.parse import urljoin
import logging

import cloudscraper


class Onpe:
    """
    ✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️
    ✏️ Esta vaina va a bajarse toda la API de actas de la ONPE
    ✏️
    ✏️ No abuses causa
    ✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️
    """

    APIBASE = "https://api.resultadossep.eleccionesgenerales2021.pe/"

    def __init__(self, base_dir):
        self.session = cloudscraper.create_scraper(
            browser={"browser": "firefox", "platform": "windows", "mobile": "False"}
        )
        self.base_dir = base_dir
        self.cache_dir = base_dir / "_cache"
        self.geo_regions = {}
        self.ubigeos = {}
        self.locales = defaultdict(dict)
        self.mesas = {}

    def get_cache_path_for_url(self, url):
        """✏️ este no es el cache que estas buscando"""
        res = url
        if res.startswith("/"):
            res = url[1:]
        if res.endswith("?name=param"):
            res = res[:-11] + ".json"
        return self.cache_dir / res

    def get(self, path):
        """✏️ jala la data del api de la onpe"""
        cached_path = self.get_cache_path_for_url(path)
        cached_path.parent.mkdir(parents=True, exist_ok=True)
        if cached_path.is_file():
            logging.info("       Recuperando cache recontrachévere 😏")
            return json.loads(cached_path.read_text())
        logging.info("       Solicitando información a ONPE 🥺")
        resp = self.session.get(urljoin(self.APIBASE, path))
        resp.raise_for_status()
        data = resp.json()
        cached_path.write_text(json.dumps(data, sort_keys=True, indent=4))
        return data

    def process_geo_region(self, region_type, ubigeo_descriptor):
        """✏️ aca centralizamos el thread pool pe causa

        lo dejamos con 8 workers pa no paltear a cloudflare 🥑
        """
        apipath = f"/ecp/ubigeos/{region_type}?name=param"
        resp = self.get(apipath)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            tasks = {
                executor.submit(self.ubigeo, ubig["CDGO_DIST"]): ubig["CDGO_DIST"]
                for ubig in resp["ubigeos"][ubigeo_descriptor]
            }
            for future in concurrent.futures.as_completed(tasks):
                ubig = tasks[future]
                logging.info(f"Procesado ubigeo {ubig}")
        self.geo_regions[region_type] = resp
        return resp

    def exterior(self):
        """✏️ aca vivo yo, yeee"""
        logging.info("🌎 Procesando ubigeos del exterior 🌎")
        return self.process_geo_region("E", "states")

    def peru(self):
        """✏️ la tierra de los ronderos, lo justo al fin!"""
        logging.info("🇵🇪 Procesando ubigeos peruanos 🇵🇪")
        return self.process_geo_region("P", "districts")

    def ubigeo(self, ubigeo):
        """✏️ entrada del procesamiento de ubigeos, distribuye a locales"""
        logging.info(f"🏢 Procesando locales de votación en {ubigeo}")
        resp = self.get(f"/mesas/locales/{ubigeo}?name=param")
        self.ubigeos[ubigeo] = resp
        for local in resp["locales"]:
            self.local(ubigeo, local["CCODI_LOCAL"])
        return resp

    def local(self, ubigeo, local):
        """✏️ procesa locales de votación y distribuye a las mesas"""
        logging.info(
            f"📕 Procesando mesas de votación en local {local}, ubigeo {ubigeo}"
        )
        resp = self.get(f"/mesas/actas/11/{ubigeo}/{local}?name=param")
        self.locales[ubigeo][local] = resp
        for mesa in resp["mesasVotacion"]:
            self.mesa(mesa["NUMMESA"])
        return resp

    def mesa(self, mesa):
        """✏️ jalamos los detalles de las mesas"""
        logging.info(f"  (╯°□°）╯︵ ┻━┻  Procesando mesa {mesa}")
        resp = self.get(f"/mesas/detalle/{mesa}?name=param")
        self.mesas[mesa] = resp
        return resp

    def lapicitos(self, start):
        """✏️ lapicitos ronderos triunfadores ✊🇵🇪✊🇵🇪✊🇵🇪✊🇵🇪✊🇵🇪✊🇵🇪  """
        dur = datetime.timedelta(seconds=time.perf_counter() - start)
        print("✨✏️" * 20)
        print(f"✨✏️ Ronderos procesaron todo en {dur} 🤠🤠🤠")
        print("✨✏️" * 20)

    def save(self):
        """✏️ guardamos todo lo que encontramos en un json gigante"""
        data_file = self.base_dir / "data.json"
        data = {
            "geo_regions": self.geo_regions,
            "ubigeos": self.ubigeos,
            "locales": self.locales,
            "mesas": self.mesas
        }
        logging.info("guardando evidencia de triunfo lapicito en data.json")
        data_file.write_text(json.dumps(data, sort_keys=True, indent=4))

    def process(self):
        """✏️ organizamos las llamadas a los thread pools y terminamos con lapicitos"""
        start = time.perf_counter()
        self.peru()
        self.exterior()
        self.save()
        self.lapicitos(start)


def main():
    """✏️ hello lapicitos"""
    logging.basicConfig(
        level=logging.DEBUG,
        format="✏️ %(relativeCreated)6d ✏️ %(threadName)s ✏️ %(message)s",
    )
    onpe = Onpe(pathlib.Path(__file__).resolve().parent)
    onpe.process()
    print("yee")


if __name__ == "__main__":
    main()
