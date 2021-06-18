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
import datetime
import json
import pathlib
import time
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

    def __init__(self, apibase, cache_token, base_dir, ignore_cache):
        self.session = cloudscraper.create_scraper(
            browser={"browser": "firefox", "platform": "windows", "mobile": "False"}
        )
        self.apibase = apibase
        self.cache_token = cache_token
        self.base_dir = base_dir
        self.ignore_cache = ignore_cache
        self.cache_dir = base_dir / "_cache" / self.cache_token

    def get_cache_path_for_url(self, url):
        """✏️ este no es el cache que estas buscando"""
        res = url
        if res.startswith("/"):
            res = url[1:]
        if res.endswith("?name=param"):
            res = res[:-11] + ".json"
        return self.cache_dir / res

    def get(self, path, hook=None):
        """✏️ jala la data del api de la onpe

        El hook es un rondero digital que va a post-procesar data antes de cachearla"""
        cached_path = self.get_cache_path_for_url(path)
        cached_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.ignore_cache and cached_path.is_file():
            # logging.info("       Recuperando cache recontrachévere 😏")
            return json.loads(cached_path.read_text())
        # logging.info("       Solicitando información a ONPE 🥺")
        url = urljoin(self.apibase, path.lstrip("/"))
        resp = self.session.get(url)
        resp.raise_for_status()
        data = resp.json()
        if hook is not None:
            hook(data)
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
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f"🧨🧨🧨🧨🧨🧨🧨🧨🧨 laca causa, pero nada detiene al lapiz, sin miedo!: {ubig} - {exc}")
                # logging.info(f"Procesado ubigeo {ubig}")
        return resp

    def exterior(self):
        """✏️ aca vivo yo, yeee"""
        # logging.info("🌎 Procesando ubigeos del exterior 🌎")
        return self.process_geo_region("E", "states")

    def peru(self):
        """✏️ la tierra de los ronderos, lo justo al fin!"""
        # logging.info("🇵🇪 Procesando ubigeos peruanos 🇵🇪")
        return self.process_geo_region("P", "districts")

    def ubigeo(self, ubigeo):
        """✏️ entrada del procesamiento de ubigeos, distribuye a locales"""
        # logging.info(f"🏢 Procesando locales de votación en {ubigeo}")
        resp = self.get(f"/mesas/locales/{ubigeo}?name=param")
        for local in resp["locales"]:
            self.local(ubigeo, local["CCODI_LOCAL"])
        return resp

    def local(self, ubigeo, local):
        """✏️ procesa locales de votación y distribuye a las mesas"""
        # logging.info(
        #     f"📕 Procesando mesas de votación en local {local}, ubigeo {ubigeo}"
        # )
        resp = self.get(f"/mesas/actas/11/{ubigeo}/{local}?name=param")
        for mesa in resp["mesasVotacion"]:
            self.mesa(mesa["NUMMESA"])
        return resp

    def mesa(self, mesa):
        """✏️ jalamos los detalles de las mesas"""
        # logging.info(f"  (╯°□°）╯︵ ┻━┻  Procesando mesa {mesa}")

        def hook(data):
            """nos mochamos links a imagenes que expiran"""
            for proceso_data in data.get("procesos", {}).values():
                if isinstance(proceso_data, dict):
                    proceso_data.pop("imageActa", None)
                    for resol in proceso_data.get("resoluciones", []):
                        resol.pop("IMAGEN", None)

        resp = self.get(f"/mesas/detalle/{mesa}?name=param", hook)
        return resp

    def lapicitos(self, start):
        """✏️ lapicitos ronderos triunfadores ✊🇵🇪✊🇵🇪✊🇵🇪✊🇵🇪✊🇵🇪✊🇵🇪  """
        dur = datetime.timedelta(seconds=time.perf_counter() - start)
        print("✨✏️" * 20)
        print(f"✨✏️ Ronderos procesaron todo en {dur} 🤠🤠🤠")
        print("✨✏️" * 20)

    def process(self):
        """✏️ organizamos las llamadas a los thread pools y terminamos con lapicitos"""
        start = time.perf_counter()
        self.peru()
        self.exterior()
        self.lapicitos(start)


def main():
    """✏️ hello lapicitos"""
    logging.basicConfig(
        level=logging.DEBUG,
        format="✏️ %(relativeCreated)6d ✏️ %(threadName)s ✏️ %(message)s",
    )
    base_dir = pathlib.Path(__file__).resolve().parent
    onpe2021_2 = Onpe(
        apibase="https://api.resultadossep.eleccionesgenerales2021.pe/",
        cache_token="20210606",
        base_dir=base_dir,
        ignore_cache=True,
    )
    # onpe2021_2.process()
    onpe2021_1 = Onpe(
        apibase="https://resultadoshistorico.onpe.gob.pe/v1/EG2021/",
        cache_token="20210411",
        base_dir=base_dir,
        ignore_cache=False,
    )
    onpe2021_1.process()


if __name__ == "__main__":
    main()
