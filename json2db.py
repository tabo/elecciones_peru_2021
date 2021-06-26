import json
import logging
import math
import pathlib
import sqlite3
from sqlite_utils.cli import insert_upsert_implementation


class Converter:
    def __init__(self, base_dir, election1_id, election2_id):
        self.base_dir = base_dir
        self.election1_id = election1_id
        self.election2_id = election2_id
        self.data = {}
        self.dbfile = self.base_dir / "elecciones_peru_2021.db"
        self.db = sqlite3.connect(":memory:")
        self.ubigeos = {}

    def savedb(self):
        logging.info(f"Guardando base de datos a {self.dbfile}")
        bck = sqlite3.connect(self.dbfile)
        with bck:
            self.db.backup(bck, pages=0)
        self.db.close()
        bck.close()

    def process(self):
        self.create_functions()
        self.load_data()
        self.process_mesas_actas()
        self.process_ubigeos()
        self.process_ubigeos_locales()
        self.process_locales_mesas()
        self.create_summary()
        self.create_keikino59()
        self.create_v1_onpe_disparities()
        self.create_v2_onpe_disparities()
        self.analisis_payaso()
        self.savedb()
        self.onpe_pcm()
        logging.info("Ronderos triunfan otra vez!")

    def load_data(self):
        fname = self.base_dir / "data.json"
        logging.info(f"loading {fname}")
        self.data = json.loads(fname.read_text())

    def merged_ubigeos(self):
        res = {"departments": {}, "provinces": {}, "districts": {}}

        def proc(geo_hierarchy, name_field, desc_field, items, is_extranjero):
            for item in items:
                parent = item["CDGO_PADRE"] if item["CDGO_PADRE"] != "000000" else None
                res[geo_hierarchy][item[name_field]] = {
                    "codigo": item[name_field],
                    "pariente": parent,
                    "descripcion": item[desc_field],
                    "extranjero": is_extranjero,
                }

        for election_id, data in self.data.items():
            geo_regions_ = data["geo_regions"]
            proc(
                "departments",
                "CDGO_DEP",
                "DESC_DEP",
                geo_regions_["E"]["ubigeos"]["continents"],
                True,
            )
            proc(
                "provinces",
                "CDGO_PROV",
                "DESC_PROV",
                geo_regions_["E"]["ubigeos"]["countries"],
                True,
            )
            proc(
                "districts",
                "CDGO_DIST",
                "DESC_DIST",
                geo_regions_["E"]["ubigeos"]["states"],
                True,
            )
            proc(
                "departments",
                "CDGO_DEP",
                "DESC_DEP",
                geo_regions_["P"]["ubigeos"]["departments"],
                False,
            )
            proc(
                "provinces",
                "CDGO_PROV",
                "DESC_PROV",
                geo_regions_["P"]["ubigeos"]["provinces"],
                False,
            )
            proc(
                "districts",
                "CDGO_DIST",
                "DESC_DIST",
                geo_regions_["P"]["ubigeos"]["districts"],
                False,
            )

        return res

    def process_ubigeos(self):
        logging.info("procesando areas geograficas")
        self.ubigeos = self.merged_ubigeos()
        cur = self.db.cursor()
        with (self.base_dir / "sql/create_table_ubigeos.sql").open("r") as fh:
            cur.execute(fh.read())

        for geotype in ["departments", "provinces", "districts"]:
            values = self.ubigeos[geotype].values()
            cur.executemany(
                """
            INSERT INTO ubigeos(codigo, pariente, descripcion, extranjero)
             VALUES(:codigo, :pariente, :descripcion, :extranjero)
             """,
                values,
            )
        self.db.commit()

    def process_ubigeos_locales(self):
        logging.info("procesando locales en ubigeos")
        cur = self.db.cursor()
        for election_id, election_data in self.data.items():
            res = []
            tablename = f"locales_{election_id}"
            cur.execute(
                f"""
            CREATE TABLE {tablename} (
                ubigeo text,
                local text,
                direccion text,
                nombre text,
                PRIMARY KEY(local),
                FOREIGN KEY(ubigeo) REFERENCES ubigeos(codigo)
            )"""
            )
            for ubigeo, ubigeo_data in election_data["ubigeos"].items():
                assert len(ubigeo_data) == 1
                for local in ubigeo_data["locales"]:
                    res.append(
                        {
                            "ubigeo": local["CCODI_UBIGEO"],
                            "local": local["CCODI_LOCAL"],
                            "direccion": local["TDIRE_LOCAL"],
                            "nombre": local["TNOMB_LOCAL"],
                        }
                    )
            cur.executemany(
                f"""
            INSERT INTO {tablename}(ubigeo, local, direccion, nombre)
             VALUES(:ubigeo, :local, :direccion, :nombre)
             """,
                res,
            )
        self.db.commit()

    def process_locales_mesas(self):
        logging.info("procesando mesas en locales")
        cur = self.db.cursor()
        for election_id, election_data in self.data.items():
            res = []
            mesas_table = f"mesas_{election_id}"
            locales_table = f"locales_{election_id}"
            cur.execute(
                f"""
            CREATE TABLE {mesas_table} (
                ubigeo text,
                local text,
                mesa text,
                imagen integer,
                procesado integer,
                PRIMARY KEY(mesa),
                FOREIGN KEY(ubigeo) REFERENCES ubigeos(codigo),
                FOREIGN KEY(local) REFERENCES {locales_table}(local)
            )"""
            )
            for ubigeo, ubigeo_data in election_data["locales"].items():
                for local_id, local_data in ubigeo_data.items():
                    assert len(local_data) == 1
                    for mesadata in local_data["mesasVotacion"]:
                        res.append(
                            {
                                "ubigeo": ubigeo,
                                "local": local_id,
                                "mesa": mesadata["NUMMESA"],
                                "imagen": mesadata["IMAGEN"],
                                "procesado": mesadata["PROCESADO"],
                            }
                        )
            cur.executemany(
                f"""
            INSERT INTO {mesas_table}(ubigeo, local, mesa, imagen, procesado)
             VALUES(:ubigeo, :local, :mesa, :imagen, :procesado)
             """,
                res,
            )
        self.db.commit()

    def process_mesas_actas(self):
        logging.info("procesando actas en mesas")
        cur = self.db.cursor()
        for election_id, election_data in self.data.items():
            actas_res = []
            resol_res = []
            votos_res = []
            actas_table = f"actas_{election_id}"
            resol_table = f"resoluciones_{election_id}"
            votos_table = f"votos_{election_id}"
            mesas_table = f"mesas_{election_id}"
            cur.execute(
                f"""
            CREATE TABLE {actas_table} (
                tipo_acta text,
                mesa text,
                CCENT_COMPU text,
                CCODI_UBIGEO text,
                CCOPIA_ACTA text,
                DEPARTAMENTO text,
                DISTRITO text,
                NNUME_HABILM integer,
                N_CANDIDATOS integer,
                OBSERVACION text,
                OBSERVACION_TXT text,
                PROVINCIA text,
                TDIRE_LOCAL text,
                TNOMB_LOCAL text,
                TOT_CIUDADANOS_VOTARON integer,
                PRIMARY KEY(mesa, tipo_acta)
                FOREIGN KEY(CCODI_UBIGEO) REFERENCES ubigeos(codigo),
                FOREIGN KEY(mesa) REFERENCES {mesas_table}(mesa)
            )"""
            )
            cur.execute(
                f"""
            CREATE TABLE {resol_table} (
                tipo_acta text,
                CCENT_COMPU text,
                CNUME_RESOL text,
                CNUME_ACTA text,  -- mesa
                CESTADO_RESOL text,
                CPROCED_RESOL text,
                CNUME_RESOL_JNE text,
                PRIMARY KEY(CNUME_ACTA, CNUME_RESOL)
                FOREIGN KEY(CNUME_ACTA) REFERENCES {mesas_table}(mesa),
                FOREIGN KEY(CNUME_ACTA, tipo_acta) REFERENCES {actas_table}(mesa, tipo_acta)
            )"""
            )
            cur.execute(
                f"""
            CREATE TABLE {votos_table} (
                tipo_acta text,
                mesa text,
                AUTORIDAD text,
                CCODI_AUTO text,
                CON_EMITIDOS numeric,
                CON_VALIDOS numeric,
                NLISTA numeric,
                congresal numeric,
                PRIMARY KEY (mesa, tipo_acta, AUTORIDAD),
                FOREIGN KEY(mesa) REFERENCES {mesas_table}(mesa),
                FOREIGN KEY(mesa, tipo_acta) REFERENCES {actas_table}(mesa, tipo_acta)
            )"""
            )
            for mesa, mesa_data in election_data["mesas"].items():
                proc_data = mesa_data["procesos"]
                for seccion_en_json, tipo_acta in [
                    ("generalPre", "presidencial"),
                    # TODO: ("generalCon", "congresal"),
                    # TODO: ("generalPar", "parlamento")
                ]:
                    acta_data = proc_data[seccion_en_json]
                    acta_details = acta_data[tipo_acta]
                    acta_details.update({"mesa": mesa, "tipo_acta": tipo_acta})
                    actas_res.append(acta_details)
                    for resolucion in acta_data["resoluciones"]:
                        resolucion.pop("IMAGEN", None)
                        resolucion.update({"tipo_acta": tipo_acta})
                        resol_res.append(resolucion)
                    for voto in acta_data["votos"]:
                        voto.update({"mesa": mesa, "tipo_acta": tipo_acta})
                        if "NLISTA" not in voto:
                            voto["NLISTA"] = None
                        votos_res.append(voto)
            cur.executemany(
                f"""
            INSERT INTO {actas_table}(tipo_acta, mesa, CCENT_COMPU, CCODI_UBIGEO, CCOPIA_ACTA, DEPARTAMENTO, DISTRITO, NNUME_HABILM, N_CANDIDATOS, OBSERVACION, OBSERVACION_TXT, PROVINCIA, TDIRE_LOCAL, TNOMB_LOCAL, TOT_CIUDADANOS_VOTARON)
             VALUES(:tipo_acta, :mesa, :CCENT_COMPU, :CCODI_UBIGEO, :CCOPIA_ACTA, :DEPARTAMENTO, :DISTRITO, :NNUME_HABILM, :N_CANDIDATOS, :OBSERVACION, :OBSERVACION_TXT, :PROVINCIA, :TDIRE_LOCAL, :TNOMB_LOCAL, :TOT_CIUDADANOS_VOTARON)
             """,
                actas_res,
            )
            cur.executemany(
                f"""
            INSERT INTO {resol_table}(CCENT_COMPU, CNUME_RESOL, CNUME_ACTA, CESTADO_RESOL, CPROCED_RESOL, CNUME_RESOL_JNE, tipo_acta)
             VALUES(:CCENT_COMPU, :CNUME_RESOL, :CNUME_ACTA, :CESTADO_RESOL, :CPROCED_RESOL, :CNUME_RESOL_JNE, :tipo_acta)
             """,
                resol_res,
            )
            cur.executemany(
                f"""
            INSERT INTO {votos_table}(tipo_acta, mesa, AUTORIDAD, CCODI_AUTO, CON_EMITIDOS, CON_VALIDOS, NLISTA, congresal)
             VALUES(:tipo_acta, :mesa, :AUTORIDAD, :CCODI_AUTO, :CON_EMITIDOS, :CON_VALIDOS, :NLISTA, :congresal)
             """,
                votos_res,
            )
        self.db.commit()

    def create_summary(self):
        logging.info("creando resumen presidencial 2021")
        cur = self.db.cursor()
        with (self.base_dir / "sql/create_table_presidencial.sql").open("r") as fh:
            cur.execute(fh.read())

    def onpe_pcm(self):
        """✏️ Subimos a la base de datos rondera los datos abiertos publicados por ONPE

        Muy paja esto! Bien jugado ONPE! Veritas Liberabit Vos!

        Anuncio en: https://twitter.com/ONPE_oficial/status/1405924353615749128
        """
        buena_onpe = {
            "onpe_pcm_v1": "Resultados_1ra_vuelta_Version_PCM.csv",
            "onpe_pcm_v2": "Resultados_2da_vuelta_Version_PCM .csv",
        }
        for table, fname in buena_onpe.items():
            logging.info(f"Cargando datos abiertos de ONPE: {fname}")
            insert_upsert_implementation(
                path=self.dbfile,
                json_file=(self.base_dir / "_cache/onpe_pcm/" / fname).open("rb"),
                pk=None,
                nl=False,
                table=table,
                csv=True,
                tsv=False,
                delimiter=";",
                quotechar='"',
                sniff=False,
                no_headers=False,
                batch_size=4096,
                alter=False,
                upsert=False,
                encoding="latin-1",
            )

    def create_keikino59(self):
        """✏️ Lista de 59 mesas en el berrinche de Fuerza Popular

        Fuente: https://twitter.com/epatriau/status/1405721214123921409
        """
        logging.info("creando view con 59 mesas del berrinche de keiko")
        cur = self.db.cursor()
        with (self.base_dir / "sql/create_view_keikino.sql").open("r") as fh:
            cur.execute(fh.read())

    def create_v1_onpe_disparities(self):
        """✏️ Encuentra disparidades con open data de ONPE (1era vuelta)"""
        logging.info("creando views de disparidades de primera vuelta")
        cur = self.db.cursor()
        with (self.base_dir / "sql/create_view_v1_mesas_faltantes.sql").open("r") as fh:
            cur.execute(fh.read())
        with (self.base_dir / "sql/create_view_v1_diferencias_open_data.sql").open("r") as fh:
            cur.execute(fh.read())

    def create_v2_onpe_disparities(self):
        """✏️ Encuentra disparidades con open data de ONPE (2da vuelta)"""
        logging.info("creando views de disparidades de segunda vuelta")
        cur = self.db.cursor()
        with (self.base_dir / "sql/create_view_v2_mesas_faltantes.sql").open("r") as fh:
            cur.execute(fh.read())
        with (self.base_dir / "sql/create_view_v2_diferencias_open_data.sql").open("r") as fh:
            cur.execute(fh.read())

    def analisis_payaso(self):
        logging.info(f"Cargando puras payasadas 🤡")
        cur = self.db.cursor()
        with (self.base_dir / "sql/create_table_ratios_de_payaso.sql").open("r") as fh:
            logging.info("Creando tabla de ratios de payaso 🤡")
            cur.execute(fh.read())
        with (self.base_dir / "sql/insert_into_ratios_de_payaso.sql").open("r") as fh:
            logging.info("Cargando data de ratios de payaso 🤡")
            cur.execute(fh.read())
        with (self.base_dir / "sql/create_table_payasadas.sql").open("r") as fh:
            logging.info("Creando tabla de payasadas 🤡")
            cur.execute(fh.read())
        with (self.base_dir / "sql/create_view_payasada_summary.sql").open("r") as fh:
            logging.info("Creando view de resumen de payasadas 🤡")
            cur.execute(fh.read())
        self.db.commit()

    def create_functions(self):
        self.db.create_function("floor", 1, math.floor)
        self.db.create_function("power", 2, math.pow)


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="✏️ %(relativeCreated)6d ✏️ %(message)s",
    )
    base_dir = pathlib.Path(__file__).resolve().parent
    c = Converter(base_dir=base_dir, election1_id="20210411", election2_id="20210606")
    c.process()


if __name__ == "__main__":
    main()
