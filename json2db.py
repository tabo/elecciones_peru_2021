import json
import logging
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
        bck = sqlite3.connect(self.dbfile)
        with bck:
            self.db.backup(bck, pages=0)
        self.db.close()
        bck.close()

    def process(self):
        self.load_data()
        self.process_mesas_actas()
        self.process_ubigeos()
        self.process_ubigeos_locales()
        self.process_locales_mesas()
        self.create_summary()
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
        cur.execute(
            """
        CREATE TABLE ubigeos (
            codigo text,
            pariente text,
            descripcion text,
            extranjero boolean,
            PRIMARY KEY(codigo),
            FOREIGN KEY(pariente) REFERENCES ubigeos(codigo)
        )"""
        )

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
        cur.execute(
            """
        CREATE TABLE presidencial AS
SELECT
       v2a.mesa,

       v2a.TOT_CIUDADANOS_VOTARON AS 'v2_TOT_CIUDADANOS_VOTARON',
       v2_fp.congresal AS 'v2_fp',
       v2_perulibre.congresal AS 'v2_perulibre',
       v2_emitidos.congresal AS 'v2_emitidos',
       v2_validos.congresal AS 'v2_validos',
       v2_blanco.congresal AS 'v2_blanco',
       v2_impugnados.congresal AS 'v2_impugnados',
       v2_nulos.congresal AS 'v2_nulos',

       v2a.OBSERVACION AS 'v2_OBSERVACION',
       v2a.OBSERVACION_TXT AS 'v2_OBSERVACION_TXT',

       v1a.TOT_CIUDADANOS_VOTARON AS 'v1_TOT_CIUDADANOS_VOTARON',
       v1_fp.congresal AS 'v1_fp',
       v1_perulibre.congresal AS 'v1_perulibre',
       v1_emitidos.congresal AS 'v1_emitidos',
       v1_validos.congresal AS 'v1_validos',
       v1_blanco.congresal AS 'v1_blanco',
       v1_impugnados.congresal AS 'v1_impugnados',
       v1_nulos.congresal AS 'v1_nulos',
       v1_ap.congresal AS 'v1_ap',
       v1_app.congresal AS 'v1_app',
       v1_avpais.congresal AS 'v1_avpais',
       v1_dd.congresal AS 'v1_dd',
       v1_fa.congresal AS 'v1_fa',
       v1_jpp.congresal AS 'v1_jpp',
       v1_sp.congresal AS 'v1_sp',
       v1_morado.congresal AS 'v1_morado',
       v1_pnp.congresal AS 'v1_pnp',
       v1_ppc.congresal AS 'v1_ppc',
       v1_pps.congresal AS 'v1_pps',
       v1_pp.congresal AS 'v1_pp',
       v1_runa.congresal AS 'v1_runa',
       v1_rp.congresal AS 'v1_rp',
       v1_upp.congresal AS 'v1_upp',
       v1_vn.congresal AS 'v1_vn',

       v1a.OBSERVACION AS 'v1_OBSERVACION',
       v1a.OBSERVACION_TXT AS 'v1_OBSERVACION_TXT',

       v2a.CCODI_UBIGEO AS 'v2_CCODI_UBIGEO',
       v2a.DEPARTAMENTO AS 'v2_DEPARTAMENTO',
       v2a.PROVINCIA AS 'v2_PROVINCIA',
       v2a.DISTRITO AS 'v2_DISTRITO',
       v2a.TDIRE_LOCAL AS 'v2_TDIRE_LOCAL',
       v2a.TNOMB_LOCAL AS 'v2_TNOMB_LOCAL',
       v2a.CCOPIA_ACTA AS 'v2_CCOPIA_ACTA',
       v2a.NNUME_HABILM AS 'v2_NNUME_HABILM',
       v2a.N_CANDIDATOS AS 'v2_N_CANDIDATOS',
       v2a.CCENT_COMPU AS 'v2_CCENT_COMPU',

       v1a.CCODI_UBIGEO AS 'v1_CCODI_UBIGEO',
       v1a.DEPARTAMENTO AS 'v1_DEPARTAMENTO',
       v1a.PROVINCIA AS 'v1_PROVINCIA',
       v1a.DISTRITO AS 'v1_DISTRITO',
       v1a.TDIRE_LOCAL AS 'v1_TDIRE_LOCAL',
       v1a.TNOMB_LOCAL AS 'v1_TNOMB_LOCAL',
       v1a.CCOPIA_ACTA AS 'v1_CCOPIA_ACTA',
       v1a.NNUME_HABILM AS 'v1_NNUME_HABILM',
       v1a.N_CANDIDATOS AS 'v1_N_CANDIDATOS',
       v1a.CCENT_COMPU AS 'v1_CCENT_COMPU'

FROM actas_20210606 AS v2a
    LEFT JOIN actas_20210411 AS v1a ON v1a.mesa=v2a.mesa AND v1a.tipo_acta=v2a.tipo_acta
    LEFT JOIN votos_20210411 AS v1_ap ON v2a.mesa = v1_ap.mesa AND v2a.tipo_acta = v1_ap.tipo_acta AND v1_ap.AUTORIDAD='ACCION POPULAR'
    LEFT JOIN votos_20210411 AS v1_app ON v2a.mesa = v1_app.mesa AND v2a.tipo_acta = v1_app.tipo_acta AND v1_app.AUTORIDAD='ALIANZA PARA EL PROGRESO'
    LEFT JOIN votos_20210411 AS v1_avpais ON v2a.mesa = v1_avpais.mesa AND v2a.tipo_acta = v1_avpais.tipo_acta AND v1_avpais.AUTORIDAD='AVANZA PAIS - PARTIDO DE INTEGRACION SOCIAL'
    LEFT JOIN votos_20210411 AS v1_dd ON v2a.mesa = v1_dd.mesa AND v2a.tipo_acta = v1_dd.tipo_acta AND v1_dd.AUTORIDAD='DEMOCRACIA DIRECTA'
    LEFT JOIN votos_20210411 AS v1_fa ON v2a.mesa = v1_fa.mesa AND v2a.tipo_acta = v1_fa.tipo_acta AND v1_fa.AUTORIDAD='EL FRENTE AMPLIO POR JUSTICIA, VIDA Y LIBERTAD'
    LEFT JOIN votos_20210411 AS v1_fp ON v2a.mesa = v1_fp.mesa AND v2a.tipo_acta = v1_fp.tipo_acta AND v1_fp.AUTORIDAD='FUERZA POPULAR'
    LEFT JOIN votos_20210411 AS v1_jpp ON v2a.mesa = v1_jpp.mesa AND v2a.tipo_acta = v1_jpp.tipo_acta AND v1_jpp.AUTORIDAD='JUNTOS POR EL PERU'
    LEFT JOIN votos_20210411 AS v1_sp ON v2a.mesa = v1_sp.mesa AND v2a.tipo_acta = v1_sp.tipo_acta AND v1_sp.AUTORIDAD='PARTIDO DEMOCRATICO SOMOS PERU'
    LEFT JOIN votos_20210411 AS v1_morado ON v2a.mesa = v1_morado.mesa AND v2a.tipo_acta = v1_morado.tipo_acta AND v1_morado.AUTORIDAD='PARTIDO MORADO'
    LEFT JOIN votos_20210411 AS v1_pnp ON v2a.mesa = v1_pnp.mesa AND v2a.tipo_acta = v1_pnp.tipo_acta AND v1_pnp.AUTORIDAD='PARTIDO NACIONALISTA PERUANO'
    LEFT JOIN votos_20210411 AS v1_perulibre ON v2a.mesa = v1_perulibre.mesa AND v2a.tipo_acta = v1_perulibre.tipo_acta AND v1_perulibre.AUTORIDAD='PARTIDO POLITICO NACIONAL PERU LIBRE'
    LEFT JOIN votos_20210411 AS v1_ppc ON v2a.mesa = v1_ppc.mesa AND v2a.tipo_acta = v1_ppc.tipo_acta AND v1_ppc.AUTORIDAD='PARTIDO POPULAR CRISTIANO - PPC'
    LEFT JOIN votos_20210411 AS v1_pps ON v2a.mesa = v1_pps.mesa AND v2a.tipo_acta = v1_pps.tipo_acta AND v1_pps.AUTORIDAD='PERU PATRIA SEGURA'
    LEFT JOIN votos_20210411 AS v1_pp ON v2a.mesa = v1_pp.mesa AND v2a.tipo_acta = v1_pp.tipo_acta AND v1_pp.AUTORIDAD='PODEMOS PERU'
    LEFT JOIN votos_20210411 AS v1_runa ON v2a.mesa = v1_runa.mesa AND v2a.tipo_acta = v1_runa.tipo_acta AND v1_runa.AUTORIDAD='RENACIMIENTO UNIDO NACIONAL'
    LEFT JOIN votos_20210411 AS v1_rp ON v2a.mesa = v1_rp.mesa AND v2a.tipo_acta = v1_rp.tipo_acta AND v1_rp.AUTORIDAD='RENOVACION POPULAR'
    LEFT JOIN votos_20210411 AS v1_emitidos ON v2a.mesa = v1_emitidos.mesa AND v2a.tipo_acta = v1_emitidos.tipo_acta AND v1_emitidos.AUTORIDAD='TOTAL VOTOS EMITIDOS'
    LEFT JOIN votos_20210411 AS v1_validos ON v2a.mesa = v1_validos.mesa AND v2a.tipo_acta = v1_validos.tipo_acta AND v1_validos.AUTORIDAD='TOTAL VOTOS VALIDOS'
    LEFT JOIN votos_20210411 AS v1_upp ON v2a.mesa = v1_upp.mesa AND v2a.tipo_acta = v1_upp.tipo_acta AND v1_upp.AUTORIDAD='UNION POR EL PERU'
    LEFT JOIN votos_20210411 AS v1_vn ON v2a.mesa = v1_vn.mesa AND v2a.tipo_acta = v1_vn.tipo_acta AND v1_vn.AUTORIDAD='VICTORIA NACIONAL'
    LEFT JOIN votos_20210411 AS v1_blanco ON v2a.mesa = v1_blanco.mesa AND v2a.tipo_acta = v1_blanco.tipo_acta AND v1_blanco.AUTORIDAD='VOTOS EN BLANCO'
    LEFT JOIN votos_20210411 AS v1_impugnados ON v2a.mesa = v1_impugnados.mesa AND v2a.tipo_acta = v1_impugnados.tipo_acta AND v1_impugnados.AUTORIDAD='VOTOS IMPUGNADOS'
    LEFT JOIN votos_20210411 AS v1_nulos ON v2a.mesa = v1_nulos.mesa AND v2a.tipo_acta = v1_nulos.tipo_acta AND v1_nulos.AUTORIDAD='VOTOS NULOS'
    LEFT JOIN votos_20210606 AS v2_fp ON v2a.mesa = v2_fp.mesa AND v2a.tipo_acta = v2_fp.tipo_acta AND v2_fp.AUTORIDAD='FUERZA POPULAR'
    LEFT JOIN votos_20210606 AS v2_perulibre ON v2a.mesa = v2_perulibre.mesa AND v2a.tipo_acta = v2_perulibre.tipo_acta AND v2_perulibre.AUTORIDAD='PARTIDO POLITICO NACIONAL PERU LIBRE'
    LEFT JOIN votos_20210606 AS v2_emitidos ON v2a.mesa = v2_emitidos.mesa AND v2a.tipo_acta = v2_emitidos.tipo_acta AND v2_emitidos.AUTORIDAD='TOTAL VOTOS EMITIDOS'
    LEFT JOIN votos_20210606 AS v2_validos ON v2a.mesa = v2_validos.mesa AND v2a.tipo_acta = v2_validos.tipo_acta AND v2_validos.AUTORIDAD='TOTAL VOTOS VALIDOS'
    LEFT JOIN votos_20210606 AS v2_blanco ON v2a.mesa = v2_blanco.mesa AND v2a.tipo_acta = v2_blanco.tipo_acta AND v2_blanco.AUTORIDAD='VOTOS EN BLANCO'
    LEFT JOIN votos_20210606 AS v2_impugnados ON v2a.mesa = v2_impugnados.mesa AND v2a.tipo_acta = v2_impugnados.tipo_acta AND v2_impugnados.AUTORIDAD='VOTOS IMPUGNADOS'
    LEFT JOIN votos_20210606 AS v2_nulos ON v2a.mesa = v2_nulos.mesa AND v2a.tipo_acta = v2_nulos.tipo_acta AND v2_nulos.AUTORIDAD='VOTOS NULOS'
    WHERE v2a.tipo_acta='presidencial'
    ;
        """
        )

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
