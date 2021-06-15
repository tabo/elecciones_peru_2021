import json
import logging
import pathlib
import sqlite3


class Converter:
    def __init__(self, base_dir, election1_id, election2_id):
        self.base_dir = base_dir
        self.election1_id = election1_id
        self.election2_id = election2_id
        self.data_file = base_dir / f"data_{self.election1_id}.json"
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
        self.process_ubigeos()
        self.process_ubigeos_locales()
        self.process_locales_mesas()
        self.savedb()
        logging.info("procesao!")

    def load_data(self):
        for election_id in (self.election1_id, self.election2_id):
            self.data[election_id] = json.loads(
                (self.base_dir / f"data_{election_id}.json").read_text()
            )

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
        self.ubigeos = self.merged_ubigeos()
        cur = self.db.cursor()
        cur.execute(
            """
        CREATE TABLE ubigeos (
            codigo text,
            pariente text,
            descripcion text,
            extranjero boolean,
            FOREIGN KEY(pariente) REFERENCES ubigeos(codigo)
        )"""
        )
        cur.execute("CREATE UNIQUE INDEX codigo_pk ON ubigeos(codigo)")

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
                FOREIGN KEY(ubigeo) REFERENCES ubigeos(codigo)
            )"""
            )
            cur.execute(
                f"CREATE UNIQUE INDEX {tablename}_pk ON {tablename}(ubigeo, local)"
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
        cur = self.db.cursor()
        for election_id, election_data in self.data.items():
            res = []
            tablename = f"mesas_{election_id}"
            cur.execute(
                f"""
            CREATE TABLE {tablename} (
                ubigeo text,
                local text,
                mesa text,
                imagen integer,
                procesado integer,
                FOREIGN KEY(ubigeo) REFERENCES ubigeos(codigo),
                FOREIGN KEY(ubigeo, local) REFERENCES locales_{election_id}(ubigeo, local)
            )"""
            )
            cur.execute(
                f"CREATE UNIQUE INDEX {tablename}_pk ON {tablename}(ubigeo, local, mesa)"
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
            INSERT INTO {tablename}(ubigeo, local, mesa, imagen, procesado)
             VALUES(:ubigeo, :local, :mesa, :imagen, :procesado)
             """,
                res,
            )
        self.db.commit()


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
