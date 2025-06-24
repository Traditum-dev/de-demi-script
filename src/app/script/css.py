import datetime
import sys
import os
import pytz
from uuid import uuid4
import logging
import functools
from io import StringIO
import base64
import os
from google.cloud import storage
import psycopg2
import pandas as pd

from app.core.settings import settings
from app.core.database import connect


def disable_print_if_verbose_decorator(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not getattr(self, "verbose", False):
            original_stdout = sys.stdout
            sys.stdout = open(os.devnull, "w")
            try:
                return func(self, *args, **kwargs)
            finally:
                sys.stdout.close()
                sys.stdout = original_stdout
        else:
            return func(self, *args, **kwargs)

    return wrapper


class ScriptCss:
    def __init__(
        self,
        connection: psycopg2.extensions.connection,
        verbose: bool = False,
        gcloud: bool = False,
    ):
        self.connection = connection
        self.verbose = verbose
        self.gcloud = gcloud
        self.logger = logging.getLogger(__name__)

    @disable_print_if_verbose_decorator
    def load_new_data(self) -> pd.DataFrame:
        gcloud = self.gcloud
        if gcloud:
            print("Loading data from bucket in GCP...")
            logging.info("Loading data from bucket in GCP...")
            client = storage.Client()
            bucket = settings.GCLOUD_BUCKET
            blobs = bucket.list_blobs(prefix=f"/CSS")
            latest_blob: storage.Blob = max(blobs, key=lambda x: x.updated)
            content = latest_blob.download_as_text(encoding="latin-1")
            data = pd.read_csv(StringIO(content), encoding="latin-1")
            client.close()

        else:
            print("Loading data from local file...")
            logging.info("Loading data from local file...")
            data = pd.read_csv("CSS-Informe_Afiliados.txt", encoding="latin-1", sep="|")

        print("Data loaded successfully!")
        logging.info("Data loaded successfully!")
        logging.info("-" * 30)

        return data

    def load_old_data(self):
        """
        CSS col ref:
        afi
        'NUMEROTARJETA',ta
        'ID_AFILIADO',
        'ID_TITULAR' ta
        'ID_TIPOPARENTESCO',ta
        'TIPO_PARENTESCO', ta

        persona
        'APELLIDO_NOMBRE',ta
        'FECHA_NACIMIENTO',ta
        'SEXO',ta

        persona doc
        'TIPO_DOCUMENTO',ta
        'NUMERODOCUMENTO',ta

        contacto
        'EMAIL',ta
        'TELEFONO',ta

        afi plan
        'NOMBRE_PLAN',
        'ESTADO_AFILIACION',
        'TIPO_CONDICION',
        'MOROSO',

        dato bancario
        'CBU',

        domicilio
        'PROVINCIA',
        'LOCALIDAD',
        'CODIGO_POSTAL',
        'CALLE',
        'NUMERO',
        'PISO',
        'DEPARTAMENTO',
        """
        print("Querying data from core...")
        logging.info("Querying data from core...")
        query = """SELECT
        afiliado.id AS id_afi,
        afiliado_plan.id AS id_afiliado_plan,
        id_afiliado_titular,
        persona.id AS id_persona,
        codigo,
        persona.nombre,
        apellido,
        genero_biologico,
        fecha_nacimiento,
        afiliado_parentezco_tipo.tipo AS tipo_parentezco,
        persona_documento.id_param_documento_identificatorio,
        persona_documento.valor AS n_documento,
        param_documento_identificatorio.tipo AS tipo_doc,
        contacto.tipo AS tipo_contacto,
        financiadora_plan.nombre as nombre_plan

        FROM afiliado
        LEFT JOIN persona ON persona.id = afiliado.id_persona
        LEFT JOIN persona_documento ON persona_documento.id_persona = persona.id
        LEFT JOIN param_documento_identificatorio ON param_documento_identificatorio.id = persona_documento.id_param_documento_identificatorio
        LEFT JOIN persona_contacto ON persona_contacto.id_persona = persona.id
        LEFT JOIN contacto ON contacto.id = persona_contacto.id_contacto
        LEFT JOIN afiliado_parentezco_tipo ON afiliado_parentezco_tipo.id = afiliado.id_afiliado_parentezco_tipo
        LEFT JOIN afiliado_plan ON afiliado_plan.id_afiliado = afiliado.id
        LEFT JOIN financiadora_plan ON financiadora_plan.id = afiliado_plan.id_financiadora_plan
        WHERE afiliado.id_financiadora = 'ec5b3f64-aaed-4024-959d-f77346f11a01'
        """
        df = pd.read_sql(query, con=self.connection)
        return df

    def generate_base32():
        return base64.b32encode(os.urandom(20)).decode("utf-8")

    def insert_missing_afiliados(self, missing_df: pd.DataFrame):
        cursor = self.connection.cursor()
        try:
            for row in missing_df.itertuples(index=False):
                ########################################################################################################
                # auth
                auth_role_entity_query = """
                INSERT INTO auth_role_entity (id, type)
                VALUES (%s, %s)
                """
                id_afiliado = str(uuid4())
                values_auth_role_entity = (id_afiliado, "afiliado")
                cursor.execute(auth_role_entity_query, values_auth_role_entity)
                ########################################################################################################
                # persona
                fecha_nacimiento = getattr(row, "FECHA_NACIMIENTO", "")
                nombre = getattr(row, "NOMBRE", "")
                apellido = getattr(row, "APELLIDO", "")
                genero_out = getattr(row, "SEXO", "")
                # persona
                persona_query = """
                INSERT INTO persona (nombre, apellido, fecha_nacimiento, genero_biologico)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """
                print(nombre, apellido, fecha_nacimiento, genero_out)
                cursor.execute(
                    persona_query, (nombre, apellido, fecha_nacimiento, genero_out)
                )
                id_persona = cursor.fetchone()[0]
                ########################################################################################################
                # persona_documento
                valor = getattr(row, "NUMERODOCUMENTO", None)
                id_param_documento_identificatorio = getattr(row, "TIPO_DOCUMENTO", "")
                ########################################################################################################
                # persona documento
                id_persona_documento = str(uuid4())
                persona_documento_query = """
                INSERT INTO persona_documento (id, id_persona, id_param_documento_identificatorio, valor)
                VALUES (%s, %s, %s, %s)
                """
                print(id_persona_documento)
                print(valor)
                print(id_param_documento_identificatorio)
                print(id_persona)
                cursor.execute(
                    persona_documento_query,
                    (
                        id_persona_documento,
                        id_persona,
                        id_param_documento_identificatorio,
                        valor,
                    ),
                )
                ########################################################################################################
                # domicilio
                # por ahora no
                ########################################################################################################
                # afiliado
                id_fina = "ec5b3f64-aaed-4024-959d-f77346f11a01"
                id_afiliado_titular = id_afiliado
                codigo = str(getattr(row, "NUMEROTARJETA", ""))
                opt_secret = base64.b32encode(os.urandom(20)).decode("utf-8")
                afiliado_query = """
                INSERT INTO afiliado (id, id_persona, id_afiliado_titular, codigo, id_financiadora, otp_secret)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                vals_afiliado = (
                    id_afiliado,
                    id_persona,
                    id_afiliado_titular,
                    codigo,
                    id_fina,
                    opt_secret,
                )
                # print(vals_afiliado)
                cursor.execute(afiliado_query, vals_afiliado)
                ########################################################################################################
                # afiliado plan
                id_afiliado_plan = str(uuid4())
                id_financiadora_plan = getattr(row, "NOMBRE_PLAN", "")
                id_financiadora_plan_new = getattr(row, "NOMBRE_PLAN_NEW", "")
                print("TRICKI", id_financiadora_plan, id_financiadora_plan_new)
                afiliado_plan_query = """
                INSERT INTO afiliado_plan (id, id_afiliado, id_financiadora_plan)
                VALUES (%s, %s, %s)
                """
                cursor.execute(
                    afiliado_plan_query,
                    (id_afiliado_plan, id_afiliado, id_financiadora_plan_new),
                )
                insert_afiliado_plan_estado = """
                INSERT INTO afiliado_plan_estado (id, id_afiliado_plan, estado, fecha_desde)
                VALUES (%s, %s, %s, %s)
                """
                id_afiliado_plan_estado = str(uuid4())
                buenos_aires_tz = pytz.timezone("America/Argentina/Buenos_Aires")

                cursor.execute(
                    insert_afiliado_plan_estado,
                    (
                        id_afiliado_plan_estado,
                        id_afiliado_plan,
                        "ACTIVO",
                        str(datetime.datetime.now(buenos_aires_tz).date()),
                    ),
                )

            self.connection.commit()
            self.logger.info(f"Inserted {len(missing_df)} missing afiliados.")

        except Exception as e:
            self.connection.rollback()
            self.logger.error("Failed to insert missing afiliados", exc_info=True)

        finally:
            cursor.close()

    def get_updated_value(self, row, new_col, old_col):
        new_value = getattr(row, new_col, "")
        old_value = getattr(row, old_col, "")
        return new_value if new_value != old_value else old_value

    def update_rows(self, df: pd.DataFrame):
        cursor = self.connection.cursor()
        try:
            for row in df.itertuples(index=False):
                print("updating persona")
                persona_data = {
                    col: self.get_updated_value(row, new_col, col)
                    for col, new_col in [
                        ("nombre", "NOMBRE"),
                        ("apellido", "APELLIDO"),
                        ("genero_biologico", "SEXO"),
                        ("fecha_nacimiento", "FECHA_NACIMIENTO"),
                    ]
                }
                if persona_data:
                    set_clause = ", ".join(
                        [f"{key} = %s" for key in persona_data.keys()]
                    )
                    update_persona = f"""
                    UPDATE persona
                    SET {set_clause}
                    WHERE id = %s
                    """
                    values = list(persona_data.values()) + [
                        getattr(row, "id_persona", "")
                    ]
                    cursor.execute(update_persona, values)
                print("updating persona documento")
                persona_documento_data = {
                    col: self.get_updated_value(row, new_col, col)
                    for col, new_col in [
                        ("valor", "NUMERODOCUMENTO"),
                        ("id_param_documento_identificatorio", "TIPO_DOCUMENTO"),
                    ]
                }
                if persona_documento_data:
                    set_clause = ", ".join(
                        [f"{key} = %s" for key in persona_documento_data.keys()]
                    )
                    update_persona_documento = f"""
                        UPDATE
                            persona_documento
                        SET
                            {set_clause}
                        WHERE
                            id_persona = %s
                        """
                    values = list(persona_documento_data.values()) + [
                        getattr(row, "id_persona", "")
                    ]
                    cursor.execute(update_persona_documento, values)

                codigo_titular = self.get_updated_value(
                    row, "TITULAR_TARJETA", "codigo_titular"
                )
                codigo_titular = self.get_updated_value(row, "TITULAR_TARJETA", "codigo_titular")
                id_titular_query = """
                    SELECT id FROM afiliado
                    WHERE codigo = %s
                    AND id_financiadora = 'ec5b3f64-aaed-4024-959d-f77346f11a01'
                """

                cursor.execute(id_titular_query, (codigo_titular,))
                result = cursor.fetchone()

                if result:
                    id_titular = result[0]
                    update_query = """
                        UPDATE afiliado
                        SET id_afiliado_titular = %s
                        WHERE codigo = %s
                    """
                    cursor.execute(update_query, (id_titular, codigo_titular))
                else:
                    print("No record found for the given codigo_titular.")
                # afiliado plan
                id_afiliado_plan = str(uuid4())
                id_financiadora_plan_new = getattr(row, "NOMBRE_PLAN_NEW", "")
                afiliado_plan_query = """
                INSERT INTO afiliado_plan (id, id_afiliado, id_financiadora_plan)
                VALUES (%s, %s, %s)
                """
                cursor.execute(
                    afiliado_plan_query,
                    (id_afiliado_plan, id_titular, id_financiadora_plan_new),
                )
                insert_afiliado_plan_estado = """
                INSERT INTO afiliado_plan_estado (id, id_afiliado_plan, estado, fecha_desde)
                VALUES (%s, %s, %s, %s)
                """
                id_afiliado_plan_estado = str(uuid4())
                buenos_aires_tz = pytz.timezone("America/Argentina/Buenos_Aires")

                cursor.execute(
                    insert_afiliado_plan_estado,
                    (
                        id_afiliado_plan_estado,
                        id_afiliado_plan,
                        "ACTIVO",
                        str(datetime.datetime.now(buenos_aires_tz).date()),
                    ),
                )
            self.connection.commit()
            self.logger.info(f"Updated {len(df)} outdated afiliados.")
        except Exception as e:
            self.connection.rollback()
            self.logger.error("Failed to update afiliados", exc_info=True)

        finally:
            cursor.close()

    def standarize_data(self, df: pd.DataFrame):
        # esta parecido en webhook pero el nombre es distinto
        df = df.copy(deep=True)
        CSS_PLAN_LIST = {
            "PLAN ACTIVOS": "d7db3d91-cd65-47b4-8d56-6ba98fb4e005",  # ta ACTIVOS
            "PLAN PASIVOS": "c59b698b-8f4f-42b6-a36a-a0941f722a4e",  # ta PASIVOS
            "CONVENIO DE RECIPROCIDAD - COBERTURA ACTIVOS": "7af213ac-5c6d-4f17-95a8-c634ad0c1a80",  # ta  CONVENIO DE RECIPROCIDAD - COBERTURA ACTIVOS
            "CONVENIO DE RECIPROCIDAD - ACTO MEDICO": "c7a750dd-7250-403c-9542-5e0b90bb5eb9",  # ta CONVENIO DE RECIPROCIDAD - ACTO MEDICO
            "PENSION A LA VEJEZ DESAMPARADA": "d395b38d-34c7-4538-a531-3df5132ef4de",  # ta PENSIONMAS
            "AFILIADO EN TRANSITO": "76b509c7-f221-4416-a6bb-8c26d44677d2",  # AFILIADO EN TRANSITO
            "VIALIDAD": "1c4e365c-ae6d-492f-96c4-20c8543e8c81",  # CONVENIO VIALIDAD NACIONAL
        }

        CSS_DOCUMENTO_MAP = {
            "DNI": 1,
            "CUIL": 2,
            "CUIT": 3,
            "PASAPORTE": 4,
            "CERTIFICADO DE ESTUDIANTE": 5,
            "CERTIFICADO DE PREEXISTENCIA": 6,
            "LE": 7,  # LIBRETA DE ENROLAMIENTO
            "LC": 8,  # LIBRETA CIVICA
            "CI": 9,  # CEDULA DE IDENTIDAD
            "SIN INFORMAR": 10,
        }
        gender_map = {"M": "MASCULINO", "F": "FEMENINO", "U": "INTERSEXUAL"}
        df[["APELLIDO", "NOMBRE"]] = df["APELLIDO_NOMBRE"].apply(
            lambda x: pd.Series(
                [x.split(" ", 1)[0], x.split(" ", 1)[1] if " " in x else ""]
            )
        )
        df["TIPO_DOCUMENTO"] = df["TIPO_DOCUMENTO"].apply(
            lambda x: CSS_DOCUMENTO_MAP.get(x)
        )
        df["NOMBRE_PLAN_NEW"] = df["NOMBRE_PLAN"].apply(lambda x: CSS_PLAN_LIST.get(x))
        df["SEXO"] = df["SEXO"].apply(lambda x: gender_map.get(x))
        df["FECHA_NACIMIENTO"] = pd.to_datetime(
            df["FECHA_NACIMIENTO"], format="%d-%m-%Y", errors="coerce"
        ).dt.date
        df["NUMEROTARJETA"] = df["NUMEROTARJETA"].astype(str)

        return df

    def compare_rows(self, comparison_df):
        column_comparisons = (
            (comparison_df["NOMBRE"] != comparison_df["nombre"])
            | (comparison_df["APELLIDO"] != comparison_df["apellido"])
            | (
                comparison_df["TIPO_DOCUMENTO"]
                != comparison_df["id_param_documento_identificatorio"]
            )
            | (comparison_df["NUMERODOCUMENTO"] != comparison_df["n_documento"])
            | (comparison_df["FECHA_NACIMIENTO"] != comparison_df["fecha_nacimiento"])
            | (comparison_df["TITULAR_TARJETA"] != comparison_df["codigo_titular"])
            | (comparison_df["NOMBRE_PLAN_NEW"]) != comparison_df["id_afiliado_plan"]
        )

        afis_to_update = comparison_df[column_comparisons]["codigo"]
        afis_to_update = afis_to_update.drop_duplicates().tolist()
        print("Afis to update:", len(afis_to_update))

        return afis_to_update

    def update_grupo_afi(self, df_core: pd.DataFrame, df_bucket: pd.DataFrame):
        titulares_core = df_core.query("id_afiliado_titular == id_afi")[
            ["id_afiliado_titular", "codigo"]
        ]
        titulares_core["codigo"] = titulares_core["codigo"].astype(str)
        groups_core = (
            df_core.groupby("id_afiliado_titular")
            .agg(
                MEMBER_IDS_CORE=("codigo", list),
                COUNT_CORE=("codigo", "count"),
            )
            .reset_index()
        )
        core_result = groups_core.merge(
            titulares_core, on="id_afiliado_titular", how="left"
        )
        #######
        df_bucket["NUMEROTARJETA"] = df_bucket["NUMEROTARJETA"].astype(str)
        titulares = df_bucket.query("ID_TITULAR == ID_AFILIADO")[
            ["ID_TITULAR", "NUMEROTARJETA"]
        ].rename(columns={"NUMEROTARJETA": "TITULAR_TARJETA"})
        grouper = (
            df_bucket.groupby("ID_TITULAR")
            .agg(MEMBER_IDS=("NUMEROTARJETA", list), COUNT=("NUMEROTARJETA", "count"))
            .reset_index()
        )
        result = grouper.merge(titulares, on="ID_TITULAR", how="left")
        comparison = result.merge(
            core_result, left_on="TITULAR_TARJETA", right_on="codigo"
        )

    def add_titular_data(self, data_old: pd.DataFrame, data_new: pd.DataFrame):
        data_new = data_new.copy()
        data_old = data_old.copy()
        print(data_new)
        titulares_old = data_old[["id_afiliado_titular", "codigo", "nombre"]].rename(
            columns={"codigo": "codigo_titular", "nombre": "nombre_titular"}
        )
        data_old = data_old.merge(
            titulares_old, left_on="id_afi", right_on="id_afiliado_titular"
        )

        titulares_new = data_new[
            ["ID_TITULAR", "NUMEROTARJETA", "APELLIDO_NOMBRE"]
        ].rename(
            columns={
                "NUMEROTARJETA": "TITULAR_TARJETA",
                "APELLIDO_NOMBRE": "NOMBRE_TITULAR",
            }
        )
        data_new = data_new.merge(
            titulares_new, left_on="ID_AFILIADO", right_on="ID_TITULAR"
        )
        # data_new = data_new.query("NUMEROTARJETA != TITULAR_TARJETA")
        return data_new, data_old

    def compare_data(self, old_data, new_data):
        # encontrar los afis que faltan
        # estos hay que cargarlos de 0
        missing_afis = new_data[~new_data["NUMEROTARJETA"].isin(old_data["codigo"].astype(int))]
        if len (missing_afis) >0:
            missing_afis_standard = self.standarize_data(df=missing_afis)
            self.insert_missing_afiliados(missing_afis_standard)
        # los que estan, hay que ver si tienen data vieja en algun lado
        existing_afis = new_data[
            new_data["NUMEROTARJETA"].isin(old_data["codigo"].astype(int))
        ]
        existing_afis_standard = self.standarize_data(df=existing_afis.head())
        existing_afis_standard, old_data = self.add_titular_data(
            old_data, existing_afis_standard
        )
        comparison_df = existing_afis_standard.merge(
            old_data,
            left_on="NUMEROTARJETA",
            right_on="codigo",
            how="left",
            suffixes=("_new", "_old"),
        )
        print(comparison_df)
        afis_to_update = self.compare_rows(comparison_df)
        print("updating afifos", afis_to_update)
        update_data =comparison_df[comparison_df["codigo"].isin(afis_to_update)]
        #print(update_data)
        #self.update_rows(update_data)
        # comparison_df.to_csv("comparison.csv")
