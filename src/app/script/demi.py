import datetime
import math
import sys
import os
import pytz
from uuid import uuid4
import logging
import functools
import base64
import os
import psycopg2
import pandas as pd
from ftplib import FTP
import numpy as np
from unidecode import unidecode

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


class ScriptDemi:
    def __init__(
        self,
        connection: psycopg2.extensions.connection,
        verbose: bool = False,
        ftp: bool = False,
    ):
        self.connection = connection
        self.verbose = verbose
        self.ftp = ftp
        self.logger = logging.getLogger(__name__)

    @disable_print_if_verbose_decorator
    def load_new_data(self) -> pd.DataFrame:
        ftp = self.ftp
        if ftp is True:
            try:

                ftp = FTP(settings.BASE_FTP)
                ftp.login(user=settings.FTP_USER, passwd=settings.FTP_PASSW)
                print("✅ FTP login successful!")

                ftp.cwd("CredencialDigital")
                data = pd.read_csv("DEMISALUD-Afiliados.txt", encoding="latin-1", sep="|")
                print(data.head())
                ftp.quit()
                print("FTP connection closed.")
            except Exception as e:
                print(f"❌ Error with FTP: {e}")
                logging.error(f"Error with FTP: {e}")
        else:
            print("Loading data from local file...")
            logging.info("Loading data from local file...")
            data = pd.read_csv("DEMISALUD-Afiliados.txt", encoding="latin-1", sep="|")
            print("✅ Local Data loaded successfully!")
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
        financiadora_plan.nombre AS nombre_plan,
        financiadora_plan.id AS id_financiadora_plan

        FROM afiliado
        LEFT JOIN persona ON persona.id = afiliado.id_persona
        LEFT JOIN persona_documento ON persona_documento.id_persona = persona.id
        LEFT JOIN param_documento_identificatorio ON param_documento_identificatorio.id = persona_documento.id_param_documento_identificatorio
        LEFT JOIN persona_contacto ON persona_contacto.id_persona = persona.id
        LEFT JOIN contacto ON contacto.id = persona_contacto.id_contacto
        LEFT JOIN persona_domicilio ON persona_domicilio.id_persona = persona.id
		LEFT JOIN domicilio ON domicilio.id = persona_domicilio.id_domicilio
        LEFT JOIN afiliado_parentezco_tipo ON afiliado_parentezco_tipo.id = afiliado.id_afiliado_parentezco_tipo
        LEFT JOIN afiliado_plan ON afiliado_plan.id_afiliado = afiliado.id
        LEFT JOIN financiadora_plan ON financiadora_plan.id = afiliado_plan.id_financiadora_plan
        WHERE afiliado.id_financiadora = '69633cef-cd44-4ce2-ae8c-3000b61c6849'
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
                id_domicilio = str(uuid4())
                codigo_postal = getattr(row, "CODIGO_POSTAL", "")
                calle = getattr(row, "CALLE", "")
                numeracion = getattr(row, "NUMERO")
                piso = getattr(row, "PISO")
                departamento = getattr(row, "DEPARTAMENTO")
                desc = "NO"
                id_loc_localidad = getattr(row, "id_loc_localidad")
                domicilio_query = """
                INSERT INTO domicilio (id, codigo_postal, calle, numeracion, piso, departamento, descripcion, id_loc_localidad)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                vals_domicilio = (
                    id_domicilio,
                    codigo_postal,
                    calle,
                    numeracion,
                    piso,
                    departamento,
                    desc,
                    id_loc_localidad
                )
                cursor.execute(domicilio_query, vals_domicilio)
                # persona domicilio
                persona_domicilio_query = """
                INSERT INTO persona_domicilio (id_persona, id_domicilio, es_principal)
                VALUES (%s, %s, %s)
                """
                values_persona_domicilio = (
                    id_persona,
                    id_domicilio,
                    True
                )
                cursor.execute(persona_domicilio_query, values_persona_domicilio)
                ########################################################################################################
                # afiliado
                id_fina = "69633cef-cd44-4ce2-ae8c-3000b61c6849"
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
                cursor.execute(afiliado_query, vals_afiliado)
                ########################################################################################################
                #persona contacto
                contacto_query = """
                INSERT INTO contacto (id, valor, tipo)
                VALUES (%s, %s, %s)
                """
                persona_contacto_query = """
                INSERT INTO persona_contacto (id_persona, id_contacto)
                VALUES (%s, %s)
                """

                if getattr(row, "TELEFONO", "") != "NULL":
                    id_contacto_telefono = str(uuid4())

                    values_contacto_telefono = (
                        id_contacto_telefono,
                        getattr(row, "TELEFONO", ""),
                        "{LLAMADAS}"
                    )

                    cursor.execute(contacto_query, values_contacto_telefono)

                    values_persona_contacto_telefono = (
                        id_persona,
                        id_contacto_telefono
                    )

                    cursor.execute(persona_contacto_query, values_persona_contacto_telefono)

                #if getattr(row, "EMAIL", "") != "NULL":
                #    id_contacto_email = str(uuid4())
#
                #    values_contacto_email = (
                #        id_contacto_email,
                #        getattr(row, "EMAIL", ""),
                #        "{EMAIL}"
                #    )
#
                #    cursor.execute(contacto_query, values_contacto_email)
#
                #    values_persona_contacto_email = (
                #        id_persona,
                #        id_contacto_email
                #    )
#
                #    cursor.execute(persona_contacto_query, values_persona_contacto_email)
                ########################################################################################################
                # afiliado plan
                id_afiliado_plan = str(uuid4())
                id_financiadora_plan = getattr(row, "NOMBRE_PLAN", "")
                id_financiadora_plan_new = getattr(row, "NOMBRE_PLAN_NEW", "")
                afiliado_plan_query = """
                INSERT INTO afiliado_plan (id, id_afiliado, id_financiadora_plan)
                VALUES (%s, %s, %s)
                """
                print(id_financiadora_plan_new)
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


    def update_rows(self, df: pd.DataFrame):
        cursor = self.connection.cursor()
        try:
            for row in df.itertuples(index=True):
                # Check persona data
                #print(row)
                print("updating persona")
                #row = dict(row)
                persona_data = {
                    "nombre": row.NOMBRE if row.NOMBRE != row.nombre and row.nombre is not None and (row.NOMBRE is not None and len(row.NOMBRE) > 0) else row.nombre,
                    "apellido": row.APELLIDO if row.APELLIDO != row.apellido and row.apellido is not None and (row.APELLIDO is not None and len(row.APELLIDO) > 0) else row.apellido,
                    "genero_biologico": row.SEXO if row.SEXO != row.genero_biologico and (row.SEXO is not None and len(row.SEXO) > 0) else row.genero_biologico,
                    "fecha_nacimiento": row.FECHA_NACIMIENTO if row.FECHA_NACIMIENTO != row.fecha_nacimiento and row.FECHA_NACIMIENTO is not None else row.fecha_nacimiento
                }

                #if not all(self.is_valid(value) for value in persona_data.values()):
                #    print(f"Skipping update for persona due to invalid data: {persona_data}")
                #    continue

                set_clause = ", ".join([f"{key} = %s" for key in persona_data.keys()])
                update_persona_query = f"""
                    UPDATE persona
                    SET {set_clause}
                    WHERE id = %s
                """
                values = list(persona_data.values()) + [getattr(row, "id_persona", "")]
                cursor.execute(update_persona_query, values)

                print("updating persona documento")
                persona_documento_data = {
                    "valor": row.NUMERODOCUMENTO if row.NUMERODOCUMENTO != row.n_documento and row.NUMERODOCUMENTO is not None else row.n_documento,
                    "id_param_documento_identificatorio": row.TIPO_DOCUMENTO if row.TIPO_DOCUMENTO != row.id_param_documento_identificatorio and row.id_param_documento_identificatorio is not None else row.id_param_documento_identificatorio,

                }

                #if not all(self.is_valid(value) for value in persona_documento_data.values()):
                #    print(f"Skipping update for persona_documento due to invalid data: {persona_documento_data}")
                #    continue  # Skip this row if any field is invalid

                set_clause = ", ".join([f"{key} = %s" for key in persona_documento_data.keys()])
                update_persona_documento_query = f"""
                    UPDATE persona_documento
                    SET {set_clause}
                    WHERE id_persona = %s
                """
                values = list(persona_documento_data.values()) + [getattr(row, "id_persona", "")]
                cursor.execute(update_persona_documento_query, values)

                print("updating afiliado")
                print(row)
                codigo_titular = row.TITULAR_TARJETA if row.TITULAR_TARJETA != row.codigo_titular and row.TITULAR_TARJETA is not None else row.codigo_titular

                if isinstance(codigo_titular, (int, float)) and (math.isnan(codigo_titular) or codigo_titular == ""):
                    codigo_titular = row.codigo_titular
                    #print(f"Skipping update for afiliado: invalid codigo_titular {codigo_titular}")
                    #continue

                #if not self.is_valid(codigo_titular):
                #    print(f"Skipping update for afiliado: invalid codigo_titular {codigo_titular}")
                #    continue

                id_titular_query = """
                    SELECT id FROM afiliado
                    WHERE codigo = %s
                    AND id_financiadora = 'ec5b3f64-aaed-4024-959d-f77346f11a01'
                """
                cursor.execute(id_titular_query, (codigo_titular,))
                result = cursor.fetchone()

                if result:
                    id_titular = result[0]
                    update_afiliado_query = """
                        UPDATE afiliado
                        SET id_afiliado_titular = %s
                        WHERE codigo = %s
                    """
                    cursor.execute(update_afiliado_query, (id_titular, codigo_titular))
                else:
                    print("No record found for the given codigo_titular.")

                print("inserting into afiliado_plan")
                if row.NOMBRE_PLAN_NEW != row.id_financiadora_plan:
                    print("previous plan is deprecated, creating new status for old plan")
                    old_plan_status_id = str(uuid4())
                    buenos_aires_tz = pytz.timezone("America/Argentina/Buenos_Aires")
                    insert_afiliado_plan_estado_query = """
                    INSERT INTO afiliado_plan_estado (id, id_afiliado_plan, estado, fecha_desde)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(
                        insert_afiliado_plan_estado_query,
                        (
                            old_plan_status_id,
                            row.id_afiliado_plan,
                            "INACTIVO",
                            str(datetime.datetime.now(buenos_aires_tz).date())
                        )
                    )
                    print("Inserting new plan and status entry")
                    new_plan_id = str(uuid4())
                    insert_afiliado_plan_query = """
                    INSERT INTO afiliado_plan (id, id_afiliado, id_financiadora_plan)
                    VALUES (%s, %s, %s)
                    """
                    cursor.execute(
                        insert_afiliado_plan_query,
                        (
                            new_plan_id,
                            row.id_afi,
                            row.NOMBRE_PLAN_NEW
                        )
                    )
                    new_plan_status_id = str(uuid4())
                    insert_afiliado_plan_estado_query = """
                    INSERT INTO afiliado_plan_estado (id, id_afiliado_plan, estado, fecha_desde)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(
                        insert_afiliado_plan_estado_query,
                        (
                            new_plan_status_id,
                            new_plan_id,
                            "ACTIVO",
                            str(datetime.datetime.now(buenos_aires_tz).date())
                        )
                    )
            self.connection.commit()

        except Exception as e:
            self.connection.rollback()
            self.logger.error("Failed to update afiliados", exc_info=True)
        finally:
            cursor.close()



    def standarize_data(self, df: pd.DataFrame):
        DEMI_PLAN_LIST = {
            "AZUL PLUS-VOL-ROS": "b60f55eb-c083-416e-a7fa-70657ba4ab81",
            "AZUL PLUS-OBL-ROS": "b60f55eb-c083-416e-a7fa-70657ba4ab81",
            "AZUL PLUS- OBLIG-SM": "b60f55eb-c083-416e-a7fa-70657ba4ab81",
            "AZUL-COSEGURO A CARGO SOCIO 20,00%":"96b8c983-7724-414e-88e5-3427d7f43b0a",
            "DEMI OP - OBLIG- SM": "a9064b7f-d422-4eac-9eec-e8946f7990aa",
            "DEMI OP - OBLIG- ROS": "a9064b7f-d422-4eac-9eec-e8946f7990aa",
            "DEMI OP - VOL- SM": "a9064b7f-d422-4eac-9eec-e8946f7990aa",
            "DEMI-COSEGURO A CARGO SOCIO 30,00%": "76b509c7-f221-4416-a6bb-8c26d44677d2",
            "VITALICIO": "5f322351-b6a9-4976-902a-a05f75779944",
            "VERDE - OBLIGATORIO": "7aec8bd7-22cf-42e0-84a9-2d0e6637a388",
            "PLAN BASICO" : "5f322351-b6a9-4976-902a-a05f75779944",
            "DS 1000": "a1896f07-e202-4c89-be5e-24de5b174014"

        }

        DEMI_DOCUMENTO_MAP = {
            "DNI": 1,
            "LE": 7,
            "LC": 8,
        }

        gender_map = {"M": "MASCULINO", "F": "FEMENINO", "U": "INTERSEXUAL"}
        df[["APELLIDO", "NOMBRE"]] = df["APELLIDO_NOMBRE"].str.split(" ", n=1, expand=True)
        df["TIPO_DOCUMENTO"] = df["TIPO_DOCUMENTO"].map(DEMI_DOCUMENTO_MAP)
        df["NOMBRE_PLAN_NEW"] = df["NOMBRE_PLAN"].map(DEMI_PLAN_LIST)
        df["SEXO"] = df["SEXO"].map(gender_map)
        df["FECHA_NACIMIENTO"] = pd.to_datetime(
            df["FECHA_NACIMIENTO"], format="%d-%m-%Y", errors="coerce"
        )
        df["NUMEROTARJETA"] = df["NUMEROTARJETA"].astype(int).astype(str)
        df["ID_TITULAR"] = df.apply(
                lambda row: row["ID_AFILIADO"] if pd.isna(row["ID_TITULAR"]) else row["ID_TITULAR"],
                axis=1
            )
        df["id_loc_estado"] = df["PROVINCIA"].apply(
            lambda x: unidecode(x).lower()
        )

        state_names = list(df["id_loc_estado"].unique())

        state_names = [unidecode(x.lower()) for x in state_names]

        state_mapping = {
            "cordoba": "Córdoba",
            "caba": "Ciudad Autónoma de Buenos Aires",
            "entre rios": "Entre Ríos",
            "santa fe": "Santa Fe",
        }

        state_names = [state_mapping.get(x, x) for x in state_names]

        state_ids = {}

        cursor = self.connection.cursor()
        for state_name in state_names:
            cursor.execute(
                "SELECT id FROM loc_estado WHERE LOWER(nombre) LIKE %s", (state_name.lower(),)
            )
            result = cursor.fetchone()
            if result:
                state_ids[state_name] = result[0]
            else:
                state_ids[state_name] = None
        cursor.close()

        df["id_loc_estado"] = (
            df["id_loc_estado"].map(state_mapping).map(state_ids)
        )
        city_replacements = {
            "CAP.": "CAPITAN ",
            "SJ.": "SAN JOSE ",
            "GOB.": "GOBERNADOR ",
            "GRAL.": "GENERAL",
            "San Jose de la Esquina": "SAN JOSE DE LA ESQUINA",
            "CORONEL DOMINGUEZ": "CORONEL RODOLFO S. DOMINGUEZ",
            "PUERTO SAN MARTIN": "PUERTO GENERAL SAN MARTIN",
            "NUEVA CORDOBA": "CORDOBA",
            "CNEL OLMEDO": "CORDOBA",
            "Cordoba": "CORDOBA",
            "CÓRDOBA": "CORDOBA",
            "CABA": "CIUDAD DE BUENOS AIRES",
        }

        df["id_loc_localidad"] = df["LOCALIDAD"].replace(
            city_replacements, regex=True
        )
        unique_cities = set(
            zip(df["id_loc_localidad"], df["id_loc_estado"])
        )

        city_ids = {}
        cursor = self.connection.cursor()
        for city_name, state_name in unique_cities:
            cursor.execute(
                f"SELECT id FROM loc_localidad WHERE LOWER(nombre) LIKE '{city_name.lower()}' AND id_loc_estado = '{state_name}' AND id_financiadora IS NULL"
            )
            result = cursor.fetchone()
            if result:
                city_ids[city_name] = result[0]
            else:
                city_ids[city_name] = None
        cursor.close()
        df["id_loc_localidad"] = df["id_loc_localidad"].map(city_ids)
        df.drop(columns=["id_loc_estado"], inplace=True)
        df.fillna("", inplace=True) # Reemplazados NaN con String vacío para Front CD Flutter
        return df

    def compare_rows(self, comparison_df):
        column_comparisons = (
            (comparison_df["NOMBRE"] != comparison_df["nombre"])
            | (comparison_df["APELLIDO"] != comparison_df["apellido"])
            | (
                comparison_df["TIPO_DOCUMENTO"]
                != comparison_df["id_param_documento_identificatorio"]
            )
            | (comparison_df["NUMERODOCUMENTO"].astype(str) != comparison_df["n_documento"])
            | (comparison_df["FECHA_NACIMIENTO"] != comparison_df["fecha_nacimiento"])
            | (comparison_df["TITULAR_TARJETA"] != comparison_df["codigo_titular"])
            | (comparison_df["NOMBRE_PLAN_NEW"]) != comparison_df["id_financiadora_plan"]
        )

        afis_to_update = comparison_df[column_comparisons]["codigo"].astype(str)
        afis_to_update = afis_to_update.drop_duplicates().tolist()
        print("Afis to update:", len(afis_to_update))

        return afis_to_update

    def add_titular_data(self, data_old: pd.DataFrame, data_new: pd.DataFrame):
        print("flaco")
        codigo_map = dict(zip(data_old["id_afi"], data_old["codigo"]))
        nombre_map = dict(zip(data_old["id_afi"], data_old["nombre"]))

        data_old["codigo_titular"] = data_old["id_afiliado_titular"].map(codigo_map)
        data_old["nombre_titular"] = data_old["id_afiliado_titular"].map(nombre_map)

        codigo_map_new = dict(zip(data_new["ID_AFILIADO"], data_new["NUMEROTARJETA"]))
        nombre_map_new = dict(zip(data_new["ID_AFILIADO"], data_new["APELLIDO_NOMBRE"]))

        data_new["TITULAR_TARJETA"] = data_new["ID_TITULAR"].map(codigo_map_new)
        data_new["NOMBRE_TITULAR"] = data_new["ID_TITULAR"].map(nombre_map_new)

        return data_new, data_old

    def compare_data(self, old_data, new_data):
        # encontrar los afis que faltan
        # estos hay que cargarlos de 0
        missing_afis = new_data[~new_data["NUMEROTARJETA"].astype(str).isin(old_data["codigo"])] # Se convierte a string para que coincida con el tipo de datos de old_data

        if len (missing_afis) >0:
            print("missing afis, starting loading")
            missing_afis_standard = self.standarize_data(df=missing_afis)
            self.insert_missing_afiliados(missing_afis_standard)
            print("loading complete")
        # los que estan, hay que ver si tienen data vieja en algun lado
        existing_afis = new_data[
            new_data["NUMEROTARJETA"].isin(old_data["codigo"].astype(int))
        ]
        existing_afis_standard = self.standarize_data(df=existing_afis)
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

        afis_to_update = self.compare_rows(comparison_df)
        comparison_df["codigo"] = comparison_df["codigo"].astype(str)
        update_data =comparison_df[comparison_df["codigo"].isin(afis_to_update)]
        self.update_rows(update_data)
