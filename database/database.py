"""modulo de conexion a la DB"""

from typing import Dict
from datetime import datetime
import json
import pymysql
import db_config as db


def get_connection() -> pymysql.Connection:
    """get connection object"""

    try:
        return pymysql.connect(user=db.USERNAME, password=db.PASSWORD, host=db.HOST, database=db.DATABASE)
    except Exception as ex:
        raise ex


def query(sql_query: str):
    """Execute a query"""
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(sql_query)
    cursor.close()
    connection.close()


def select_query(sql_query: str) -> str:
    """Execute a SELECT sql query"""
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()

    records = json.dumps(tuple(rows), indent=4, sort_keys=True, default=str, ensure_ascii=False)
    cursor.close()
    connection.close()
    return records


class Database:
    """controller de la db"""

    @staticmethod
    def get_all_info() -> Dict:
        """return default all info"""

        sql_query = """
            SELECT
                cev.CODIGO,
                cev.FECHA_COMERCIAL as FECHA,
                p.PAIS_NOMBRE as PAIS,
                cev.TOTAL_CONFIRMADOS,
                cev.TOTAL_RECUPERADOS,
                cev.TOTAL_MUERTOS
            FROM 
                dw_ds.COVID_EN_VIVO cev
                JOIN dw.PAISES p ON cev.CODIGO = p.REGISTRO_CODIGO
            GROUP BY
                cev.CODIGO,
                cev.FECHA_COMERCIAL,
                p.PAIS_NOMBRE
            LIMIT 100
        """
        return select_query(sql_query)

    @staticmethod
    def get_info_by_date_range(date_from: datetime, date_to: datetime) -> Dict:
        """return db info in range of date"""

        str_from = date_from.strftime("%Y-%m-%d")
        str_to = date_to.strftime("%Y-%m-%d")

        sql_query = f"""
            SELECT
                cev.CODIGO,
                cev.FECHA_COMERCIAL as FECHA,
                p.PAIS_NOMBRE as PAIS,
                cev.TOTAL_CONFIRMADOS,
                cev.TOTAL_RECUPERADOS,
                cev.TOTAL_MUERTOS
            FROM 
                dw_ds.COVID_EN_VIVO cev
                JOIN dw.PAISES p ON cev.CODIGO = p.REGISTRO_CODIGO
            WHERE
                cev.FECHA_COMERCIAL BETWEEN {str_from} AND {str_to}
            GROUP BY
                cev.CODIGO,
                cev.FECHA_COMERCIAL,
                p.PAIS_NOMBRE
        """
        return select_query(sql_query)
