import pandas_gbq as pgbp
from os_environ import osEnviron
from settings import config
from google.cloud import bigquery

osEnviron.set_os_environ(config.KEY_PATH)

PROJECT_ID = 'pe-fesa-datalake-dev01'
DATA_SET = config.DATA_SET
ORIGIN_TABLE = 'BD_Prime'
DESTINATION_TABLE = 'BD_Prime_test'

ORIGIN_TABLE_ID = f'{PROJECT_ID}.{DATA_SET}.{ORIGIN_TABLE}'
DESTINATION_TABLE_ID = f'{PROJECT_ID}.{DATA_SET}.{DESTINATION_TABLE}'

client = bigquery.Client(project = PROJECT_ID)


# sql = f"""
# UPDATE `{DESTINATION_TABLE_ID}` a
# SET Organizacion = 'Ferreyros1'
# WHERE EXISTS 
# (
#     SELECT DATE_TRUNC(DATE(b.Mes_encuesta), MONTH) FROM 
#     `{ORIGIN_TABLE_ID}` b 
#     WHERE
#     DATE_TRUNC(DATE(a.Mes_encuesta), MONTH) = DATE_TRUNC(DATE(b.Mes_encuesta), MONTH)
# )
# """

sql = f"""
DELETE FROM `{DESTINATION_TABLE_ID}` a
WHERE EXISTS 
(
    SELECT DATE_TRUNC(DATE(b.Mes_encuesta), MONTH) FROM 
    `{ORIGIN_TABLE_ID}` b 
    WHERE
    DATE_TRUNC(DATE(a.Mes_encuesta), MONTH) = DATE_TRUNC(DATE(b.Mes_encuesta), MONTH)
)
"""

query = client.query(sql)
query.result()

print(f'TABLA {DESTINATION_TABLE} actualizada')




# sql2 = f"""
# SELECT Identificador_unico, Organizacion, Mes_encuesta
# FROM {DESTINATION_TABLE_ID}
# """

# sql2 = f"""
# SELECT Identificador_unico, Organizacion, Mes_encuesta
# FROM {ORIGIN_TABLE_ID}
# """

# sql2 = f"""
# SELECT a.Mes_encuesta, a.Identificador_unico
#     FROM `{DESTINATION_TABLE_ID}` a
#     WHERE EXISTS 
#     (
#         SELECT DATE_TRUNC(DATE(b.Mes_encuesta), MONTH) FROM 
#         `{ORIGIN_TABLE_ID}` b 
#         WHERE
#         DATE_TRUNC(DATE(a.Mes_encuesta), MONTH) = DATE_TRUNC(DATE(b.Mes_encuesta), MONTH)
#     )   
# """

# df = pgbp.read_gbq(sql2, project_id=PROJECT_ID)

# print(df)

