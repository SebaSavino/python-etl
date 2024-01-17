import os
import datetime
from io import BytesIO

import paramiko
import click as cli
import pandas as pd
from decouple import config as get_env

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

sftp_user = get_env("SFTP_USER")
sftp_pass = get_env("SFTP_PASS")
sftp_host = get_env("SFTP_HOST")
sftp_port = get_env("SFTP_PORT", default=22, cast=int)


def extract(sftp_remote_path: str) -> BytesIO:
    try:
        # Establecer conexión SFTP
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_pass)

        # Creamos el cliente SFTP
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Descargar el archivo CSV desde el servidor SFTP
        file_stream = BytesIO()
        sftp.getfo(sftp_remote_path, file_stream)

        # Ajustar el puntero para que esté al principio del archivo
        file_stream.seek(0)

        # Cerrar las conexiones
        sftp.close()
        transport.close()

        return file_stream
    except FileNotFoundError:
        cli.echo(f"No se encontró {sftp_remote_path}")
        # Send email or slack message
        exit(1)
    except Exception as error:
        cli.echo(error)
        # Send email or slack message
        exit(1)


def transform(file_stream: BytesIO) -> pd.DataFrame:
    df = pd.read_csv(file_stream)

    # Utiliza los valores de la última ( o la primera ) columna como cabeceras
    headers = df.iloc[:, -1].tolist()

    # Elimina la última columna
    df = df.iloc[:, :-1]

    # Transponer el DataFrame después de asignar las cabeceras para que las columnas sean filas
    df_transposed = df.T

    # Asigna las cabeceras al DataFrame transpuesto
    df_transposed.columns = headers

    # Resetear los índices
    df_transposed_reset = df_transposed.reset_index(drop=True)

    # Convertir las cantidades en números para poder hacer operaciones como sumar
    df_transposed_reset["amount"] = df_transposed_reset["amount"].astype(int)

    # Agrupar por 'customerId' y calcular promedio, suma y media de 'amount'
    df_orders_by_customers = df_transposed_reset.groupby("customerId").agg(
        {"amount": ["mean", "sum", "median"]}
    )

    # Renombrar las columnas resultantes
    df_orders_by_customers.columns = ["avgAmount", "totalAmount", "medianAmount"]

    # Sumar el valor de date
    df_orders_by_customers["date"] = df_transposed_reset["createdAt"].iloc[0]

    return df_orders_by_customers


def load(data: pd.DataFrame) -> None:
    # Tambien se podria obtener estas credenciales desde AWS Secret Manager o similares
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "creds.json"

    # Especifica el ID del proyecto y el conjunto de datos
    project = get_env("GCP_PROJECT")
    dataset = get_env("BQ_DATASET")
    table = get_env("BQ_TABLE")

    # Crea una instancia del cliente de BigQuery
    client = bigquery.Client(project=project)

    # Verifica si el conjunto de datos existe, si no, lo creamos
    try:
        client.get_dataset(dataset)
    except NotFound:
        client.create_dataset(dataset)

    # Cargamos los datos en BigQuery
    client.load_table_from_dataframe(data, f"{dataset}.{table}").result()
    cli.echo("Datos cargados en BigQuery")


@cli.command()
@cli.option(
    "-d",
    "--date",
    help="Fecha en formato yyyy-MM-dd",
    default=str(datetime.date.today()),
)
def Xepelin_ETL(date: str):
    try:
        # Validamos el formato de la fecha
        datetime.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        cli.echo("Formato de fecha incorrecto. Utiliza el formato aaaa-mm-dd")
        exit(1)

    file = extract(f"orders_{date}.csv")
    data = transform(file)
    load(data)


if __name__ == "__main__":
    Xepelin_ETL()
