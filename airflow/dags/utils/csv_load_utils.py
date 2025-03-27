from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
from sqlalchemy import Table, MetaData
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine, Connection
    from .config import DataSet


class CSVLoaderConfig:
    """
    Configuration class for handling CSV file loading parameters.
    """
    FILE_STORAGE = '/opt/airflow/csvs'

    def __init__(self, dataset: 'DataSet', engine: 'Engine'):
        self.dataset = dataset
        self.table_name = f'raw_{dataset.value}'
        self.__init_files()
        self.table_columns = self.__get_columns(engine)
        self.temp_table_name = f"tmp_tbl_{dataset.value}"

    def get_file_fullpath(self, filename: str) -> str:
        """Returns the full path of the file based on the dataset mode."""
        return f'{self.FILE_STORAGE}/{self.dataset.value}/{filename}' if self.is_incremental else f'{self.FILE_STORAGE}/{filename}'

    def __init_files(self):
        """Initializes file paths and determines if the dataset is incremental."""
        self.files = []
        dir_path = Path(f'{self.FILE_STORAGE}/{self.dataset.value}')
        file_path = Path(f'{self.FILE_STORAGE}/{self.dataset.value}.csv')

        if dir_path.is_dir():
            self.is_incremental = True
            self.files = [file.name for file in dir_path.glob('./[!.]*.csv')]
        elif file_path.is_file():
            self.is_incremental = False
            self.files.append(file_path.name)

        if not self.files:
            raise AirflowSkipException('No new files to proceed')

    def __get_columns(self, engine: 'Engine'):
        """Retrieves column names from the target database table."""
        table = Table(self.table_name, MetaData(schema='bronze'), autoload_with=engine)
        return [col.name for col in table.columns if col.name != 'file_id']


class CSVLoader:
    """
    Handles the process of uploading CSV data into a PostgreSQL database.
    """
    def __init__(self, dataset: 'DataSet'):
        hook = PostgresHook(postgres_conn_id='postgres_default')
        self.engine: 'Engine' = hook.get_sqlalchemy_engine()
        self.config = CSVLoaderConfig(dataset, self.engine)

    def create_log_record(self, conn: 'Connection', filename: str) -> int:
        """Creates a log record for the uploaded file and returns the file ID."""
        return conn.execute(
            """
            INSERT INTO bronze.file_load_log (dataset, file_name)
            VALUES (%s, %s)
            RETURNING id
            """, [self.config.dataset.value, filename]
        ).scalar_one()

    def create_temp_table(self, conn: 'Connection'):
        """Creates a temporary table for processing the CSV data."""
        conn.execute(f"""
            CREATE TEMP TABLE {self.config.temp_table_name} ON COMMIT DROP AS (
                SELECT {','.join(self.config.table_columns)}
                FROM bronze.{self.config.table_name}
                WHERE FALSE
            )
        """)

    def upload_csv_to_temp_table(self, conn: 'Connection', filename: str):
        """Uploads CSV data into the temporary table."""
        with open(self.config.get_file_fullpath(filename), 'r') as csv_file_stream:
            cur = conn.connection.cursor()
            copy_stmnt = f"""
                COPY {self.config.temp_table_name}({','.join(self.config.table_columns)})
                FROM STDIN WITH (FORMAT csv, HEADER TRUE)
            """
            cur.copy_expert(copy_stmnt, csv_file_stream)

    def truncate_table(self, conn: 'Connection'):
        """Truncates the destination table before loading new data."""
        conn.execute(f"TRUNCATE TABLE bronze.{self.config.table_name} CASCADE")

    def copy_from_temp_table(self, conn: 'Connection', file_id: int):
        """Copies data from the temporary table to the destination table."""
        conn.execute(f"""
            INSERT INTO bronze.{self.config.table_name}
            SELECT {','.join(self.config.table_columns)}, {file_id} AS file_id
            FROM {self.config.temp_table_name}
        """)

    def rename_file(self, filename: str, file_id: int):
        """Renames the processed CSV file to mark it as processed."""
        Path(self.config.get_file_fullpath(filename)).rename(self.config.get_file_fullpath(f'.p{file_id}.{filename}'))

    def upload(self) -> str:
        """Executes the full upload process for all available CSV files."""
        for file in self.config.files:
            with self.engine.connect() as conn:
                with conn.begin():
                    file_id = self.create_log_record(conn, file)
                    self.create_temp_table(conn)
                    if not self.config.is_incremental:
                        self.truncate_table(conn)
                    self.upload_csv_to_temp_table(conn, file)
                    self.copy_from_temp_table(conn, file_id)
            self.rename_file(file, file_id)
        return self.config.table_name


def upload(dataset: 'DataSet') -> str:
    """Uploads CSV data for a given dataset and returns the updated table name."""
    print(f'Upload {dataset} started...')
    loader = CSVLoader(dataset)
    updated_table = loader.upload()
    print('Upload finished')
    return updated_table
