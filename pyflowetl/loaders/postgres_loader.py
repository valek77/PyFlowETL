import pandas as pd
from sqlalchemy import create_engine, text
from pyflowetl.log import get_logger, log_memory_usage

class PostgresLoader:
    def __init__(self, connection_string: str, table_name: str, mode: str = "insert",
                 key_columns: list[str] = None, chunksize: int = 500):
        """
        :param connection_string: es. 'postgresql://user:password@localhost:5432/dbname'
        :param table_name: nome della tabella target
        :param mode: 'insert', 'update', 'upsert'
        :param key_columns: colonne chiave per update/upsert
        :param chunksize: righe per batch
        """
        self.engine = create_engine(connection_string)
        self.table_name = table_name
        self.mode = mode.lower()
        self.key_columns = key_columns or []
        self.chunksize = chunksize
        self.logger = get_logger()

    def load(self, df: pd.DataFrame):
        self.logger.info(f"[PostgresLoader] Modalità: {self.mode}")
        if self.mode == "insert":
            self._insert(df)
        elif self.mode == "update":
            self._update(df)
        elif self.mode == "upsert":
            self._upsert(df)
        else:
            raise ValueError(f"[PostgresLoader] Modalità non supportata: {self.mode}")

        log_memory_usage("[PostgresLoader] post-load")

    def _insert(self, df: pd.DataFrame):
        df.to_sql(self.table_name, con=self.engine, if_exists="append", index=False, chunksize=self.chunksize)
        self.logger.info(f"[PostgresLoader] Inserite {len(df)} righe")

    def _update(self, df: pd.DataFrame):
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                set_clause = ", ".join([f"{col} = :{col}" for col in df.columns if col not in self.key_columns])
                where_clause = " AND ".join([f"{col} = :{col}" for col in self.key_columns])
                sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause}"
                conn.execute(text(sql), row.to_dict())
        self.logger.info(f"[PostgresLoader] Aggiornate {len(df)} righe")

    def _upsert(self, df: pd.DataFrame):
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                columns = list(row.keys())
                values = [f":{col}" for col in columns]
                updates = [f"{col} = EXCLUDED.{col}" for col in columns if col not in self.key_columns]
                conflict_cols = ", ".join(self.key_columns)
                sql = f"""
                    INSERT INTO {self.table_name} ({', '.join(columns)})
                    VALUES ({', '.join(values)})
                    ON CONFLICT ({conflict_cols}) DO UPDATE SET {', '.join(updates)}
                """
                conn.execute(text(sql), row.to_dict())
        self.logger.info(f"[PostgresLoader] Upsert su {len(df)} righe")
