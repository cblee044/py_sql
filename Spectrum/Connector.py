import pandas as pd
import numpy as np
import sys
from sqlalchemy import create_engine, exc, types
from sqlalchemy.engine import URL

class Connection():
    def __init__(self, config):
        self.config = config
        connection_string = f"""
            DRIVER={{{config.sql_driver}}};
            SERVER={config.sql_server};
            DATABASE={config.sql_database};
            Trusted_Connection={config.sql_trust}
        """
        connection_url = URL.create(
            "mssql+pyodbc", 
            query={"odbc_connect": connection_string}
        )

        print("Connecting to SQL Server")
        self.eng = create_engine(
            connection_url, 
            fast_executemany=True
        )
    
    def connect_hcclnc(self):
        print("Connecting to Clarity NCAL")
        self.ora_eng = create_engine(
            f"oracle+cx_oracle://{self.config.ora_hcclnc_username}:{self.config.ora_hcclnc_password}@{self.config.ora_hcclnc_host}:{self.config.ora_hcclnc_port}/?service_name={self.config.ora_hcclnc_servicename}&encoding=UTF-8&nencoding=UTF-8"
        )
    
    def upload_ora(self, df, table_name):
        dtyp = {c:types.VARCHAR(df[c].str.len().max()) \
            for c in df.columns[(df.dtypes == 'object') | (df.dtypes == 'string')].tolist()}        
        with self.ora_eng.begin() as con:
            print(f'uploading dataframe to Oracle SQL under table name {table_name}')
            df.to_sql(table_name, if_exists='replace', dtype=dtyp, con=con)