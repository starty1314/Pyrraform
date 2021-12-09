from sqlalchemy import create_engine, MetaData, Table, insert
from sqlalchemy.orm import sessionmaker


class DBOps:
    """
    This is for DataBase Operation with SQLAlchemy
    """
    def __init__(self, db_instance_type, db_server, db_name, port, ca_path=None, user_name=None, password=None, ssl=True):
        """
        Init database operation class
        :param db_instance_type: database type
        :param db_server: hostname, endpoint or ip of the database server
        :param db_name: database name
        :param port: database connection port
        :param ca_path: CA root certificate path
        :param user_name: database user name
        :param password: database password
        :param ssl: whether enable SSL or not, default is enable
        """
        instance_type = ['ORACLE', 'SQLSERVER', 'MYSQL', 'SQLITE', 'POSTGRESQL']
        if db_instance_type.upper() not in instance_type:
            raise ValueError(f'Invalid DB instance type, Expected one of: {instance_type}')

        if user_name is None and password is None:
            if db_instance_type.upper() == 'SQLITE':
                self.engine = create_engine('sqlite:///:memory:', echo=True)
            else:
                # Create DB connection with Passwordless connection
                self.engine = create_engine(f'mssql+pyodbc://{db_server}/{db_name}?driver=SQL Server?Trusted_Connection=yes')
        else:
            # For SQL authentication
            if db_instance_type.upper() == "ORACLE":
                self.engine = create_engine(f'oracle+cx_oracle://{user_name}:{password}@{db_server}:{port}/?service_name={db_name}')
            elif db_instance_type.upper() == "SQLSERVER":
                self.engine = create_engine(f'mssql+pyodbc://{user_name}:{password}@{db_server}:{port}/{db_name}?driver=SQL Server')
            elif db_instance_type.upper() == "MYSQL":
                self.engine = create_engine(f'mysql+mysqlconnector://{user_name}:{password}@{db_server}/{db_name}')
            elif db_instance_type.upper() == "POSTGRESQL":
                if ssl:
                    ssl_args = {
                        'sslmode': 'verify-full',
                        'sslrootcert': ca_path
                    }
                    self.engine = create_engine(f'postgresql+psycopg2://{user_name}:{password}@{db_server}:{port}/{db_name}',
                                                connect_args=ssl_args)
                else:
                    self.engine = create_engine(f'postgresql+psycopg2://{user_name}:{password}@{db_server}:{port}/{db_name}')

        # Base = declarative_base(bind=self.engine)
        # metadata = Base.metadata
        _session = sessionmaker(bind=self.engine)
        self.session = _session()
        self.conn = self.engine.connect()

    def get_table(self, table_name, schema=None):
        """
        Get table metadata for call reference
        :param table_name: Target's table Name
        :param schema: Optional parameter schema for Oracle especially
        :return: Table MetaData object
        """
        try:
            if schema is not None:
                metadata = MetaData(bind=self.engine, schema=schema)
            else:
                metadata = MetaData(bind=self.engine)

            my_table = Table(table_name, metadata, autoload=True)
        except Exception as e:
            raise e
        else:
            print(f'Get table[{table_name}] metadata object')
            return my_table

    def create_db_table(self, metadata):
        """
        Create table from metadata in database
        :param metadata: SQLAlchemy Table MetaData
        :return: N/A
        """
        try:
            metadata.create_all(self.engine)
        except Exception as e:
            raise e
        else:
            print(f'Table [{list(metadata.tables.keys())[0]}] is created!')
            return True

    def insert_data(self, table_name, data, schema=None):
        """
        Insert data to specified table
        :param table_name: Target's table name
        :param data: and data that needs to insert to DB
        :param schema: Optional parameter schema for Oracle especially
        :return: N/A
        """
        try:
            if schema is not None:
                metadata = MetaData(bind=self.engine, schema=schema)
            else:
                metadata = MetaData(bind=self.engine)

            my_table = Table(table_name, metadata, autoload=True)

            ins = insert(my_table).values(data)
            result = self.conn.execute(ins)
            result.close()
        except Exception as e:
            raise e
        else:
            print('Data inserted!')
            return result

    def insert_bulk_data(self, table_name, data, schema=None):
        """
        Insert multiple lines of data into DB
        :param table_name: Target's table name
        :param data: Data needs to be inserted
        :param schema: Optional parameter schema for Oracle especially
        :return: N/A
        """
        try:
            if schema is not None:
                metadata = MetaData(bind=self.engine, schema=schema)
            else:
                metadata = MetaData(bind=self.engine)

            my_table = Table(table_name, metadata, autoload=True)

            result = self.conn.execute(my_table.insert(), data)
            result.close()
        except Exception as e:
            raise e
        else:
            print(f'{len(data)} lines of Data inserted!')
            return result

    def execute_sql(self, sql_statement):
        """
        Run a sql statement within current connection
        :param sql_statement: A SQL statement to run
        :return: The result from the SQL statement
        """
        return self.conn.execute(sql_statement)

    def insert_dataframe(self, table_model, df_data):
        """
        Insert data in Pandas format
        :param table_model: Table model class
        :param df_data: data in dataframe format
        :return: result from bulk_insert_mappings
        """
        try:
            result = self.session.bulk_insert_mappings(table_model, df_data.to_dict(orient="records"))
        except Exception as e:
            raise e
        else:
            self.session.commit()
            self.session.close()
            return result
