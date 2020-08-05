from typing import Iterable
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.dialects.postgresql import JSONB

import otri.utils.config as cfg
from otri.database.database_adapter import DatabaseAdapter


class AlchemyAdapter(DatabaseAdapter):

    def __init__(self, file: str = None):
        super().__init__()
        config = cfg.get_config(file)
        # User:Password@host:port/database
        conn_str = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            config("user"),
            config("password"),
            config("host"),
            config("port"),
            config("database")
        )
        self._engine = create_engine(conn_str)
        self._Session = sessionmaker(bind=self._engine)
        self._Base = automap_base()
        self._Base.prepare(self._engine, reflect=True)
        self._tables = dict()
        for table in self._Base.classes:
            self._tables[table.__table__.name] = table

    def get_base(self):
        '''
        Retrieve the base.
        '''
        pass

    def get_tables(self):
        '''
        Retrieve the tables.
        '''
        return self._tables

    def add_data(self, items):
        '''
        Add any number of items that are in tables that exist in the db.
        '''
        session = self._Session()
        session.add_all(items)
        session.commit()


if __name__ == "__main__":

    db = AlchemyAdapter("local")
    atom = db.get_tables()['atoms_b'](data_json={"key":"value"})
    print(atom.data_json)
