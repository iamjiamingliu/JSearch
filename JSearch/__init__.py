from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeMeta
from sqlmodel import SQLModel, Field, Session
from typing import *


class SearchEngine:
    def __init__(self, db_engine: Engine):
        self.db_engine: Engine = db_engine
        self.models: Set[Union[DeclarativeMeta, SQLModel]] = set()
        self.cur_session: Session = None

    def register_model(self, model: Union[DeclarativeMeta, SQLModel], columns: List[Field], importance=None):
        """If not provided with an importance, then every column will be treated with the same weight."""
        if importance is None:
            importance = [1] * len(columns)
        self.models.add(model)
        print(model.__tablename__)
        print(self.models)

    def add(self, data: Union[DeclarativeMeta, SQLModel]):
        assert self.cur_session is not None
        print(data.__class__)

    def add_all(self, data: List[Union[DeclarativeMeta, SQLModel]]):
        assert self.cur_session is not None
        ...

    def commit(self):
        assert self.cur_session is not None
        self.cur_session.commit()

    def search(self, query: str, model: Union[DeclarativeMeta, SQLModel], limit: int = 12, offset: int = 0) \
            -> List[Union[DeclarativeMeta, SQLModel]]:
        ...

    def __enter__(self) -> Session:
        self.cur_session = Session(self.db_engine)
        return self.cur_session

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.cur_session.close()
        self.cur_session = None
