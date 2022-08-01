from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeMeta
from sqlmodel import SQLModel, Field, Session, select
from typing import *
import re
from nltk.stem.porter import PorterStemmer
from autocorrect import Speller
from sqlalchemy.sql import func


class SearchEngine:
    def __init__(self, db_engine: Engine, case_sensitive=False, correct_spelling=True, plural_sensitive=False,
                 filler_words=None):
        self.db_engine: Engine = db_engine
        self.registered_models: Dict[Union[DeclarativeMeta, SQLModel], Dict] = {}
        # {col: {'total_tokens': int, 'tokens_table': Anonymous, 'frequency_table': Anonymous} for col in columns}
        self.case_sensitive = case_sensitive
        self.correct_spelling = correct_spelling
        self.plural_sensitive = plural_sensitive
        self.filler_words = {'ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once',
                              'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do',
                              'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am',
                              'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until', 'below', 'are',
                              'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more',
                              'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to',
                              'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and',
                              'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because',
                              'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself',
                              'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few',
                              'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how',
                              'further', 'was', 'here', 'than'} if filler_words is None else filler_words
        self.autocorrect = Speller()
        self.stemmer = PorterStemmer()
        self.cur_session: Session = None

    def __create_jsearch_tables(self, model, column):
        class TokenTable(SQLModel, table=True):
            __tablename__ = f"{model.__tablename__}_{column.name}_jsearch_tokens"
            token: str = Field(primary_key=True, index=True)
            parent: int = Field(primary_key=True, foreign_key=f'{model.__tablename__}.id')
            frequency: int

        class FrequencyTable(SQLModel, table=True):
            __tablename__ = f"{model.__tablename__}_{column.name}_jsearch_counter"
            token: str = Field(primary_key=True, index=True)
            frequency: int

        SQLModel.metadata.create_all(self.db_engine)
        return TokenTable, FrequencyTable

    def register_model(self, model: Union[DeclarativeMeta, SQLModel], columns: List[Field]):
        assert model not in self.registered_models
        self.registered_models[model] = {c: {} for c in columns}
        for col in columns:
            tokens_table, frequency_table = self.__create_jsearch_tables(model, col)
            self.registered_models[model][col]['tokens_table'] = tokens_table
            self.registered_models[model][col]['frequency_table'] = frequency_table
            with Session(self.db_engine) as session:
                sql = select(func.sum(frequency_table.frequency))
                self.registered_models[model][col]['total_tokens'] = [session.exec(sql).first() or 0][0]

    def add(self, record: Union[DeclarativeMeta, SQLModel]):
        assert self.cur_session is not None
        assert record.__class__ in self.registered_models
        self.cur_session.add(record)
        for key, val in record:
            if key in {col.name for col in self.registered_models[record.__class__]}:
                column = [c for c in self.registered_models[record.__class__] if c.name == key][0]
                self.__register_jsearch_record(column, val)

    def __register_jsearch_record(self, column, text):
        tokens = self.__tokenize(text)

        print(tokens)

    def add_all(self, data: List[Union[DeclarativeMeta, SQLModel]]):
        assert self.cur_session is not None
        for record in data:
            self.add(record)

    def commit(self):
        assert self.cur_session is not None
        self.cur_session.commit()

    def search(self, model: Union[DeclarativeMeta, SQLModel], query: str, limit: int = 12, offset: int = 0)\
            -> List[Union[DeclarativeMeta, SQLModel]]:
        tokens = self.__tokenize(query.strip())

    def __tokenize(self, text: str) -> List[str]:
        """returns {"original": original, "corrected": corrected}"""
        if not self.case_sensitive:
            text = text.lower()
        tokens = [(w if self.plural_sensitive else self.stemmer.stem(w))
                  for w in re.split(r"\W", text.strip()) if (w and w not in self.filler_words)]
        if self.correct_spelling:
            return [self.autocorrect(w) for w in tokens]
        return tokens

    def __enter__(self) -> Session:
        self.cur_session = Session(self.db_engine)
        return self.cur_session

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.cur_session.close()
        self.cur_session = None
