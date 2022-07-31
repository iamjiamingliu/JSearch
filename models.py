from sqlmodel import SQLModel, Field, create_engine
from typing import *
from datetime import datetime


class Blog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    title: str
    body: str
    timestamp: datetime


db_engine = create_engine("sqlite:///blog.db", echo=True)

if __name__ == '__main__':
    SQLModel.metadata.create_all(db_engine)
