from models import Blog, db_engine
from JSearch import SearchEngine
from datetime import datetime

# Step 1: creates the search engine based on SQLModel
engine = SearchEngine(db_engine)
engine.register_model(Blog, columns=[Blog.title, Blog.body], importance=[2, 1])

# Step 2: insert some data
with engine:
    engine.add(Blog(title="Hello JSearch", body="awesome full text search for SQLModel", timestamp=datetime.now()))
    # Or, you can bulk insert using engine.add_all([Blog(), Blog(), Blog()])

    # Saves the insert both to the Blog table you created
    # and adds it to the index tables managed by JSearch
    engine.commit()

# Let's try searching!
query = "j serches"  # smartly deals with spelling error, plurals, upper-lower case, etc.
results = engine.search(query, model=Blog, limit=12, offset=0)  # returns a list of Blog objects
print(results)
