from sqlalchemy import create_engine, text

# Your database URI
db_uri = 'postgresql://postgres:ncar@localhost:5432/dropsonde_db'

# Create an engine
engine = create_engine(db_uri)

# Test the connection
with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))
    print(result.fetchone())  # Should return (1,)
