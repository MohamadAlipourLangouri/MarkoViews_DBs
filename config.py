# PostgreSQL database name, username, and password
DATABASE_URL = "postgresql://postgres:Mohammad66697678@localhost/MarkoViews_Databases"

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

