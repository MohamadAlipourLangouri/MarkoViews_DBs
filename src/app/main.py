

from sqlalchemy import text, Table, Column, Integer, Float, MetaData
from app.models import create_tables, session, Author, Wrote, Studentp, Advisorp, Affiliation, Pub
from app.views import create_view_v1, create_view_v2, enforce_constraint_v2, show_view_v1, show_view_v3, \
    transform_mvdb_to_indb, create_nv_tables, populate_nv_tables, compute_PQ, m_compute_PQ, compute_P0_Q_or_W, \
    compute_P0_W, create_view_v3

import math


# Create all tables
create_tables()

# Insert data
def populate_data():
    # Clear existing data from the tables (order matters to avoid foreign key constraint violations)
    session.execute(text("DELETE FROM wrote"))
    session.execute(text("DELETE FROM affiliation"))
    session.execute(text("DELETE FROM pub"))
    session.execute(text("DELETE FROM advisorp"))
    session.execute(text("DELETE FROM studentp"))
    session.execute(text("DELETE FROM author"))
    session.commit()

    try:
        # Insert the new data
        session.add_all([
            # Authors
            Author(aid=1, name="Alice"),
            Author(aid=2, name="Bob"),
            Author(aid=3, name="Charlie"),
            Author(aid=4, name="David"),

            # Publications and Wrote
            Wrote(aid=1, pid=101),
            Wrote(aid=2, pid=101),
            Wrote(aid=1, pid=102),
            Wrote(aid=2, pid=102),
            Wrote(aid=3, pid=103),
            Wrote(aid=4, pid=103),
            Wrote(aid=3, pid=104),
            Wrote(aid=4, pid=104),
            Wrote(aid=1, pid=105),
            Wrote(aid=3, pid=105),
            Wrote(aid=1, pid=106),
            Wrote(aid=2, pid=106),

            # Students
            Studentp(aid=1, year=2005, probability=0.8),
            Studentp(aid=2, year=2006, probability=0.7),

            # Advisors
            Advisorp(aid1=1, aid2=2, probability=0.9),
            Advisorp(aid1=1, aid2=3, probability=0.8),
            Advisorp(aid1=2, aid2=4, probability=0.7),
            Advisorp(aid1=3, aid2=4, probability=0.75),

            # Affiliations
            Affiliation(aid=1, inst='University A'),
            Affiliation(aid=2, inst='University A'),
            Affiliation(aid=3, inst='University B'),
            Affiliation(aid=4, inst='University B'),

            # Publications
            Pub(pid=101, year=2006),
            Pub(pid=102, year=2006),
            Pub(pid=103, year=2007),
            Pub(pid=104, year=2008),
            Pub(pid=105, year=2022),
            Pub(pid=106, year=2006)
        ])
        session.commit()
        print("Sample data added successfully.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred while adding sample data: {e}")

populate_data()
# Run This to create NV tables and populate them

#---------------
# generating results
create_view_v1()
create_view_v2()
create_view_v3()

create_nv_tables()
populate_nv_tables()
# Show MarkoView outputs
show_view_v1()
enforce_constraint_v2()
show_view_v3()

# Transform the MVDB into the INDB
transform_mvdb_to_indb()
#Compute the arbitrary Query condition
query_condition = "aid1 = 1 AND aid2 = 2"  # Example condition
probability = compute_PQ(session, query_condition)
probability_Q_OR_W = compute_P0_Q_or_W(session,query_condition)
Probability_P0_W = compute_P0_W(session)
print(f"The probability of the query is: {probability}")
print(f"the Prob of P0_Q_or_W is{probability_Q_OR_W}")
print(f"the Prob of P0_W is: {Probability_P0_W}")


probability2 = m_compute_PQ(session, query_condition)
print(f"The probability of the query for m_PQ is: {probability2}")