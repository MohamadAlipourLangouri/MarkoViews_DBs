from sqlalchemy import text, Table, Column, Integer, Float, MetaData, engine, String
from app.models import session, engine

# MarkoView V1: Advisor and Student co-authorship
def create_view_v1():
    view_v1_sql = """
        CREATE OR REPLACE VIEW V1 AS
        SELECT 
            a.aid1, 
            a.aid2, 
            CAST(COUNT(w1.pid) AS FLOAT) / 2 AS weight,
            CASE
                WHEN s2.year = p.year AND s1.aid IS NULL THEN 'Advisor-Advisee'
                ELSE 'Co-authors'
            END AS relationship
        FROM 
            advisorp a
        JOIN 
            wrote w1 ON a.aid1 = w1.aid
        JOIN 
            wrote w2 ON a.aid2 = w2.aid
        JOIN 
            pub p ON w1.pid = p.pid  -- Join with Pub to get the publication year
        LEFT JOIN 
            studentp s2 ON a.aid2 = s2.aid AND s2.year = p.year  -- Check if aid2 was a student in the publication year
        LEFT JOIN 
            studentp s1 ON a.aid1 = s1.aid AND s1.year = p.year  -- Check if aid1 was a student in the publication year
        WHERE 
            w1.pid = w2.pid
        GROUP BY 
            a.aid1, a.aid2, s2.year, p.year, s1.aid;
    """
    try:
        session.execute(text(view_v1_sql))
        session.commit()
        print("V1 created successfully.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred while creating V1: {e}")



# MarkoView V2: Constraint on advisors advising two students who advise each other
def create_view_v2():
    try:
        # Drop the view if it exists
        session.execute(text("DROP VIEW IF EXISTS V2"))

        # Create the view
        view_v2_sql = """
        CREATE VIEW V2 AS
        SELECT 
            a1.aid1, 
            a1.aid2, 
            a2.aid2 AS aid3,
            COUNT(w1.pid) / 5.0 AS weight  -- Example weight calculation
        FROM 
            advisorp a1
        JOIN 
            advisorp a2 ON a1.aid1 = a2.aid1
        JOIN 
            wrote w1 ON a1.aid2 = w1.aid
        JOIN 
            wrote w2 ON a2.aid2 = w2.aid
        WHERE 
            a1.aid2 <> a2.aid2
        GROUP BY 
            a1.aid1, a1.aid2, a2.aid2;
        """
        session.execute(text(view_v2_sql))
        session.commit()
        print("V2 created successfully.")
    except Exception as e:
        print(f"An error occurred while creating V2: {e}")


# MarkoView V3: If two people have published a lot together recently, then their affiliations are very likely to be same
def create_view_v3():
    view_v3_sql = """
    CREATE OR REPLACE VIEW V3 AS
    SELECT w1.aid AS aid1, w2.aid AS aid2, aff1.inst, COUNT(w1.pid) / 5.0 AS weight
    FROM affiliation aff1
    JOIN wrote w1 ON aff1.aid = w1.aid
    JOIN wrote w2 ON aff1.aid = w2.aid
    JOIN pub p ON w1.pid = w2.pid AND w1.pid = p.pid
    JOIN affiliation aff2 ON w2.aid = aff2.aid
    WHERE p.year > 2004
    GROUP BY w1.aid, w2.aid, aff1.inst
    HAVING COUNT(w1.pid) > 30;
    """
    try:
        session.execute(text(view_v3_sql))
        session.commit()
        print("V3 created successfully.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred while creating V3: {e}")


# Function to check for rule violation in MarkoView V2
def enforce_constraint_v2():
    result = session.execute(text("SELECT * FROM V2"))
    print("MarkoView V2 (Advisor Violation Rule):")
    for row in result:
        print(f"Advisor {row.aid1} advises both {row.aid2} and {row.aid3}, violating the rule!")



# # function to calculate weights for the views
# def calculate_weights_for_v1():
#     result = session.execute(text("SELECT * FROM V1"))
#     for row in result:
#         print(f"Advisor {row.aid1} advised {row.aid2} with weight {row.weight}")



# Query and display the results of MarkoView V1
def show_view_v1():
    print("Querying MarkoView V1...")
    result = session.execute(text("SELECT * FROM V1"))

    # Initialize a flag to track if there are results
    has_results = False

    print("MarkoView V1 (Advisor and Student Co-Authorship):")
    for row in result:
        has_results = True
        print(f"Advisor {row.aid1} advised {row.aid2} with weight {row.weight}. Relationship: {row.relationship}")

    # If no results were found, print a message
    if not has_results:
        print("No results found in V1.")


# Query and display the results of MarkoView V2
def show_view_v2():
    result = session.execute(text("SELECT * FROM V2"))
    print("MarkoView V2 (Advisor Violation Rule):")
    for row in result:
        print(f"Advisor {row.aid1} advises both {row.aid2} and {row.aid3}, violating the rule!")



def show_view_v3():
    try:
        # Query the V3 view
        view_v3_sql = "SELECT aid1, aid2, inst, weight FROM V3"
        result = session.execute(text(view_v3_sql))

        # Check if there are any results
        if result.rowcount == 0:
            print("No results found in V3.")
        else:
            print("MarkoView V3 (Co-authorship and Affiliation):")
            for row in result:
                print(f"Author 1 ID: {row.aid1}, Author 2 ID: {row.aid2}, Institution: {row.inst}, Weight: {row.weight}")

    except Exception as e:
        print(f"An error occurred while querying V3: {e}")


# Step 1: Define NV tables for each view
def create_nv_tables():
    metadata = MetaData()

    # Define NV1 table (based on V1)
    nv1 = Table(
        'nv1', metadata,
        Column('aid1', Integer, primary_key=True),
        Column('aid2', Integer, primary_key=True),
        Column('weight', Float),
        Column('w_0', Float)
    )

    # Define NV2 table (based on V2)
    nv2 = Table(
        'nv2', metadata,
        Column('aid1', Integer, primary_key=True),
        Column('aid2', Integer, primary_key=True),
        Column('aid3', Integer, primary_key=True),
        Column('weight', Float),
        Column('w_0', Float)
    )

    # Define NV3 table (based on V3)
    nv3 = Table(
        'nv3', metadata,
        Column('aid1', Integer, primary_key=True),
        Column('aid2', Integer, primary_key=True),
        Column('inst', String),  # Corrected to use String instead of string
        Column('weight', Float),
        Column('w_0', Float)
    )

    # Create all tables in the database
    metadata.create_all(engine)

# Step 2: Populate NV tables based on the views
def populate_nv_tables():
    # Populate NV1 based on V1
    v1_tuples = session.execute(text("SELECT aid1, aid2, weight FROM V1"))
    for row in v1_tuples:
        # Check if the tuple already exists in nv1
        existing = session.execute(
            text("SELECT aid1, aid2 FROM nv1 WHERE aid1 = :aid1 AND aid2 = :aid2"),
            {"aid1": row.aid1, "aid2": row.aid2}
        ).fetchone()

        if existing is None:
            session.execute(
                text("INSERT INTO nv1 (aid1, aid2, weight) VALUES (:aid1, :aid2, :weight)"),
                {"aid1": row.aid1, "aid2": row.aid2, "weight": row.weight}
            )
        else:
            print(f"Skipping duplicate entry for aid1={row.aid1}, aid2={row.aid2} in nv1")

    # Populate NV2 based on V2
    v2_tuples = session.execute(text("SELECT aid1, aid2, aid3, weight FROM V2"))
    for row in v2_tuples:
        # Check if the tuple already exists in nv2
        existing = session.execute(
            text("SELECT aid1, aid2, aid3 FROM nv2 WHERE aid1 = :aid1 AND aid2 = :aid2 AND aid3 = :aid3"),
            {"aid1": row.aid1, "aid2": row.aid2, "aid3": row.aid3}
        ).fetchone()

        if existing is None:
            session.execute(
                text("INSERT INTO nv2 (aid1, aid2, aid3, weight) VALUES (:aid1, :aid2, :aid3, :weight)"),
                {"aid1": row.aid1, "aid2": row.aid2, "aid3": row.aid3, "weight": row.weight}
            )
        else:
            print(f"Skipping duplicate entry for aid1={row.aid1}, aid2={row.aid2}, aid3={row.aid3} in nv2")

    # Populate NV3 based on V3
    v3_tuples = session.execute(text("SELECT aid1, aid2, inst, weight FROM V3"))
    for row in v3_tuples:
        # Check if the tuple already exists in nv3
        existing = session.execute(
            text("SELECT aid1, aid2, inst FROM nv3 WHERE aid1 = :aid1 AND aid2 = :aid2 AND inst = :inst"),
            {"aid1": row.aid1, "aid2": row.aid2, "inst": row.inst}
        ).fetchone()

        if existing is None:
            session.execute(
                text("INSERT INTO nv3 (aid1, aid2, inst, weight) VALUES (:aid1, :aid2, :inst, :weight)"),
                {"aid1": row.aid1, "aid2": row.aid2, "inst": row.inst, "weight": row.weight}
            )
        else:
            print(f"Skipping duplicate entry for aid1={row.aid1}, aid2={row.aid2}, inst={row.inst} in nv3")

    session.commit()
    print("NV tables populated successfully.")

# Run this to create NV tables and populate them
create_nv_tables()
populate_nv_tables()


def transform_mvdb_to_indb():
    # Transform NV1 tuples
    nv1_tuples = session.execute(text("SELECT aid1, aid2, weight FROM nv1"))
    for row in nv1_tuples:
        # Apply the transformation logic
        transformed_weight = (1 - row.weight) / row.weight
        session.execute(
            text("UPDATE nv1 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2"),
            {"aid1": row.aid1, "aid2": row.aid2, "w_0": transformed_weight}
        )

    # Transform NV2 tuples
    nv2_tuples = session.execute(text("SELECT aid1, aid2, aid3, weight FROM nv2"))
    for row in nv2_tuples:
        # Apply the transformation logic
        transformed_weight = (1 - row.weight) / row.weight
        session.execute(
            text("UPDATE nv2 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2 AND aid3 = :aid3"),
            {"aid1": row.aid1, "aid2": row.aid2, "aid3": row.aid3, "w_0": transformed_weight}
        )

    # Transform NV3 tuples
    nv3_tuples = session.execute(text("SELECT aid1, aid2, inst, weight FROM nv3"))
    for row in nv3_tuples:
        # Apply the transformation logic
        transformed_weight = (1 - row.weight) / row.weight
        session.execute(
            text("UPDATE nv3 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2 AND inst = :inst"),
            {"aid1": row.aid1, "aid2": row.aid2, "inst": row.inst, "w_0": transformed_weight}
        )

    session.commit()
    print("Transformation to INDB complete.")

