from sqlalchemy import text, Table, Column, Integer, Float, MetaData, engine, String
from app.models import session, engine

# MarkoView V1: Advisor and Student co-authorship
def create_view_v1():
    view_v1_sql = """
        CREATE OR REPLACE VIEW V1 AS
        SELECT 
            a.aid1, 
            a.aid2, 
            CAST(COUNT(w1.pid) AS FLOAT) / 2 AS weight  -- Calculate weight based on co-authored publications during the student period
        FROM 
            Advisorp a
        JOIN 
            Studentp s ON a.aid2 = s.aid  -- Ensure aid2 is the student in the relationship
        JOIN 
            Wrote w1 ON a.aid1 = w1.aid  -- Publications by aid1 (advisor)
        JOIN 
            Wrote w2 ON a.aid2 = w2.aid  -- Publications by aid2 (student)
        JOIN 
            Pub p ON w1.pid = p.pid AND w2.pid = p.pid  -- Ensure they co-authored the same publication
        WHERE 
            p.year = s.year  -- Only consider publications made during the year aid2 was a student
        GROUP BY 
            a.aid1, a.aid2;
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
            0 AS weight  -- Set weight to 0 as per the paper's specification
        FROM 
            advisorp a1
        JOIN 
            advisorp a2 ON a1.aid1 = a2.aid1
        WHERE 
            a1.aid2 <> a2.aid2;  -- Ensure aid2 and aid3 are distinct
        """
        session.execute(text(view_v2_sql))
        session.commit()
        print("V2 created successfully.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred while creating V2: {e}")

#lets break down V3 to ensure correct answer:
# def check_coauthorships():
#     try:
#         coauthorship_sql = """
#         SELECT w1.aid AS aid1, w2.aid AS aid2, COUNT(w1.pid) AS coauthorship_count
#         FROM wrote w1
#         JOIN wrote w2 ON w1.pid = w2.pid
#         JOIN pub p ON w1.pid = p.pid
#         WHERE p.year > 2004 AND w1.aid <> w2.aid
#         GROUP BY w1.aid, w2.aid
#         """
#         result = session.execute(text(coauthorship_sql))
#         print("Co-authorships (aid1, aid2, count):")
#         for row in result:
#             print(f"aid1: {row.aid1}, aid2: {row.aid2}, count: {row.coauthorship_count}")
#     except Exception as e:
#         print(f"An error occurred while checking co-authorships: {e}")
#
# check_coauthorships()
# def check_affiliations():
#     try:
#         affiliation_sql = """
#         SELECT aff1.aid AS aid1, aff2.aid AS aid2, aff1.inst
#         FROM affiliation aff1
#         JOIN affiliation aff2 ON aff1.inst = aff2.inst
#         WHERE aff1.aid <> aff2.aid
#         """
#         result = session.execute(text(affiliation_sql))
#         print("Affiliation pairs (aid1, aid2, institution):")
#         for row in result:
#             print(f"aid1: {row.aid1}, aid2: {row.aid2}, institution: {row.inst}")
#     except Exception as e:
#         print(f"An error occurred while checking affiliations: {e}")
#
# check_affiliations()
# def debug_v3_query():
#     try:
#         debug_sql = """
#         SELECT w1.aid AS aid1, w2.aid AS aid2, aff1.inst, COUNT(w1.pid) / 5.0 AS weight
#         FROM affiliation aff1
#         JOIN wrote w1 ON aff1.aid = w1.aid
#         JOIN wrote w2 ON w1.pid = w2.pid
#         JOIN pub p ON w1.pid = p.pid
#         JOIN affiliation aff2 ON w2.aid = aff2.aid
#         WHERE p.year > 2004 AND aff1.inst = aff2.inst AND w1.aid <> w2.aid
#         GROUP BY w1.aid, w2.aid, aff1.inst
#         HAVING COUNT(w1.pid) > 2
#         """
#         result = session.execute(text(debug_sql))
#         print("Debug V3 results:")
#         for row in result:
#             print(f"aid1: {row.aid1}, aid2: {row.aid2}, institution: {row.inst}, weight: {row.weight}")
#     except Exception as e:
#         print(f"An error occurred while debugging V3 query: {e}")
#
# debug_v3_query()


# MarkoView V3: If two people have published a lot together recently, then their affiliations are very likely to be same
def create_view_v3():
    view_v3_sql = """
                        CREATE OR REPLACE VIEW V3 AS
        SELECT 
            a1.aid AS aid1,
            a2.aid AS aid2,
            a1.inst AS inst,
            COUNT(DISTINCT p.pid) / 5 AS publication_score
        FROM 
            Affiliation a1
        JOIN 
            Affiliation a2 ON a1.inst = a2.inst
        JOIN 
            Wrote w1 ON a1.aid = w1.aid
        JOIN 
            Wrote w2 ON a2.aid = w2.aid AND w1.pid = w2.pid
        JOIN 
            Pub p ON p.pid = w1.pid
        WHERE 
            p.year > 2004
            AND a1.aid <> a2.aid  -- Exclude self-pairing
        GROUP BY 
            a1.aid, a2.aid, a1.inst
        HAVING 
            COUNT(DISTINCT p.pid) > 2; -- Filter only those pairs with more than 2 co-publications

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
    try:
        # Execute the query to check for violations in V2
        result = session.execute(text("SELECT * FROM V2")).fetchall()

        # Check if there are any results
        if not result:
            print("MarkoView V2 (Advisor Violation Rule): No violations found.")
            return

        # If violations are found, print them
        print("MarkoView V2 (Advisor Violation Rule):")
        for row in result:
            print(f"Advisor {row.aid1} advises both {row.aid2} and {row.aid3}, violating the rule!")
    except Exception as e:
        print(f"An error occurred while enforcing the V2 constraint: {e}")




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
        print(f"Advisor {row.aid1} advised {row.aid2} with weight {row.weight}. ")

    # If no results were found, print an appropriate message
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
        # Check if the weight is 0 (indicating a constraint)
        if row.weight == 0:
            transformed_weight = 0  # Directly set w_0 to 0 for constraint tuples
        else:
            # Apply the transformation logic for non-constraint tuples
            transformed_weight = (1 - row.weight) / row.weight

        session.execute(
            text("UPDATE nv2 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2 AND aid3 = :aid3"),
            {"aid1": row.aid1, "aid2": row.aid2, "aid3": row.aid3, "w_0": transformed_weight}
        )

    # Transform NV3 tuples
    nv3_tuples = session.execute(text("SELECT aid1, aid2, inst, weight FROM nv3"))
    for row in nv3_tuples:
        if row.weight == 0:
            # Handle the edge case where weight is zero (constraint tuple)
            transformed_weight = 0  # or handle in a specific way as per your requirements
        else:
            # Apply the transformation logic
            transformed_weight = (1 - row.weight) / row.weight

        session.execute(
            text("UPDATE nv3 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2 AND inst = :inst"),
            {"aid1": row.aid1, "aid2": row.aid2, "inst": row.inst, "w_0": transformed_weight}
        )

    session.commit()

    session.commit()
    print("Transformation to INDB complete.")

# ---------------- Computing the P(Q) -------------------

def compute_P0_Q_or_W(session, query):
    prob_product = 1
    query_sql = f"""
        SELECT w_0 FROM nv1 WHERE {query}
        UNION ALL
        SELECT w_0 FROM nv2 WHERE {query}
        UNION ALL
        SELECT w_0 FROM nv3 WHERE {query}
    """
    result = session.execute(text(query_sql)).fetchall()

    # Calculate the product of (1 - w_0) for each tuple's probability
    for row in result:
        weight = row[0]
        print(f"Processing tuple with w_0 = {weight}, current product = {prob_product}")
        prob_product *= (1 - weight)
        print(f"Updated product = {prob_product}")

    total_probability = 1 - prob_product
    print(f"Final computed probability (P0_Q_or_W) = {total_probability}")
    return total_probability



def compute_P0_W(session):
    # Initialize probability product for independent events
    prob_product = 1
    epsilon = 1e-10  # Small value to handle edge cases

    # Direct probabilities of tuples from NV tables
    query_sql = """
        SELECT w_0 FROM nv1
        UNION ALL
        SELECT w_0 FROM nv2
        UNION ALL
        SELECT w_0 FROM nv3
    """
    result = session.execute(text(query_sql)).fetchall()

    # Compute the product of (1 - w_0) for each tuple's probability
    for row in result:
        weight = row[0]
        # Include negative weights as they are
        prob_product *= (1 - weight)

    # Compute the final probability using the complement
    total_probability = 1 - prob_product
    return total_probability


def compute_PQ(session, query):
    # Compute P0(Q or W)
    P0_Q_or_W = compute_P0_Q_or_W(session, query)

    # Compute P0(W) - the union of all NV tables
    P0_W = compute_P0_W(session)

    # Apply the formula to calculate the final probability P(Q)
    if P0_W >= 1:  # Avoid division by zero and handle edge cases
        return 1.0
    else:
        PQ = (P0_Q_or_W - P0_W) / (1 - P0_W)
        return PQ


# -------------------------- Revised Contribution ----------

def m_compute_PQ(session, query):
    # Compute P0(Q or W)
    m_P0_Q_or_W = m_compute_P0_Q_or_W(session, query)

    # Compute P0(W) - the union of all NV tables
    m_P0_W = m_compute_P0_W(session)

    # Avoid division by zero, ensure m_P0_W is not exactly 1
    if m_P0_W >= 1:
        m_P0_W = 0.999999  # Small adjustment to avoid division by zero

    # Compute P(Q) using the formula
    m_PQ = (m_P0_Q_or_W - m_P0_W) / (1 - m_P0_W)
    return m_PQ

def m_compute_P0_Q_or_W(session, query):
    # Initialize probability product for independent events
    prob_product = 1
    epsilon = 1e-10  # Small value to handle edge cases

    # Direct probabilities of tuples from NV tables
    query_sql = f"""
        SELECT w_0 FROM nv1 WHERE {query}
        UNION ALL
        SELECT w_0 FROM nv2 WHERE {query}
        UNION ALL
        SELECT w_0 FROM nv3 WHERE {query}
    """
    result = session.execute(text(query_sql)).fetchall()

    # Check for any tuple with w_0 = 1 (certainty)
    for row in result:
        weight = row[0]
        if weight == 1:
            return 1  # If any tuple is certain, return probability 1 immediately
        # Otherwise, multiply (1 - weight)
        prob_product *= (1 - min(max(weight, epsilon), 1 - epsilon))

    # Compute the final probability using the complement
    total_probability = 1 - prob_product
    return total_probability

def m_compute_P0_W(session):
    # Initialize probability product for independent events
    prob_product = 1
    epsilon = 1e-10  # Small value to handle edge cases

    # Direct probabilities of tuples from NV tables
    query_sql = """
        SELECT w_0 FROM nv1
        UNION ALL
        SELECT w_0 FROM nv2
        UNION ALL
        SELECT w_0 FROM nv3
    """
    result = session.execute(text(query_sql)).fetchall()

    # Check for any tuple with w_0 = 1 (certainty)
    for row in result:
        weight = row[0]
        if weight == 1:
            return 1  # If any tuple is certain, return probability 1 immediately
        # Otherwise, multiply (1 - weight)
        prob_product *= (1 - min(max(weight, epsilon), 1 - epsilon))

    # Compute the final probability using the complement
    total_probability = 1 - prob_product
    return total_probability