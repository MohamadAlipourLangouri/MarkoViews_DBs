from sqlalchemy import text, Table, Column, Integer, Float, MetaData, engine, String
from config import session, engine


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

# ----------------------------------- Views created --------------------

# Defining NV classes to strore the results of the MarkoViews transformation
def create_nv_tables():
    metadata = MetaData()

    # Define NV1 table (based on V1)
    nv1 = Table(
        'nv1', metadata,
        Column('aid1', Integer, primary_key=True),
        Column('aid2', Integer, primary_key=True),
        Column('weight', Float),
        Column('w_0', Float)  # Transformed weight
    )

    # Define NV2 table (based on V2)
    nv2 = Table(
        'nv2', metadata,
        Column('aid1', Integer, primary_key=True),
        Column('aid2', Integer, primary_key=True),
        Column('aid3', Integer, primary_key=True),
        Column('weight', Float),
        Column('w_0', Float)  # Transformed weight
    )

    # Define NV3 table (based on V3)
    nv3 = Table(
        'nv3', metadata,
        Column('aid1', Integer, primary_key=True),
        Column('aid2', Integer, primary_key=True),
        Column('inst', String),
        Column('weight', Float),
        Column('w_0', Float)  # Transformed weight
    )

    # Create all tables in the database
    metadata.create_all(engine)

# --------------------- populate the NV tables
def populate_nv_tables():
    # Populate NV1 based on V1
    v1_tuples = session.execute(text("SELECT aid1, aid2, weight FROM V1"))
    for row in v1_tuples:
        # Transform the weight using the function refined earlier
        transformed_weight = transform_weight(row.weight, is_markoview_tuple=True)
        if transformed_weight is not None:
            # Check if the tuple already exists in nv1
            existing = session.execute(
                text("SELECT aid1, aid2 FROM nv1 WHERE aid1 = :aid1 AND aid2 = :aid2"),
                {"aid1": row.aid1, "aid2": row.aid2}
            ).fetchone()

            if existing is None:
                session.execute(
                    text("INSERT INTO nv1 (aid1, aid2, weight, w_0) VALUES (:aid1, :aid2, :weight, :w_0)"),
                    {"aid1": row.aid1, "aid2": row.aid2, "weight": row.weight, "w_0": transformed_weight}
                )
            else:
                print(f"Skipping duplicate entry for aid1={row.aid1}, aid2={row.aid2} in nv1")

    # Repeat similar steps for NV2 and NV3 tables
    # NV2
    v2_tuples = session.execute(text("SELECT aid1, aid2, aid3, weight FROM V2"))
    for row in v2_tuples:
        transformed_weight = transform_weight(row.weight, is_markoview_tuple=True)
        if transformed_weight is not None:
            existing = session.execute(
                text("SELECT aid1, aid2, aid3 FROM nv2 WHERE aid1 = :aid1 AND aid2 = :aid2 AND aid3 = :aid3"),
                {"aid1": row.aid1, "aid2": row.aid2, "aid3": row.aid3}
            ).fetchone()

            if existing is None:
                session.execute(
                    text("INSERT INTO nv2 (aid1, aid2, aid3, weight, w_0) VALUES (:aid1, :aid2, :aid3, :weight, :w_0)"),
                    {"aid1": row.aid1, "aid2": row.aid2, "aid3": row.aid3, "weight": row.weight, "w_0": transformed_weight}
                )
            else:
                print(f"Skipping duplicate entry for aid1={row.aid1}, aid2={row.aid2}, aid3={row.aid3} in nv2")

    # NV3
    v3_tuples = session.execute(text("SELECT aid1, aid2, inst, weight FROM V3"))
    for row in v3_tuples:
        transformed_weight = transform_weight(row.weight, is_markoview_tuple=True)
        if transformed_weight is not None:
            existing = session.execute(
                text("SELECT aid1, aid2, inst FROM nv3 WHERE aid1 = :aid1 AND aid2 = :aid2 AND inst = :inst"),
                {"aid1": row.aid1, "aid2": row.aid2, "inst": row.inst}
            ).fetchone()

            if existing is None:
                session.execute(
                    text("INSERT INTO nv3 (aid1, aid2, inst, weight, w_0) VALUES (:aid1, :aid2, :inst, :weight, :w_0)"),
                    {"aid1": row.aid1, "aid2": row.aid2, "inst": row.inst, "weight": row.weight, "w_0": transformed_weight}
                )
            else:
                print(f"Skipping duplicate entry for aid1={row.aid1}, aid2={row.aid2}, inst={row.inst} in nv3")

    session.commit()
    print("NV tables populated successfully.")


#-----------------------------------------


def compute_probability(expression, aid1=None, aid2=None, aid3=None):
    total_prob = 0.0

    if expression == "union_nv_tables":
        # Union of all tuples across nv1, nv2, and nv3 (represents W)
        nv1_prob = session.execute(text("SELECT SUM(w_0) FROM nv1")).scalar() or 0
        nv2_prob = session.execute(text("SELECT SUM(w_0) FROM nv2")).scalar() or 0
        nv3_prob = session.execute(text("SELECT SUM(w_0) FROM nv3")).scalar() or 0

        total_prob = nv1_prob + nv2_prob + nv3_prob - (nv1_prob * nv2_prob) - (nv1_prob * nv3_prob) - (nv2_prob * nv3_prob) + (nv1_prob * nv2_prob * nv3_prob)

    elif expression == "v1_coauthorship":
        # Example: Probability of a specific advisor-student co-authorship in V1
        total_prob = session.execute(
            text("SELECT SUM(w_0) FROM nv1 WHERE aid1 = :aid1 AND aid2 = :aid2"),
            {'aid1': aid1, 'aid2': aid2}
        ).scalar() or 0

    elif expression == "v2_advisor_violation":
        # Example: Probability of an advisor violation in V2
        total_prob = session.execute(
            text("SELECT SUM(w_0) FROM nv2 WHERE aid1 = :aid1 AND aid2 = :aid2 AND aid3 = :aid3"),
            {'aid1': aid1, 'aid2': aid2, 'aid3': aid3}
        ).scalar() or 0

    elif expression == "v3_shared_affiliation":
        # Example: Probability of shared affiliation for frequent co-authors in V3
        total_prob = session.execute(
            text("SELECT SUM(w_0) FROM nv3 WHERE aid1 = :aid1 AND aid2 = :aid2"),
            {'aid1': aid1, 'aid2': aid2}
        ).scalar() or 0

    return min(total_prob, 1)  # Ensure probability does not exceed 1

#------------------------------------


def transform_weight(weight, is_markoview_tuple=True):
    """
    Transform the weight based on whether the tuple is in the MarkoView (Tup_V) or the original database (Tup).

    :param weight: The original weight of the tuple.
    :param is_markoview_tuple: Boolean indicating if the tuple belongs to a MarkoView (Tup_V).
    :return: The transformed weight w_0 or None if invalid input.
    """
    try:
        if is_markoview_tuple:
            # Handle edge cases based on the paper's discussion:
            if weight == 0:
                # Represents a hard constraint (impossible event)
                return float('inf')  # or a large value, depending on how you want to represent it in the DB
            elif weight == 1:
                # Represents a certainty, hence transformed weight should be infinite (or a very large number)
                return float('inf')

            # Normal transformation for 0 < weight < 1
            transformed_weight = (1 - weight) / weight
            return transformed_weight
        else:
            # If it's a tuple from the original database (Tup), use the original weight
            return weight

    except Exception as e:
        print(f"Error in transforming weight: {e}")
        return None
    transformed_weight = transform_weight(row.weight, is_markoview_tuple=True)
    if transformed_weight is not None:
        # Handle database-specific representation for 'infinity'
        if transformed_weight == float('inf'):
            session.execute(
                text(f"UPDATE nv1 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2"),
                {'w_0': None, 'aid1': row.aid1, 'aid2': row.aid2}  # or some large number if 'None' is not feasible
            )
        else:
            session.execute(
                text(f"UPDATE nv1 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2"),
                {'w_0': transformed_weight, 'aid1': row.aid1, 'aid2': row.aid2}
            )


#-------------------------------------------

def transform_mvdb_to_indb():
    # Run this to create NV tables and populate them
    create_nv_tables()
    populate_nv_tables()

    # Step 1: Generate the NV tables based on MarkoViews (assuming this is already done)
    nv1_tuples = session.execute(text("SELECT * FROM nv1")).fetchall()
    nv2_tuples = session.execute(text("SELECT * FROM nv2")).fetchall()
    nv3_tuples = session.execute(text("SELECT * FROM nv3")).fetchall()

    # Step 2: Apply the transformation to weights for tuples in the MarkoViews
    # Update NV1
    for row in nv1_tuples:
        transformed_weight = transform_weight(row.weight, is_markoview_tuple=True)  # MarkoView tuple
        if transformed_weight is not None:
            session.execute(
                text(f"UPDATE nv1 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2"),
                {'w_0': transformed_weight, 'aid1': row.aid1, 'aid2': row.aid2}
            )

    # Update NV2
    for row in nv2_tuples:
        transformed_weight = transform_weight(row.weight, is_markoview_tuple=True)  # MarkoView tuple
        if transformed_weight is not None:
            session.execute(
                text(f"UPDATE nv2 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2"),
                {'w_0': transformed_weight, 'aid1': row.aid1, 'aid2': row.aid2}
            )

    # Update NV3
    for row in nv3_tuples:
        transformed_weight = transform_weight(row.weight, is_markoview_tuple=True)  # MarkoView tuple
        if transformed_weight is not None:
            session.execute(
                text(f"UPDATE nv3 SET w_0 = :w_0 WHERE aid1 = :aid1 AND aid2 = :aid2"),
                {'w_0': transformed_weight, 'aid1': row.aid1, 'aid2': row.aid2}
            )

    # Step 3: Calculate query probabilities (P0(Q ∨ W) - P0(W)) / (1 - P0(W))
    p_0_q_or_w = compute_probability("v1_coauthorship", aid1=1, aid2=2)  # Example query using V1
    p_0_w = compute_probability("union_nv_tables")  # Represents W as the union of all MarkoViews

    print(f"P_0(Q ∨ W): {p_0_q_or_w}, P_0(W): {p_0_w}")

    # Calculate the final probability using the formula
    if p_0_w != 1:  # Prevent division by zero
        final_probability = (p_0_q_or_w - p_0_w) / (1 - p_0_w)
    else:
        final_probability = 0  # Handle edge case if P0(W) is 1

    print(f"Final Probability: {final_probability}")

    session.commit()
    print("Transformation to INDB complete.")
