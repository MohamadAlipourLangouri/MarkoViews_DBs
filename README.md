# Probabilistic Databases with MarkoViews

This project implements the concepts of Probabilistic Databases using MarkoViews based on the research paper "Probabilistic Databases with MarkoViews" by Abhay Jha and Dan Suciu, presented at VLDB 2012.

## Pre-requisites

This project was developed and tested with the following tools:
- **Python**: Version 3.8.10
- **PostgreSQL**: Version 14

Ensure these tools are installed on your system before proceeding.

## Installation

1. **Clone the repository:**
    ```bash
    git clone <https://github.com/MohamadAlipourLangouri/MarkoViews_DBs>
    cd <MarkoViews_DBs>
    ```

2. **Install dependencies:**
    Run the following command to install all necessary Python packages:
    ```bash
    pip install -r requirements.txt
    ```

3. **Database Setup:**
    - Create a new PostgreSQL database.
    - Configure the database connection in the `config.py` file by providing the correct database credentials (e.g., database name, user, password, host, and port).

4. **Run the project:**
    Execute the main Python script to start the application:
    ```bash
    python main.py
    ```

## Project Overview

This project simulates probabilistic database management using MarkoViews. It provides an implementation of MarkoViews as described in the referenced paper. Key components include:

- **MarkoViews:** Provides views (V1, V2, V3, etc.) that capture different relationships within the database and are transformed into Tuple-Independent Databases (INDB).
- **Database:** A toy dataset is populated using the `populate_data()` function, simulating a subset of the "DBLP" dataset for testing purposes.
- **View Creation:** Implements view creation logic (V1, V2, V3) as defined in the paper.
- **MVDB to INDB Transformation:** Transforms the MarkoView Database (MVDB) into an Independent Tuple Database (INDB) with support for complex probabilistic queries.

## Functionality

- **`populate_data()`**: Creates a toy database with authors, publications, affiliations, and student-advisor relationships to simulate the DBLP dataset.
- **`create_view_v1()`, `create_view_v2()`, `create_view_v3()`**: Defines and creates views in the database based on relationships between authors, publications, and affiliations.
- **`transform_mvdb_to_indb()`**: Transforms the created MarkoViews into tuple-independent databases for probabilistic query evaluation.
- **`show_view_v1()`, `show_view_v2()`, `show_view_v3()`**: Displays the contents of the views to verify the database's state and relationships.

## Acknowledgements

This work is inspired by the paper "Probabilistic Databases with MarkoViews" by Abhay Jha and Dan Suciu. 

Due to high memory requirements and encoding issues with the original "DBLP" XML dataset, we opted to create a toy database using the `populate_data()` function for testing and demonstration purposes. 

## Future Work

- Implement more complex MarkoViews and transformations.
- Optimize database queries for large-scale datasets.
- Explore integration with the original DBLP dataset after resolving encoding and memory constraints.
