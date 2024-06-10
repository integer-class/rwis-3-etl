import os

import psycopg
from dotenv import load_dotenv

load_dotenv()


def get_connection(dbname='postgres'):
    try:
        # Define your connection parameters
        conn = psycopg.connect(
            dbname=dbname,
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except (Exception, psycopg.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None


with get_connection('rwis3') as oltp_connection, get_connection('rwis3-olap') as olap_connection:
    print("Creating table dim_time, dim_resident, dim_issue_report, fact_issue_report...")
    olap_connection.execute("""
    CREATE TABLE IF NOT EXISTS public.dim_time (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        day INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL       
    );
    
    CREATE TABLE IF NOT EXISTS public.dim_resident (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS public.dim_issue_report (
        id SERIAL PRIMARY KEY,
        resident_id INTEGER NOT NULL,
        title VARCHAR(255) NOT NULL,
        description TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL,
        status VARCHAR(255) NOT NULL,
        approval_status VARCHAR(255) NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS public.fact_issue_report (
        id SERIAL PRIMARY KEY,
        dim_time_id INTEGER NOT NULL,
        dim_resident_id INTEGER NOT NULL,
        dim_issue_report_id INTEGER NOT NULL,
        FOREIGN KEY (dim_time_id) REFERENCES dim_time (id),
        FOREIGN KEY (dim_resident_id) REFERENCES dim_resident (id),
        FOREIGN KEY (dim_issue_report_id) REFERENCES dim_issue_report (id)
    );
    """)
    print("Tables have been created successfully")

    # generate 5 years of time data
    print("Generating time data...")
    olap_connection.execute("""
    INSERT INTO dim_time (date, day, month, year)
    SELECT
        date::date,
        EXTRACT(DAY FROM date),
        EXTRACT(MONTH FROM date),
        EXTRACT(YEAR FROM date)
    FROM
        generate_series(
            '2020-01-01'::date,
            '2024-12-31'::date,
            '1 day'
        ) date
    """)
    print("Time data has been generated successfully")

    # generate resident data by taking them from the resident table and then inserting it into
    # the dim_resident table
    print("Moving resident data...")
    residents = oltp_connection.execute("SELECT full_name FROM resident").fetchall()
    args = ", ".join("('{}')".format(resident[0]) for resident in residents)
    olap_connection.execute("INSERT INTO dim_resident (name) VALUES {}".format(args))
    print("Resident data has been moved successfully")

    # move the issue report data from the issue_report table to the dim_issue_report table
    print("Moving issue report data...")
    issue_reports = oltp_connection.execute(
        "SELECT resident_id, title, description, created_at, updated_at, status, approval_status FROM issue_report"
    ).fetchall()
    args = ", ".join(
        "('{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(*issue_report) for issue_report in issue_reports)
    olap_connection.execute(
        """INSERT INTO dim_issue_report (resident_id, title, description, created_at, updated_at, status, approval_status) 
        VALUES {}""".format(args)
    )
    print("Issue report data has been moved successfully")

    # create fact table by joining the dim tables with the fact table
    print("Creating fact table...")
    olap_connection.execute("""
    INSERT INTO fact_issue_report (dim_time_id, dim_resident_id, dim_issue_report_id)
    SELECT
        dim_time.id,
        dim_resident.id,
        dim_issue_report.id
    FROM
        dim_issue_report
        JOIN dim_time ON dim_issue_report.created_at::date = dim_time.date
        JOIN dim_resident ON dim_issue_report.resident_id = dim_resident.id
    """)
    print("Fact table has been created successfully")

    oltp_connection.commit()
    olap_connection.commit()
