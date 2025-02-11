from airflow.decorators import task
from utils.connect_db import connect_db
import pymysql
@task
def save_to_caseprocessing(data: list) -> None:
    conn = connect_db()
    if conn is not None:
        with conn.cursor() as cursor:
            for record in data:
                try:
                    sql = """
                    INSERT INTO Case_processing 
                    (ID, Title, Reported_Date, Content, Url, Area) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        record['ID'], 
                        record['Title'], 
                        record['Reported_Date'], 
                        record['Content'], 
                        record['Url'], 
                        record['Area']
                    ))
                except pymysql.IntegrityError as e:
                    if e.args[0] == 1062:
                        print("Record already exists in the table.")
                except Exception as e:
                    print(f"Error inserting record: {e}")
        conn.commit()
    conn.close()