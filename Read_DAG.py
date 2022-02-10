import csv
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


args= {
    "owner":"Fiton",
    "start_date": days_ago(1)
     }


def read():
    with open("/usr/local/airflow/dags/test1/Python Test2.csv", 'r') as file:
          reader = csv.reader(file)
          for row in reader:
            print(row)



dag=  DAG('Read_CSV', default_args=args, schedule_interval=None, catchup=False)  
with dag:    

        task1 = PythonOperator(task_id='Task1', python_callable = read)
        


































'''import csv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime 








def read():
    with open("C:/Users/Aralytiks/Desktop/airflow-tutorial/dags/test1/Python Test2.csv", 'r') as file:

        reader = csv.reader(file)
        for row in reader:
            print(row)




with DAG("Read_Dag",start_date=datetime(2021,2,9,16,10),

     schedule_interval="@daily",catchup=False) as dag:

     

     t1 = PythonOperator(

         task_id = "t1",

         python_callable = read

     )        


 with open('C:/Users/Aralytiks/Desktop/airflow-tutorial/dags/test/Python Test.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['John ', "Doe", "john@doe.com"])
    writer.writerow(['John ', "Doe", "john@doe.com"])
    writer.writerow(['John ', "Doe", "john@doe.com"])
    writer.writerow(['John ', "Doe", "john@doe.com"])


    with open('C:/Users/Aralytiks/Desktop/airflow-tutorial/dags/test/Python Test1.csv', 'w', newline='') as file:
    fieldnames = ['player_name', 'fide_rating']
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerow({'player_name': 'Player1', 'fide_rating': 2870})
    writer.writerow({'player_name': 'Player 2', 'fide_rating': 2822})
    writer.writerow({'player_name': 'Player 3', 'fide_rating': 2801}) '''