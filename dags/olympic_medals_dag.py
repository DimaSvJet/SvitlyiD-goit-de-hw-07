
import pendulum
import random
import time

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

from airflow.providers.common.sql.sensors.sql import SqlSensor

# Функція випадкового вибору медалі
def pick_medal():
    medals = ["Bronze", "Silver", "Gold"]
    selected_medal = random.choice(medals)
    print(f"Обрана медаль: {selected_medal}")
    return selected_medal

# Функція вибору розподілення
def choose_branch(ti):
    selected_medal=ti.xcom_pull(task_ids="pick_medal")
    print(f"Обрана гілка для медалі: {selected_medal}")
    return f"calc_{selected_medal}"


def generate_delay():
    delay = 5
    print(f"Очікування {delay} секунд...")
    time.sleep(delay)
    print("Затримка завершена.")

with DAG(
    dag_id="olympic_medals_hw7",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["hw7", "olympics"],
) as dag:

    # Створюємо таблицю для результатів
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_hw7",
        sql="""
            CREATE TABLE IF NOT EXISTS medal_counts (
                id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                medal_type VARCHAR(10) NOT NULL,
                count INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )
    # Випадковий вибір медалі
    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    branch_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_branch,
    )

    # Підрахунок медалей
    bronze_task = SQLExecuteQueryOperator(
        task_id="calc_Bronze",
        conn_id="postgres_hw7", 
        sql="""
            INSERT INTO medal_counts
                (medal_type, count, created_at)
            SELECT
                'Bronze', COUNT(*), CURRENT_TIMESTAMP
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """,
    )

    silver_task = SQLExecuteQueryOperator(
            task_id="calc_Silver",
            conn_id="postgres_hw7",
            sql="""
                        INSERT INTO medal_counts
                            (medal_type, count, created_at)
                        SELECT
                            'Silver', COUNT(*), CURRENT_TIMESTAMP
                        FROM olympic_dataset.athlete_event_results
                        WHERE medal = 'Silver';
                    """,

        )

    gold_task = SQLExecuteQueryOperator(
                task_id="calc_Gold",
                conn_id="postgres_hw7",
                sql="""
                                        INSERT INTO medal_counts
                                            (medal_type, count, created_at)
                                        SELECT
                                            'Gold', COUNT(*), CURRENT_TIMESTAMP
                                        FROM olympic_dataset.athlete_event_results
                                        WHERE medal = 'Gold';
                                    """,
            )

    delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule="none_failed_min_one_success",
    )

    check_task = SqlSensor(
        task_id="check_for_correctness",
        conn_id="postgres_hw7",
        sql="""
            SELECT CASE
                WHEN MAX(created_at) >= CURRENT_TIMESTAMP
                     - INTERVAL '30 seconds'
                THEN 1
                ELSE 0
            END
            FROM medal_counts;
        """,
        poke_interval=2,
        timeout=5,
        mode="poke",
        retries=0,
    )

    create_table >> pick_medal_task >> branch_task >> [
        bronze_task, silver_task, gold_task] >> delay_task >> check_task
