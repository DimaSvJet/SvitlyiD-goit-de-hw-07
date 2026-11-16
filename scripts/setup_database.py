
import subprocess
from pathlib import Path

DB_NAME = "olympics_hw7"

PROJECT_DIR = Path(__file__).resolve().parent.parent


def run_sql(sql):
    # Виконуємо SQL у PostgreSQL через Docker
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T",
            "postgres",
            "psql",
            "-U", "airflow",
            "-d", "postgres",
            "-tAc", sql,
        ],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        check=True,
    )

    return result.stdout.strip()


def setup_database():
    print("Перевіряємо наявність бази даних...")

    exists = run_sql(
        f"SELECT 1 FROM pg_database "
        f"WHERE datname = '{DB_NAME}';"
    )

    if exists == "1":
        print(f"База {DB_NAME} вже існує.")
    else:
        print(f"Створюємо базу {DB_NAME}...")

        run_sql(f"CREATE DATABASE {DB_NAME};")

        print("Базу даних успішно створено!")

    print("Перевірка завершена.")


if __name__ == "__main__":
    setup_database()
