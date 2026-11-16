
import csv
import subprocess
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
CSV_FILE = PROJECT_DIR / "data" / "athlete_events.csv"
IMPORT_FILE = PROJECT_DIR / "data" / "olympics_import.csv"


def run_sql(sql):
    return subprocess.run(
        [
            "docker", "compose", "exec", "-T",
            "postgres", "psql",
            "-U", "airflow",
            "-d", "olympics_hw7",
            "-v", "ON_ERROR_STOP=1",
            "-c", sql,
        ],
        cwd=PROJECT_DIR,
        text=True,
        capture_output=True,
        check=True,
    )


def load_olympics():
    # 1. Створюємо схему та таблицю
    run_sql("""
        CREATE SCHEMA IF NOT EXISTS olympic_dataset;

        CREATE TABLE IF NOT EXISTS
        olympic_dataset.athlete_event_results (
            athlete_id TEXT,
            name TEXT,
            sport TEXT,
            event TEXT,
            medal TEXT
        );
    """)

    # 2. Читаємо початковий CSV
    with open(CSV_FILE, "r", encoding="utf-8-sig", newline="") as source, \
            open(IMPORT_FILE, "w", encoding="utf-8", newline="") as target:

        reader = csv.DictReader(source)
        writer = csv.writer(target)

        required = {"ID", "Name", "Sport", "Event", "Medal"}

        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(
                f"У CSV відсутні потрібні колонки. "
                f"Знайдено: {reader.fieldnames}"
            )

        count = 0

        for row in reader:
            writer.writerow([
                row["ID"],
                row["Name"],
                row["Sport"],
                row["Event"],
                row["Medal"]
            ])
            count += 1

    print(f"Підготовлено {count} записів.")

    # 3. Завантажуємо дані через PostgreSQL COPY
    with open(IMPORT_FILE, "rb") as file:
        subprocess.run(
            [
                "docker", "compose", "exec", "-T",
                "postgres", "psql",
                "-U", "airflow",
                "-d", "olympics_hw7",
                "-v", "ON_ERROR_STOP=1",
                "-c", """
                    BEGIN;
                    TRUNCATE olympic_dataset.athlete_event_results;

                    COPY olympic_dataset.athlete_event_results
                    (athlete_id, name, sport, event, medal)
                    FROM STDIN WITH (FORMAT CSV);

                    COMMIT;
                """,
            ],
            stdin=file,
            cwd=PROJECT_DIR,
            check=True,
        )

    print("Дані успішно завантажено!")


if __name__ == "__main__":
    load_olympics()
