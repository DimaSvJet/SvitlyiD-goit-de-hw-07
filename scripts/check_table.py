
import subprocess
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent.parent


def run_sql(sql):
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T",
            "postgres", "psql",
            "-U", "airflow",
            "-d", "olympics_hw7",
            "-c", sql,
        ],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


print("КІЛЬКІСТЬ ОЛІМПІЙСЬКИХ МЕДАЛЕЙ:")

print(run_sql("""
    SELECT id, medal_type, count, created_at
FROM medal_counts
ORDER BY id DESC
LIMIT 20;
"""))
