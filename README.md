# ITAD Labs — ЛР №2

Сквозной учебный data-проект курса «Информационные технологии анализа данных».

## Проект лабораторной работы

Тема проекта из ЛР №1: **влияние сезонных и погодных условий на спрос на прокат велосипедов**.

Основной источник проекта по ЛР №1: **Bike Sharing Dataset**, UCI Machine Learning Repository. Основной файл проекта — `hour.csv` с
почасовой детализацией. Это соответствует Project Brief ЛР №1.

В ходе выполнения ЛР №2 исходный старый HTTP-адрес UCI из каркаса вернул `404 Not Found`. Согласно заданию преподавателя,
при недоступности первоисточника разрешён HTTP-fallback через GitHub raw URL, закреплённый за конкретным commit SHA.
Для воспроизводимого fallback используется `hour.csv` из репозитория `alfozan/mlflow-example`, commit
`fa1ed37c09962d5b134d07703ba95f07648b73e0`. В README этого репозитория файл указан как Bike Sharing Dataset от UCI.

## Контур ЛР №2

```text
HTTP-источник hour.csv
        ↓
HttpSensor в Airflow
        ↓
PythonOperator / S3Hook
        ↓
SeaweedFS: raw/bike-sharing/ingested_on=<логическая дата>/hour.csv
        ↓
DuckDB читает raw-файл напрямую
```

В этой лабораторной работе реализуется только перенос исходных байтов в `raw`. Очистка, staging, mart и аналитические
расчёты не выполняются.

## 1. Подготовка проекта

Создайте `.env` из `.env.example` и не коммитьте его:

```bash
cp .env.example .env
```

В PowerShell:

```powershell
Copy-Item .env.example .env
```

Затем установите зависимости:

```bash
uv sync --locked
```

## 2. Локальный снимок источника

По заданию преподавателя копия исходного файла хранится в `data/source/`.

```bash
uv run python scripts/download_source.py
uv run python scripts/verify_source.py
```

`data/source/hour.csv` не изменяется кодом ingestion и нужен только как ручной резервный snapshot.

## 3. Запуск инфраструктуры

```bash
docker compose up --build -d
```

После запуска:

- Airflow: http://localhost:8080/
- SeaweedFS: http://localhost:8888/buckets/
- S3 API SeaweedFS: http://localhost:8333

Учётные данные Airflow и S3 берутся из `.env`.

## 4. Запуск DAG

В Airflow откройте DAG `ingest_raw`, включите его и запустите вручную.

DAG выполняет две задачи:

1. `wait_for_primary_source` — проверяет доступность HTTP-источника через `HttpSensor` с интервалом 60 секунд и
   таймаутом 600 секунд.
2. `load_to_raw` — скачивает исходные байты через HTTP и сохраняет их в `raw` через S3 API.

Объект имеет ключ:

```text
s3://raw/bike-sharing/ingested_on=<logical-date>/hour.csv
```

Повторный запуск за ту же логическую дату не создаёт новый объект и не перезаписывает существующий raw-файл.

## 5. Проверка через DuckDB

После успешного запуска DAG используйте путь к объекту из SeaweedFS:

```bash
uv run --env-file .env python scripts/read_raw.py --path "s3://raw/bike-sharing/ingested_on=YYYY-MM-DD/hour.csv"
```

Команда читает файл непосредственно из SeaweedFS и выводит названия столбцов и первые пять строк.

Проверка не создаёт таблицы, staging-файлы или дополнительные объекты в хранилище.

## 6. Проверка кода

```bash
uv run ruff check .
```

## 7. Что приложить к Pull Request

Согласно заданию ЛР №2, приложите:

- копию `hour.csv` в `data/source/`;
- `docs/project_brief.md` из ЛР №1;
- код DAG `airflow/dags/ingest_raw.py`;
- подтверждение запуска `docker compose up --build -d`;
- скриншот успешного графа `ingest_raw` в Airflow;
- скриншот объекта в бакете `raw` SeaweedFS с путём `ingested_on=...`;
- подтверждение успешного чтения `hour.csv` через DuckDB.

## Источник

Источник и описание набора зафиксированы в `docs/project_brief.md`. В рамках ЛР №1 для проекта выбран Bike Sharing
Dataset UCI, а `hour.csv` указан как основной файл для анализа.
