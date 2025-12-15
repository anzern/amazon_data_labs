FROM python:3.11-slim

# system deps (important for psycopg2)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*


# install dbt
RUN pip install --no-cache-dir dbt-postgres

WORKDIR /app

CMD ["bash"]
