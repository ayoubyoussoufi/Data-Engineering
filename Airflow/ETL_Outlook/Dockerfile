FROM apache/airflow:2.6.2

USER root
# Install dependencies and Microsoft ODBC Driver for SQL Server
RUN apt-get update \
    && apt-get install -y \
       gnupg \
       curl \
       unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
