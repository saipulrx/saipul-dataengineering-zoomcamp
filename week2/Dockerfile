FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flow_prefect /opt/prefect/flow
COPY data /opt/prefect/data

ENV PREFECT_API_URL=http://127.0.0.1:4200/api
RUN prefect config set PREFECT_API_URL="${PREFECT_API_URL}"


