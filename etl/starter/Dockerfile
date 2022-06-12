FROM  bde2020/spark-worker:3.0.1-hadoop3.2
WORKDIR /home

RUN pip install poetry
ENV PYTHONPATH = /code
RUN echo "PWD is: $PYTHONPATH"

COPY ./poetry.lock pyproject.toml ./
RUN poetry config virtualenvs.create false
RUN poetry install
COPY ./app ./app
WORKDIR /home/app/jobs

CMD ["poetry", "run", "python", "etl_job.py"]