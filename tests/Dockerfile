ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}

WORKDIR /src
RUN pip install tox>=4.23

COPY src/pyper src/pyper
COPY tests tests
COPY pyproject.toml .
COPY tox.ini .
COPY .coveragerc .
COPY .git .git

RUN chmod +x tests/entrypoint.sh

ENTRYPOINT ["tests/entrypoint.sh"]