FROM python:3.12-slim

# Install uv from its docker image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
WORKDIR /code

# copy uv config files
COPY pyproject.toml uv.lock .python-version ./

# install dependencies
RUN uv sync --locked

# copy the rest of the code
# COPY pipeline/pipeline.py pipeline.py
COPY ingest_data.py .

ENTRYPOINT ["uv", "run", "ingest_data.py"]