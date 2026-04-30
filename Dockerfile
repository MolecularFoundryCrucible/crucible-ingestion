FROM python:3.11-trixie
#UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root
WORKDIR /root/

# Disable development dependencies
ENV UV_NO_DEV=1

ARG githash
ENV GITHASH=$githash

# basic utility packages
RUN apt-get update && \
    apt-get install -yq --no-upgrade unzip curl

# rclone
RUN curl https://rclone.org/install.sh | bash
COPY pyproject.toml /root/
COPY uv.lock /root/
COPY .python-version /root/
RUN /bin/uv sync --locked
RUN uv pip install git+https://github.com/MolecularFoundryCrucible/nano-crucible.git@dev
#RUN uv pip install nano-crucible
COPY . .

# Run our flow script when the container starts
CMD uv run python /root/consumer-ingestion-process.py




