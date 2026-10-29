FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY contracts ./contracts
COPY dbt ./dbt
COPY dashboard ./dashboard
COPY quality ./quality
RUN uv sync --frozen --no-dev
ENV AUS_HAZARD_ROOT=/app
CMD ["uv", "run", "python", "-m", "aus_hazard.stream", "--once"]
