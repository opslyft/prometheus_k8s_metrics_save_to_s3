FROM python:3.13.9

WORKDIR /app

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Persist uv path
ENV PATH="/root/.local/bin:/root/.cargo/bin:$PATH"

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install deps system-wide (NO venv)
RUN uv sync --frozen

# Copy app code
COPY . .

ENV AWS_DEFAULT_REGION=us-east-1

ENTRYPOINT ["bash", "run.sh"]