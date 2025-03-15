FROM python:3.12-bookworm AS builder


RUN apt-get update && apt-get install --no-install-recommends -y \
   build-essential && \
   apt-get clean && rm -rf /var/lib/apt/lists/*
# RUN apt-get update && apt-get install -y gcc && pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt && apt remove -y gcc && rm -rf /var/lib/apt/lists/*

ADD https://astral.sh/uv/install.sh /install.sh

RUN chmod 655 /install.sh && /install.sh && rm /install.sh

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /usr/local/app
COPY ./pyproject.toml .
RUN uv sync --no-cache-dir

FROM python:3.12-slim-bookworm AS production
WORKDIR /usr/local/app
COPY src ./
COPY --from=builder /usr/local/app/.venv .venv
ENV PATH="/usr/local/app/.venv/bin:$PATH"





EXPOSE 8000
# RUN useradd -m app
# RUN chown -R app:app /usr/local/app
# USER app

CMD ["gunicorn", "--workers", "3", "--bind", "0.0.0.0:8000", "--timeout", "1800", "main:app"]


