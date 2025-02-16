FROM python:3.12-slim


WORKDIR /usr/local/app

COPY requirements.txt ./
COPY src ./
RUN apt-get update && apt-get install -y gcc && pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt && apt remove -y gcc && rm -rf /var/lib/apt/lists/*


EXPOSE 8000
# RUN useradd -m app
# RUN chown -R app:app /usr/local/app
# USER app

CMD ["gunicorn", "--workers", "3", "--bind", "0.0.0.0:8000", "--timeout", "1800", "main:app"]


