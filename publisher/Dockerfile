FROM python:3.11-slim

RUN useradd --create-home --shell /bin/bash appuser
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY publisher.py .
RUN chown -R appuser:appuser /app
USER appuser

CMD ["python", "publisher.py"]
