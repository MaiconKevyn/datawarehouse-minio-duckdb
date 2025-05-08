# Dockerfile
FROM python:3.11-slim

# 1) Instala tudo de build necessário + git
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      git \
      build-essential \
      python3-dev \
      libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# 2) Copia scripts e instala deps
WORKDIR /app
COPY scripts/ /app/
RUN pip install --no-cache-dir -r requirements.txt

# 3) Entry point
CMD ["python", "extract_load.py"]
