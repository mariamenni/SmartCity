FROM python:3.12-slim

WORKDIR /app

# Clone de la smart-cities-api (arnauropero/smart-cities-api)
# FastAPI sur le port 8000 avec des capteurs Barcelona en memoire
RUN apt-get update && apt-get install -y --no-install-recommends git curl \
    && git clone --depth 1 https://github.com/arnauropero/smart-cities-api.git . \
    && apt-get purge -y --auto-remove git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["python", "main.py"]
