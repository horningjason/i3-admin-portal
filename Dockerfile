FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies first (layer caching — only rebuilds if requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app/ ./app/

# nodes.json is expected to be volume-mounted or copied in at deploy time —
# it's environment-specific config, not baked into the image (see .dockerignore).

RUN adduser --disabled-password --gecos "" portaluser \
    && chown -R portaluser:portaluser /app
USER portaluser

EXPOSE 9100

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9100"]
