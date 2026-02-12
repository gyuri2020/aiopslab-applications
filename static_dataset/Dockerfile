FROM python:3.11-slim

RUN pip install --no-cache-dir pandas

# Create restricted user for agent shell access
RUN useradd -m -s /bin/bash agent

WORKDIR /app
COPY entrypoint.sh /app/entrypoint.sh
COPY process_telemetry.py /app/process_telemetry.py
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
