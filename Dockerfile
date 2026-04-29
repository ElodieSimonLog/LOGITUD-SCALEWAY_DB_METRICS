FROM debian:bookworm-slim
 
RUN apt-get update && apt-get install -y \
    bash \
    curl \
    jq \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*
 
WORKDIR /app
COPY scaleway_db_metrics2.sh .
RUN chmod +x scaleway_db_metrics2.sh
 
CMD ["./scaleway_db_metrics2.sh"]
 