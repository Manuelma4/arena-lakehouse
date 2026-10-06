FROM eclipse-temurin:17-jre AS java
FROM python:3.11-slim-bookworm
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"
ENV SPARK_LOCAL_IP=127.0.0.1
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir '.[spark]' && python -c "from arena_lakehouse.spark_backend import session; s=session(); s.range(1).count(); s.stop()"
ENTRYPOINT ["python", "-m", "arena_lakehouse.spark_backend"]
