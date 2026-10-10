FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN useradd --create-home --uid 10001 app
WORKDIR /app
# dépendances copiées AVANT le code : la couche pip reste en cache tant que requirements.txt ne change pas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=app:app src/ ./src/
USER app
EXPOSE 3001
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "3001"]
