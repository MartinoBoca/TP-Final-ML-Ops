FROM python:3.12.3-slim-bullseye

# Cambiar el directorio de trabajo a /api
WORKDIR /api

# Copiar el archivo requirements.txt e instalar dependencias
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código fuente
COPY api/ .

# Comando para ejecutar la API con uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]