# Usar una imagen base con Python (en tu caso, la versión 3.12.3)
FROM python:3.11.10-slim-bullseye

# Actualizar pip, setuptools y wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar solo el archivo requirements.txt primero para aprovechar la cache de Docker
COPY requirements.txt ./

# Instalar las dependencias desde requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código de la aplicación
COPY ./app ./app

# Exponer el puerto 8000 para la API de uvicorn
EXPOSE 8000

# Comando para ejecutar la API con uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]