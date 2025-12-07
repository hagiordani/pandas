# Imagen base oficial de Python
FROM python:3.11-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivos del proyecto
COPY . .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Crear carpeta de uploads si no existe
RUN mkdir -p uploads

# Exponer el puerto interno que usará Gunicorn
EXPOSE 8091

# Comando de inicio con Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8091", "app:app"]



