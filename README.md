✅ README.md — Sistema SAT Flask API
Aplicación web para consulta, análisis y carga masiva de RFCs del SAT Desarrollada en Flask + MySQL + Gunicorn, optimizada para despliegue en EasyPanel.

🚀 Características principales
Panel web y API REST para consultar contribuyentes en:

Definitivos

Desvirtuados

Presuntos

Sentencias Favorables

Listado Completo 69-B

Carga masiva de RFCs desde archivo .txt

Exportación de tablas a CSV

Generación de reportes Excel con múltiples hojas

Motor de plantillas Jinja2

Conexión MySQL optimizada

Listo para producción con Gunicorn detrás de Traefik

📁 Estructura del proyecto
Código
/
├── app.py
├── config.py
├── requirements.txt
├── Dockerfile
├── templates/
├── static/
└── data/
🐳 Dockerfile (Producción con Gunicorn)
dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p uploads

EXPOSE 8091

CMD ["gunicorn", "--bind", "0.0.0.0:8091", "app:app"]
📦 requirements.txt
Asegúrate de incluir:

Código
gunicorn==21.2.0
mysql-connector-python==8.1.0
pandas==2.1.3
openpyxl==3.1.2
flask==2.3.3
werkzeug==2.3.7
🗄️ Base de datos MySQL
Crear base:

sql
CREATE DATABASE satdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
Tablas requeridas:

ListadoGlobalDefinitivo

Definitivos

Desvirtuados

Presuntos

SentenciasFavorables

Listado_Completo_69_B

Archivo recomendado: sql/estructura_satdb.sql

🔧 Variables de entorno
En EasyPanel → sat-flask-app → Entorno:

Variable	Valor
DB_HOST	mysql-sat
DB_USER	satuser
DB_PASSWORD	satpass
DB_NAME	satdb
🌐 Despliegue en EasyPanel
Crear servicio MySQL

Crear servicio Flask App desde GitHub

Seleccionar Dockerfile como método de build

Agregar variables de entorno

Configurar dominio:

Código
Dominio: api.hcar.cloud
Puerto interno: 8091
Protocolo interno: http
HTTPS: activado (Let’s Encrypt)
Deploy

✅ Endpoints principales
Página principal
Código
GET /
Búsqueda por RFC
Código
GET /search?q=XXXX&type=rfc
API JSON por RFC
Código
GET /api/contribuyente/<rfc>
Carga masiva
Código
GET /carga_masiva
POST /carga_masiva
Exportar tabla
Código
GET /exportar/<nombre_tabla>
📄 Licencia
Uso interno. No redistribuir sin autorización.
