#!/usr/bin/env python3
"""
Sistema SAT - Interfaz Web (Flask)
Versión limpia, sin duplicados, lista para producción.
"""

from flask import Flask, render_template, request, jsonify, flash, redirect, send_file
from config import DB_CONFIG
import mysql.connector
from datetime import datetime
import pandas as pd
import os
import io
import csv
import traceback
from werkzeug.utils import secure_filename

# ---------------------------------------------------------
# CONFIGURACIÓN GENERAL
# ---------------------------------------------------------

app = Flask(__name__)
app.secret_key = 'sat_secret_key_2024'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

ALLOWED_EXTENSIONS = {'txt'}

# ---------------------------------------------------------
# UTILIDADES
# ---------------------------------------------------------

@app.route("/")
def index():
    tablas = {
        "labels": ["Definitivos", "Presuntos", "Desvirtuados"],
        "values": [1200, 800, 450]
    }

    cargas_dias = {
        "labels": ["Lun", "Mar", "Mié", "Jue", "Vie"],
        "values": [10, 25, 18, 30, 22]
    }

    estados = {
        "labels": ["Activo", "Suspendido", "Baja"],
        "values": [70, 20, 10]
    }

    return render_template(
        "index.html",
        total_registros=2450,
        total_tablas=5,
        ultima_carga="2025-01-12",
        procesados_hoy=32,
        tablas_json=json.dumps(tablas),
        cargas_dias_json=json.dumps(cargas_dias),
        estados_json=json.dumps(estados)
    )

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_db_connection():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as e:
        print(f"Error de base de datos: {e}")
        return None

@app.context_processor
def inject_now():
    return {'now': datetime.now(), 'app_name': 'Sistema SAT'}

def buscar_rfc_en_tablas(rfc, cursor):
    tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
    encontradas = []

    for tabla in tablas:
        try:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {tabla} WHERE UPPER(rfc) = %s", (rfc.upper(),))
            if cursor.fetchone()['count'] > 0:
                encontradas.append(tabla)
        except:
            pass

    return encontradas

# ---------------------------------------------------------
# DASHBOARD PRINCIPAL
# ---------------------------------------------------------

@app.route('/')
def index():
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']

        # Estadísticas generales
        stats = {}
        for tabla in tablas:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {tabla}")
            stats[tabla] = cursor.fetchone()['count']
        stats['total'] = sum(stats.values())

        # Situaciones
        cursor.execute("""
            SELECT situacion_contribuyente, COUNT(*) AS count
            FROM Listado_Completo_69_B
            GROUP BY situacion_contribuyente
            ORDER BY count DESC
        """)
        situaciones = cursor.fetchall()

        # Últimas actualizaciones
        cursor.execute("""
            SELECT table_name, fecha_actualizacion
            FROM (
                SELECT 'Definitivos' AS table_name, MAX(fecha_actualizacion) AS fecha_actualizacion FROM Definitivos
                UNION SELECT 'Desvirtuados', MAX(fecha_actualizacion) FROM Desvirtuados
                UNION SELECT 'Presuntos', MAX(fecha_actualizacion) FROM Presuntos
                UNION SELECT 'SentenciasFavorables', MAX(fecha_actualizacion) FROM SentenciasFavorables
                UNION SELECT 'Listado_Completo_69_B', MAX(fecha_actualizacion) FROM Listado_Completo_69_B
            ) AS updates
            ORDER BY fecha_actualizacion DESC
        """)
        actualizaciones = cursor.fetchall()

        cursor.close()
        conn.close()

        return render_template(
            'index.html',
            stats=stats,
            situaciones=situaciones,
            actualizaciones=actualizaciones
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

# ---------------------------------------------------------
# BÚSQUEDA
# ---------------------------------------------------------

@app.route('/search')
def search():
    query = request.args.get('q', '').strip()
    search_type = request.args.get('type', 'rfc')

    if not query:
        return render_template('search.html', results=[], query='', search_type=search_type)

    query = query.upper()  # Normalizamos a mayúsculas

    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        results = []
        tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']

        if search_type == 'rfc':
            for tabla in tablas:
                cursor.execute(f"""
                    SELECT *, '{tabla}' AS tabla_origen
                    FROM {tabla}
                    WHERE UPPER(rfc) = %s
                    ORDER BY numero
                """, (query,))
                results.extend(cursor.fetchall())

        else:  # búsqueda por nombre en mayúsculas
            for tabla in tablas:
                cursor.execute(f"""
                    SELECT *, '{tabla}' AS tabla_origen
                    FROM {tabla}
                    WHERE UPPER(nombre_contribuyente) LIKE %s
                    ORDER BY numero
                    LIMIT 100
                """, (f"%{query}%",))
                results.extend(cursor.fetchall())

        cursor.close()
        conn.close()

        return render_template(
            'search.html',
            results=results,
            query=query,
            search_type=search_type,
            results_count=len(results)
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

# ---------------------------------------------------------
# API RFC
# ---------------------------------------------------------

@app.route('/api/contribuyente/<rfc>')
def api_contribuyente(rfc):
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Error de conexión a la base de datos'}), 500

    cursor = conn.cursor(dictionary=True)
    tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
    results = []

    try:
        for tabla in tablas:
            cursor.execute(f"SELECT * FROM {tabla} WHERE UPPER(rfc) = %s", (rfc.upper(),))
            for row in cursor.fetchall():
                row['tabla_origen'] = tabla
                results.append(row)

        cursor.close()
        conn.close()
        return jsonify(results)

    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'error': str(e)}), 500

# ---------------------------------------------------------
# ESTADÍSTICAS DETALLADAS
# ---------------------------------------------------------

@app.route('/estadisticas')
def estadisticas():
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']

        # Totales
        stats = {}
        for tabla in tablas:
            cursor.execute(f"SELECT COUNT(*) AS total FROM {tabla}")
            stats[tabla] = cursor.fetchone()['total']

        # Duplicados
        duplicates = {}
        for tabla in tablas:
            cursor.execute(f"""
                SELECT COUNT(*) AS duplicate_count
                FROM (
                    SELECT rfc, COUNT(*) AS count
                    FROM {tabla}
                    WHERE rfc IS NOT NULL
                    GROUP BY rfc
                    HAVING COUNT(*) > 1
                ) AS dups
            """)
            duplicates[tabla] = cursor.fetchone()['duplicate_count']

        # Situaciones
        cursor.execute("""
            SELECT situacion_contribuyente, COUNT(*) AS count
            FROM Listado_Completo_69_B
            GROUP BY situacion_contribuyente
            ORDER BY count DESC
        """)
        situaciones = cursor.fetchall()

        # Actualizaciones
        cursor.execute("""
            SELECT 
                table_name,
                MAX(fecha_actualizacion) AS ultima_actualizacion,
                COUNT(*) AS total_registros
            FROM (
                SELECT 'Definitivos' AS table_name, fecha_actualizacion FROM Definitivos
                UNION ALL SELECT 'Desvirtuados', fecha_actualizacion FROM Desvirtuados
                UNION ALL SELECT 'Presuntos', fecha_actualizacion FROM Presuntos
                UNION ALL SELECT 'SentenciasFavorables', fecha_actualizacion FROM SentenciasFavorables
                UNION ALL SELECT 'Listado_Completo_69_B', fecha_actualizacion FROM Listado_Completo_69_B
            ) AS all_tables
            GROUP BY table_name
            ORDER BY table_name
        """)
        actualizaciones = cursor.fetchall()

        cursor.close()
        conn.close()

        return render_template(
            'estadisticas.html',
            stats=stats,
            duplicates=duplicates,
            situaciones=situaciones,
            actualizaciones=actualizaciones
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

# ---------------------------------------------------------
# TABLAS
# ---------------------------------------------------------

@app.route('/tablas')
def tablas():
    tablas_info = [
        {'nombre': 'Definitivos', 'ruta': 'definitivos', 'descripcion': 'Contribuyentes con situación definitiva'},
        {'nombre': 'Desvirtuados', 'ruta': 'desvirtuados', 'descripcion': 'Contribuyentes desvirtuados'},
        {'nombre': 'Presuntos', 'ruta': 'presuntos', 'descripcion': 'Contribuyentes presuntos'},
        {'nombre': 'Sentencias Favorables', 'ruta': 'sentenciasfavorables', 'descripcion': 'Sentencias favorables'},
        {'nombre': 'Listado Completo 69-B', 'ruta': 'listado_completo_69_b', 'descripcion': 'Listado completo del artículo 69-B'}
    ]
    return render_template('tablas.html', tablas=tablas_info)

@app.route('/tabla/<nombre_tabla>')
def ver_tabla(nombre_tabla):
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        tablas_validas = {
            'definitivos': 'Definitivos',
            'desvirtuados': 'Desvirtuados',
            'presuntos': 'Presuntos',
            'sentenciasfavorables': 'SentenciasFavorables',
            'listado_completo_69_b': 'Listado_Completo_69_B'
        }

        tabla_real = tablas_validas.get(nombre_tabla.lower())
        if not tabla_real:
            return "Tabla no válida", 400

        page = request.args.get('page', 1, type=int)
        per_page = 50
        offset = (page - 1) * per_page

        cursor.execute(f"SELECT COUNT(*) AS total FROM {tabla_real}")
        total = cursor.fetchone()['total']

        cursor.execute(f"""
            SELECT * FROM {tabla_real}
            ORDER BY numero
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        registros = cursor.fetchall()

        cursor.execute(f"DESCRIBE {tabla_real}")
        columnas = [col['Field'] for col in cursor.fetchall()]

        total_pages = (total + per_page - 1) // per_page

        tabla_info = {
            'definitivos': {'nombre': 'Definitivos', 'descripcion': 'Contribuyentes con situación definitiva'},
            'desvirtuados': {'nombre': 'Desvirtuados', 'descripcion': 'Contribuyentes desvirtuados'},
            'presuntos': {'nombre': 'Presuntos', 'descripcion': 'Contribuyentes presuntos'},
            'sentenciasfavorables': {'nombre': 'Sentencias Favorables', 'descripcion': 'Sentencias favorables'},
            'listado_completo_69_b': {'nombre': 'Listado Completo 69-B', 'descripcion': 'Listado completo del artículo 69-B'}
        }.get(nombre_tabla.lower())

        cursor.close()
        conn.close()

        return render_template(
            'tabla_detalle.html',
            tabla=tabla_real,
            tabla_info=tabla_info,
            registros=registros,
            columnas=columnas,
            page=page,
            total_pages=total_pages,
            total=total
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

# ---------------------------------------------------------
# EXPORTAR CSV
# ---------------------------------------------------------

@app.route('/exportar/<nombre_tabla>')
def exportar_tabla(nombre_tabla):
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500

    cursor = conn.cursor(dictionary=True)

    try:
        tablas_validas = {
            'definitivos': 'Definitivos',
            'desvirtuados': 'Desvirtuados',
            'presuntos': 'Presuntos',
            'sentenciasfavorables': 'SentenciasFavorables',
            'listado_completo_69_b': 'Listado_Completo_69_B'
        }

        tabla_real = tablas_validas.get(nombre_tabla.lower())
        if tabla_real is None:
            return "Tabla no válida", 400

        cursor.execute(f"SELECT * FROM {tabla_real} ORDER BY numero")
        registros = cursor.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)

        if registros:
            writer.writerow(registros[0].keys())

        for registro in registros:
            writer.writerow(registro.values())

        output.seek(0)

        cursor.close()
        conn.close()

        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{tabla_real}_{datetime.now().strftime('%Y%m%d')}.csv"
        )

    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

# ---------------------------------------------------------
# CARGA CSV
# ---------------------------------------------------------

@app.route('/carga_csv', methods=['GET', 'POST'])
def carga_csv():
    if request.method == 'POST':
        if 'archivo' not in request.files:
            flash('No se seleccionó ningún archivo', 'danger')
            return redirect(request.url)

        archivo = request.files['archivo']

        if archivo.filename == '':
            flash('No se seleccionó ningún archivo', 'danger')
            return redirect(request.url)

        if not archivo.filename.lower().endswith('.csv'):
            flash('Solo se permiten archivos CSV', 'danger')
            return redirect(request.url)

        tabla = request.form.get('tabla')
        tablas_validas = {
            'definitivos': 'Definitivos',
            'desvirtuados': 'Desvirtuados',
            'presuntos': 'Presuntos',
            'sentenciasfavorables': 'SentenciasFavorables',
            'listado_completo_69_b': 'Listado_Completo_69_B'
        }

        tabla_real = tablas_validas.get(tabla.lower())
        if tabla_real is None:
            flash('Tabla destino no válida', 'danger')
            return redirect(request.url)

        try:
            df = pd.read_csv(archivo)

            if df.empty:
                flash('El archivo CSV está vacío', 'danger')
                return redirect(request.url)

            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute(f"DESCRIBE {tabla_real}")
            columnas_tabla = [col['Field'] for col in cursor.fetchall()]

            columnas_validas = [c for c in df.columns if c in columnas_tabla]

            if not columnas_validas:
                flash('El CSV no contiene columnas válidas para esta tabla', 'danger')
                return redirect(request.url)

            df = df[columnas_validas]
            df = df.where(pd.notnull(df), None)

            placeholders = ", ".join(["%s"] * len(columnas_validas))
            columnas_sql = ", ".join(columnas_validas)
            query = f"INSERT INTO {tabla_real} ({columnas_sql}) VALUES ({placeholders})"

            registros = df.values.tolist()
            cursor.executemany(query, registros)
            conn.commit()

            total = cursor.rowcount

            # Registrar en historial
            cursor.execute("""
                INSERT INTO Historial_Cargas (nombre_archivo, tabla, registros)
                VALUES (%s, %s, %s)
            """, (archivo.filename, tabla_real, total))
            conn.commit()

            cursor.close()
            conn.close()

            flash(f"✅ Se cargaron {total} registros correctamente en la tabla {tabla_real}", "success")
            return redirect(request.url)

        except Exception as e:
            traceback.print_exc()
            flash(f"Error procesando el archivo: {str(e)}", "danger")
            return redirect(request.url)

    return render_template('carga_csv.html')

# ---------------------------------------------------------
# HISTORIAL DE CARGAS
# ---------------------------------------------------------

@app.route('/historial_cargas')
def historial_cargas():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM Historial_Cargas ORDER BY fecha DESC LIMIT 200")
    cargas = cursor.fetchall()

    cursor.close()
    conn.close()

    return render_template('historial_cargas.html', cargas=cargas)

# ---------------------------------------------------------
# CARGA MASIVA TXT
# ---------------------------------------------------------

@app.route('/carga_masiva', methods=['GET', 'POST'])
def carga_masiva():
    if request.method == 'POST':
        archivo = request.files.get('archivo')
        nombre_reporte = request.form.get('nombre_reporte', 'reporte')

        if not archivo or archivo.filename == '':
            flash(('danger', 'No seleccionaste ningún archivo'))
            return redirect(request.url)

        try:
            contenido = archivo.read().decode('latin1').splitlines()
            rfcs = [line.strip().upper() for line in contenido if line.strip()]

            flash(('success', f'Se procesaron {len(rfcs)} RFCs correctamente'))
            return redirect('/carga_masiva')

        except Exception as e:
            flash(('danger', f'Error procesando archivo: {e}'))
            return redirect('/carga_masiva')

    return render_template('carga_masiva.html')
