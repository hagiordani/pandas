#!/usr/bin/env python3
"""
Interfaz Web para el Sistema SAT - Flask App
"""

from flask import Flask, render_template, request, jsonify, flash, redirect, url_for, send_file, Response
from config import DB_CONFIG
import mysql.connector
from datetime import datetime
import pandas as pd
import os
import tempfile
import traceback
import io
import csv
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'sat_secret_key_2024'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Crear carpeta de uploads si no existe
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Extensión permitida para archivos de carga masiva
ALLOWED_EXTENSIONS = {'txt'}

@app.route('/')

# hag def index():

def index_page():
    return render_template('index.html')



def allowed_file(filename):
    """Verifica si la extensión del archivo es permitida"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_db_connection():
    """Establecer conexión a la base de datos"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"Error de base de datos: {e}")
        return None

@app.context_processor
def inject_now():
    """Inyecta variables en todos los templates"""
    return {
        'now': datetime.now(),
        'app_name': 'Sistema SAT'
    }

def buscar_rfc_en_tablas(rfc, cursor):
    """Busca un RFC en todas las tablas y devuelve las tablas donde se encontró"""
    tablas_encontradas = []
    tablas = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
    
    for tabla in tablas:
        try:
            cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {tabla} 
                WHERE UPPER(rfc) = %s
            """, (rfc.upper(),))
            resultado = cursor.fetchone()
            if resultado and resultado['count'] > 0:
                tablas_encontradas.append(tabla)
        except Exception as e:
            print(f"Error buscando RFC {rfc} en tabla {tabla}: {e}")
            continue
    
    return tablas_encontradas

def procesar_archivo_rfcs(archivo_path, nombre_reporte=None):
    """Procesa un archivo de RFCs y genera un reporte Excel"""
    try:
        # Leer RFCs del archivo
        with open(archivo_path, 'r', encoding='utf-8', errors='ignore') as f:
            lineas = f.readlines()
        
        # Limpiar y filtrar RFCs
        rfcs = []
        for linea in lineas:
            rfc = linea.strip().upper()
            if rfc and len(rfc) >= 10:  # RFC mínimo de 10 caracteres
                rfcs.append(rfc)
        
        if not rfcs:
            return None, "El archivo no contiene RFCs válidos"
        
        # Eliminar duplicados pero mantener orden
        rfcs_unicos = []
        visto = set()
        for rfc in rfcs:
            if rfc not in visto:
                visto.add(rfc)
                rfcs_unicos.append(rfc)
        
        rfcs = rfcs_unicos
        
        # Conectar a la base de datos
        conn = get_db_connection()
        if not conn:
            return None, "Error de conexión a la base de datos"
        
        cursor = conn.cursor(dictionary=True)
        
        # Buscar cada RFC en las tablas
        resultados = []
        encontrados = 0
        no_encontrados = 0
        
        for rfc in rfcs:
            tablas_encontradas = buscar_rfc_en_tablas(rfc, cursor)
            
            if tablas_encontradas:
                encontrado = "SÍ"
                encontrados += 1
                tablas_str = ", ".join(tablas_encontradas)
            else:
                encontrado = "NO"
                no_encontrados += 1
                tablas_str = "NO ENCONTRADO"
            
            resultados.append({
                'RFC': rfc,
                'ENCONTRADO': encontrado,
                'TABLAS': tablas_str
            })
        
        cursor.close()
        conn.close()
        
        # Crear DataFrame
        df = pd.DataFrame(resultados)
        
        # Generar nombre del archivo Excel
        if not nombre_reporte:
            nombre_reporte = f"reporte_rfcs_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        else:
            nombre_reporte = secure_filename(nombre_reporte)
        
        excel_path = os.path.join(app.config['UPLOAD_FOLDER'], f"{nombre_reporte}.xlsx")
        
        # Crear Excel con múltiples hojas
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Hoja principal con todos los resultados
            df.to_excel(writer, sheet_name='Resultados', index=False)
            
            # Hoja con resumen estadístico
            resumen_data = {
                'ESTADÍSTICA': ['Total RFCs Procesados', 'RFCs Encontrados', 'RFCs No Encontrados', 'Porcentaje de Éxito'],
                'VALOR': [len(rfcs), encontrados, no_encontrados, f"{(encontrados/len(rfcs)*100):.2f}%" if len(rfcs) > 0 else "0%"]
            }
            df_resumen = pd.DataFrame(resumen_data)
            df_resumen.to_excel(writer, sheet_name='Resumen', index=False)
            
            # Hoja con RFCs no encontrados
            if no_encontrados > 0:
                df_no_encontrados = df[df['ENCONTRADO'] == 'NO'].copy()
                df_no_encontrados.to_excel(writer, sheet_name='No Encontrados', index=False)
            
            # Hoja con RFCs encontrados
            if encontrados > 0:
                df_encontrados = df[df['ENCONTRADO'] == 'SÍ'].copy()
                df_encontrados.to_excel(writer, sheet_name='Encontrados', index=False)
        
        # Estadísticas para el historial
        stats = {
            'total_rfcs': len(rfcs),
            'encontrados': encontrados,
            'no_encontrados': no_encontrados,
            'porcentaje_exito': f"{(encontrados/len(rfcs)*100):.2f}%" if len(rfcs) > 0 else "0%"
        }
        
        return excel_path, stats, None
        
    except Exception as e:
        traceback.print_exc()
        return None, None, str(e)

@app.route('/carga_masiva', methods=['GET', 'POST'])
def carga_masiva():
    """Página para carga masiva de RFCs"""
    
    if request.method == 'POST':
        # Verificar si se subió un archivo
        if 'archivo' not in request.files:
            flash('No se seleccionó ningún archivo', 'danger')
            return redirect(request.url)
        
        archivo = request.files['archivo']
        
        # Si el usuario no selecciona un archivo
        if archivo.filename == '':
            flash('No se seleccionó ningún archivo', 'danger')
            return redirect(request.url)
        
        # Verificar extensión
        if not allowed_file(archivo.filename):
            flash('Solo se permiten archivos .txt', 'danger')
            return redirect(request.url)
        
        # Obtener nombre del reporte
        nombre_reporte = request.form.get('nombre_reporte', '')
        
        try:
            # Guardar archivo temporalmente
            filename = secure_filename(archivo.filename)
            temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_{filename}")
            archivo.save(temp_path)
            
            # Procesar archivo
            excel_path, stats, error = procesar_archivo_rfcs(temp_path, nombre_reporte)
            
            # Limpiar archivo temporal
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            if error:
                flash(f'Error procesando archivo: {error}', 'danger')
                return redirect(request.url)
            
            if not excel_path:
                flash('No se pudo generar el reporte', 'danger')
                return redirect(request.url)
            
            # Devolver el archivo Excel para descargar
            return send_file(
                excel_path,
                as_attachment=True,
                download_name=f"reporte_rfcs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
        except Exception as e:
            flash(f'Error procesando el archivo: {str(e)}', 'danger')
            return redirect(request.url)
    
    # GET request - mostrar formulario
    return render_template('carga_masiva.html')

@app.route('/')
def index():
    """Página principal con estadísticas"""
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Estadísticas generales
        stats = {}
        tables = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            stats[table] = cursor.fetchone()['count']
        
        # Total general
        stats['total'] = sum(stats.values())
        
        # Situaciones en Listado_Completo_69_B
        cursor.execute("""
            SELECT situacion_contribuyente, COUNT(*) as count 
            FROM Listado_Completo_69_B 
            GROUP BY situacion_contribuyente 
            ORDER BY count DESC
        """)
        situaciones = cursor.fetchall()
        
        # Últimas actualizaciones
        cursor.execute("""
            SELECT table_name, fecha_actualizacion 
            FROM (
                SELECT 'Definitivos' as table_name, MAX(fecha_actualizacion) as fecha_actualizacion FROM Definitivos
                UNION SELECT 'Desvirtuados', MAX(fecha_actualizacion) FROM Desvirtuados
                UNION SELECT 'Presuntos', MAX(fecha_actualizacion) FROM Presuntos
                UNION SELECT 'SentenciasFavorables', MAX(fecha_actualizacion) FROM SentenciasFavorables
                UNION SELECT 'Listado_Completo_69_B', MAX(fecha_actualizacion) FROM Listado_Completo_69_B
            ) as updates
            ORDER BY fecha_actualizacion DESC
        """)
        actualizaciones = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return render_template('index.html', 
                             stats=stats, 
                             situaciones=situaciones,
                             actualizaciones=actualizaciones)
    
    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

@app.route('/search')
def search():
    """Búsqueda de contribuyentes"""
    query = request.args.get('q', '').strip()
    search_type = request.args.get('type', 'rfc')
    
    if not query:
        return render_template('search.html', results=[], query='', search_type=search_type)
    
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        results = []
        
        if search_type == 'rfc':
            # Búsqueda por RFC exacto
            tables = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
            
            for table in tables:
                cursor.execute(f"""
                    SELECT *, '{table}' as tabla_origen 
                    FROM {table} 
                    WHERE rfc = %s
                    ORDER BY numero
                """, (query.upper(),))
                table_results = cursor.fetchall()
                results.extend(table_results)
        
        elif search_type == 'nombre':
            # Búsqueda por nombre (parcial)
            tables = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
            
            for table in tables:
                cursor.execute(f"""
                    SELECT *, '{table}' as tabla_origen 
                    FROM {table} 
                    WHERE nombre_contribuyente LIKE %s
                    ORDER BY numero
                    LIMIT 100
                """, (f'%{query}%',))
                table_results = cursor.fetchall()
                results.extend(table_results)
        
        cursor.close()
        conn.close()
        
        return render_template('search.html', 
                             results=results, 
                             query=query, 
                             search_type=search_type,
                             results_count=len(results))
    
    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

@app.route('/api/contribuyente/<rfc>')
def api_contribuyente(rfc):
    """API para obtener datos de un contribuyente por RFC"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Error de conexión a la base de datos'}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        results = []
        tables = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
        
        for table in tables:
            cursor.execute(f"SELECT * FROM {table} WHERE rfc = %s", (rfc.upper(),))
            table_results = cursor.fetchall()
            for row in table_results:
                row['tabla_origen'] = table
                results.append(row)
        
        cursor.close()
        conn.close()
        
        return jsonify(results)
    
    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'error': str(e)}), 500

@app.route('/estadisticas')
def estadisticas():
    """Página de estadísticas detalladas"""
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Estadísticas por tabla
        stats = {}
        tables = ['Definitivos', 'Desvirtuados', 'Presuntos', 'SentenciasFavorables', 'Listado_Completo_69_B']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as total FROM {table}")
            stats[table] = cursor.fetchone()['total']
        
        # Duplicados por tabla
        duplicates = {}
        for table in tables:
            cursor.execute(f"""
                SELECT COUNT(*) as duplicate_count 
                FROM (
                    SELECT rfc, COUNT(*) as count 
                    FROM {table} 
                    WHERE rfc IS NOT NULL 
                    GROUP BY rfc 
                    HAVING COUNT(*) > 1
                ) as dups
            """)
            result = cursor.fetchone()
            duplicates[table] = result['duplicate_count'] if result else 0
        
        # Situaciones en Listado_Completo_69_B
        cursor.execute("""
            SELECT situacion_contribuyente, COUNT(*) as count 
            FROM Listado_Completo_69_B 
            GROUP BY situacion_contribuyente 
            ORDER BY count DESC
        """)
        situaciones = cursor.fetchall()
        
        # Estadísticas por fecha de actualización
        cursor.execute("""
            SELECT 
                table_name,
                MAX(fecha_actualizacion) as ultima_actualizacion,
                COUNT(*) as total_registros
            FROM (
                SELECT 'Definitivos' as table_name, fecha_actualizacion FROM Definitivos
                UNION ALL SELECT 'Desvirtuados', fecha_actualizacion FROM Desvirtuados
                UNION ALL SELECT 'Presuntos', fecha_actualizacion FROM Presuntos
                UNION ALL SELECT 'SentenciasFavorables', fecha_actualizacion FROM SentenciasFavorables
                UNION ALL SELECT 'Listado_Completo_69_B', fecha_actualizacion FROM Listado_Completo_69_B
            ) as all_tables
            GROUP BY table_name
            ORDER BY table_name
        """)
        actualizaciones = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return render_template('estadisticas.html', 
                             stats=stats, 
                             duplicates=duplicates,
                             situaciones=situaciones,
                             actualizaciones=actualizaciones)
    
    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

@app.route('/tablas')
def tablas():
    """Página principal de tablas disponibles"""
    tablas_info = [
        {
            'nombre': 'Definitivos',
            'ruta': 'definitivos',
            'descripcion': 'Contribuyentes con situación definitiva en el SAT',
            'icono': 'check-circle'
        },
        {
            'nombre': 'Desvirtuados',
            'ruta': 'desvirtuados',
            'descripcion': 'Contribuyentes desvirtuados del padrón',
            'icono': 'times-circle'
        },
        {
            'nombre': 'Presuntos',
            'ruta': 'presuntos',
            'descripcion': 'Contribuyentes presuntos en el padrón',
            'icono': 'question-circle'
        },
        {
            'nombre': 'Sentencias Favorables',
            'ruta': 'sentenciasfavorables',
            'descripcion': 'Sentencias favorables a contribuyentes',
            'icono': 'gavel'
        },
        {
            'nombre': 'Listado Completo 69-B',
            'ruta': 'listado_completo_69_b',
            'descripcion': 'Listado completo del artículo 69-B',
            'icono': 'list-alt'
        }
    ]
    
    return render_template('tablas.html', tablas=tablas_info)

@app.route('/tabla/<nombre_tabla>')
def ver_tabla(nombre_tabla):
    """Ver todos los registros de una tabla específica"""
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Validar que la tabla existe
        tablas_validas = ['definitivos', 'desvirtuados', 'presuntos', 'sentenciasfavorables', 'listado_completo_69_b']
        if nombre_tabla.lower() not in tablas_validas:
            return "Tabla no válida", 400
        
        # Obtener datos con paginación
        page = request.args.get('page', 1, type=int)
        per_page = 50
        offset = (page - 1) * per_page
        
        cursor.execute(f"SELECT COUNT(*) as total FROM {nombre_tabla}")
        total = cursor.fetchone()['total']
        
        cursor.execute(f"""
            SELECT * FROM {nombre_tabla} 
            ORDER BY numero 
            LIMIT %s OFFSET %s
        """, (per_page, offset))
        registros = cursor.fetchall()
        
        # Obtener columnas
        cursor.execute(f"DESCRIBE {nombre_tabla}")
        columnas = [col['Field'] for col in cursor.fetchall()]
        
        total_pages = (total + per_page - 1) // per_page
        
        # Obtener información de la tabla
        tabla_info = {
            'definitivos': {'nombre': 'Definitivos', 'descripcion': 'Contribuyentes con situación definitiva'},
            'desvirtuados': {'nombre': 'Desvirtuados', 'descripcion': 'Contribuyentes desvirtuados del padrón'},
            'presuntos': {'nombre': 'Presuntos', 'descripcion': 'Contribuyentes presuntos en el padrón'},
            'sentenciasfavorables': {'nombre': 'Sentencias Favorables', 'descripcion': 'Sentencias favorables a contribuyentes'},
            'listado_completo_69_b': {'nombre': 'Listado Completo 69-B', 'descripcion': 'Listado completo del artículo 69-B'}
        }.get(nombre_tabla.lower(), {'nombre': nombre_tabla, 'descripcion': ''})
        
        cursor.close()
        conn.close()
        
        return render_template('tabla_detalle.html',
                             tabla=nombre_tabla,
                             tabla_info=tabla_info,
                             registros=registros,
                             columnas=columnas,
                             page=page,
                             total_pages=total_pages,
                             total=total)
    
    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

@app.route('/exportar/<nombre_tabla>')
def exportar_tabla(nombre_tabla):
    """Exportar datos de una tabla específica"""
    conn = get_db_connection()
    if not conn:
        return "Error de conexión a la base de datos", 500
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Validar que la tabla existe
        tablas_validas = ['definitivos', 'desvirtuados', 'presuntos', 'sentenciasfavorables', 'listado_completo_69_b']
        if nombre_tabla.lower() not in tablas_validas:
            return "Tabla no válida", 400
        
        # Obtener todos los registros
        cursor.execute(f"SELECT * FROM {nombre_tabla} ORDER BY numero")
        registros = cursor.fetchall()
        
        # Generar CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Escribir encabezados
        if registros:
            writer.writerow(registros[0].keys())
        
        # Escribir datos
        for registro in registros:
            writer.writerow(registro.values())
        
        output.seek(0)
        
        cursor.close()
        conn.close()
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{nombre_tabla}_{datetime.now().strftime('%Y%m%d')}.csv"
        )
    
    except Exception as e:
        cursor.close()
        conn.close()
        return f"Error: {e}", 500

if __name__ == '__main__':
    print("✅ Iniciando servidor Flask...")
    print("✅ Sistema SAT Web Interface")
    print("✅ Nueva función: Carga Masiva de RFCs")
    print("🌐 http://localhost:8091")
    print("🌐 http://213.210.13.85:8091")
    print("📁 Ruta de uploads:", app.config['UPLOAD_FOLDER'])
    app.run(host="0.0.0.0", port=8091, debug=True)
