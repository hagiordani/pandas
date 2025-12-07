#!/usr/bin/env python3
"""
Script de inicialización de base de datos SAT
Carga todos los CSV definidos en config.py
"""

import pandas as pd
import mysql.connector
import traceback
from config import DB_CONFIG, CSV_FILES, IMPORT_CONFIG

def conectar_db():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print("❌ Error conectando a la base de datos:", e)
        exit(1)

def cargar_csv(tabla, ruta_csv):
    print(f"\n📄 Procesando: {ruta_csv} → Tabla: {tabla}")

    try:
        df = pd.read_csv(
            ruta_csv,
            skiprows=IMPORT_CONFIG['skip_rows'],
            encoding='latin1',
            on_bad_lines='skip'
        )


        if df.empty:
            print("⚠️ CSV vacío, se omite.")
            return 0

        conn = conectar_db()
        cursor = conn.cursor(dictionary=True)

        # Obtener columnas válidas de la tabla
        cursor.execute(f"DESCRIBE {tabla}")
        columnas_tabla = [col['Field'] for col in cursor.fetchall()]

        # Filtrar columnas válidas
        columnas_validas = [c for c in df.columns if c in columnas_tabla]

        if not columnas_validas:
            print("❌ Ninguna columna válida coincide con la tabla.")
            return 0

        df = df[columnas_validas]
        df = df.where(pd.notnull(df), None)

        placeholders = ", ".join(["%s"] * len(columnas_validas))
        columnas_sql = ", ".join(columnas_validas)
        query = f"INSERT INTO {tabla} ({columnas_sql}) VALUES ({placeholders})"

        registros = df.values.tolist()

        cursor.executemany(query, registros)
        conn.commit()

        total = cursor.rowcount

        # Registrar en historial
        cursor.execute("""
            INSERT INTO Historial_Cargas (nombre_archivo, tabla, registros)
            VALUES (%s, %s, %s)
        """, (ruta_csv, tabla, total))
        conn.commit()

        cursor.close()
        conn.close()

        print(f"✅ {total} registros insertados en {tabla}")
        return total

    except Exception as e:
        print("❌ Error procesando CSV:", e)
        traceback.print_exc()
        return 0

def main():
    print("\n🚀 INICIALIZACIÓN DE BASE DE DATOS SAT")
    print("--------------------------------------")

    total_global = 0

    for tabla, ruta in CSV_FILES.items():
        total = cargar_csv(tabla, ruta)
        total_global += total

    print("\n✅ PROCESO COMPLETADO")
    print(f"📊 Total de registros insertados: {total_global}")

if __name__ == "__main__":
    main()
