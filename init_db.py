#!/usr/bin/env python3
"""
Script profesional de inicialización de base de datos SAT
Carga el Listado Completo 69-B y separa automáticamente en:
- Definitivos
- Desvirtuados
- Presuntos
- Sentencias Favorables
Además llena la tabla Listado_Completo_69_B
"""

import pandas as pd
import mysql.connector
import traceback
from datetime import datetime
from config import DB_CONFIG

# ---------------------------------------------------------
# Mapeo de columnas del CSV → columnas de la base de datos
# ---------------------------------------------------------

COLUMN_MAP = {
    "No.": "numero",
    "RFC": "rfc",
    "Nombre del Contribuyente": "nombre_contribuyente",
    "Situación del contribuyente": "situacion_contribuyente",
    "Situaci�n del contribuyente": "situacion_contribuyente",

    "Número y fecha de oficio global de presunción SAT": "oficio_presuncion_sat",
    "Publicación página SAT presuntos": "publicacion_sat_presuntos",
    "Número y fecha de oficio global de presunción DOF": "oficio_presuncion_dof",
    "Publicación DOF presuntos": "publicacion_dof_presuntos",

    "Número y fecha de oficio global de contribuyentes que desvirtuaron SAT": "oficio_desvirtuado_sat",
    "Publicación página SAT desvirtuados": "publicacion_sat_desvirtuados",
    "Número y fecha de oficio global de contribuyentes que desvirtuaron DOF": "oficio_desvirtuado_dof",
    "Publicación DOF desvirtuados": "publicacion_dof_desvirtuados",

    "Número y fecha de oficio global de definitivos SAT": "oficio_definitivo_sat",
    "Publicación página SAT definitivos": "publicacion_sat_definitivos",
    "Número y fecha de oficio global de definitivos DOF": "oficio_definitivo_dof",
    "Publicación DOF definitivos": "publicacion_dof_definitivos",

    "Número y fecha de oficio global de sentencia favorable SAT": "oficio_sentencia_sat",
    "Publicación página SAT sentencia favorable": "publicacion_sat_sentencia",
    "Número y fecha de oficio global de sentencia favorable DOF": "oficio_sentencia_dof",
    "Publicación DOF sentencia favorable": "publicacion_dof_sentencia",
}

# ---------------------------------------------------------
# Conexión a la base de datos
# ---------------------------------------------------------

def conectar_db():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Exception as e:
        print("❌ Error conectando a la base de datos:", e)
        exit(1)

# ---------------------------------------------------------
# Limpieza de fechas
# ---------------------------------------------------------

def parse_fecha(valor):
    if pd.isna(valor):
        return None
    try:
        return datetime.strptime(str(valor), "%d/%m/%Y").date()
    except:
        return None

# ---------------------------------------------------------
# Inserción en tabla
# ---------------------------------------------------------

def insertar_en_tabla(tabla, registros):
    if not registros:
        return 0

    conn = conectar_db()
    cursor = conn.cursor()

    columnas = registros[0].keys()
    placeholders = ", ".join(["%s"] * len(columnas))
    columnas_sql = ", ".join(columnas)

    query = f"INSERT INTO {tabla} ({columnas_sql}) VALUES ({placeholders})"

    valores = [tuple(r.values()) for r in registros]

    cursor.executemany(query, valores)
    conn.commit()

    total = cursor.rowcount

    cursor.close()
    conn.close()

    print(f"✅ Insertados {total} registros en {tabla}")
    return total

# ---------------------------------------------------------
# Proceso principal
# ---------------------------------------------------------

def main():
    print("\n🚀 INICIALIZACIÓN DE BASE DE DATOS SAT")
    print("--------------------------------------")

    # Cargar CSV principal
    df = pd.read_csv(
        "data/Listado_Completo_69-B.csv",
        encoding="latin1",
        skiprows=2,
        on_bad_lines="skip"
    )

    # Renombrar columnas
    df = df.rename(columns=COLUMN_MAP)

    # Limpiar columnas desconocidas
    df = df[[c for c in df.columns if c in COLUMN_MAP.values()]]

    # Limpiar fechas
    for col in df.columns:
        if "publicacion" in col:
            df[col] = df[col].apply(parse_fecha)

    # Convertir NaN → None
    df = df.where(pd.notnull(df), None)

    # Convertir a diccionarios
    registros = df.to_dict(orient="records")

    # Insertar en tabla completa
    insertar_en_tabla("Listado_Completo_69_B", registros)

    # Separar por tipo
    tipos = {
        "Definitivo": "Definitivos",
        "Desvirtuado": "Desvirtuados",
        "Presunto": "Presuntos",
        "Sentencia Favorable": "SentenciasFavorables",
    }

    for tipo, tabla in tipos.items():
        subset = [r for r in registros if r.get("situacion_contribuyente") == tipo]
        insertar_en_tabla(tabla, subset)

    print("\n✅ PROCESO COMPLETADO")

if __name__ == "__main__":
    main()
