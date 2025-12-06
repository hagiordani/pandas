"""
Configuración centralizada para el sistema SAT
"""

# Configuración de conexión a la base de datos
DB_CONFIG = {
    "host": "mysql-sat",
    "user": "satuser",
    "password": "satpass",
    "database": "satdb"
}

# Rutas de archivos CSV - COMPLETO
CSV_FILES = {
    'ListadoGlobalDefinitivo': 'data/ListadoGlobalDefinitivo.csv',
    'Definitivos': 'data/Definitivos.csv',
    'Desvirtuados': 'data/Desvirtuados.csv',
    'Presuntos': 'data/Presuntos.csv',
    'SentenciasFavorables': 'data/SentenciasFavorables.csv',
    'Listado_Completo_69_B': 'data/Listado_Completo_69-B.csv'
}

# Configuración de importación - ACTUALIZADO
IMPORT_CONFIG = {
    'skip_rows': 2,
    'encoding': 'utf-8',
    'date_format': '%d/%m/%Y',
    'fechas_actualizacion': {
        'ListadoGlobalDefinitivo': '2025-06-13',
        'Definitivos': '2025-10-31',
        'Desvirtuados': '2025-10-31',
        'Presuntos': '2025-10-31',
        'SentenciasFavorables': '2025-10-31',
        'Listado_Completo_69_B': '2025-09-30'
    }
}

# Configuración de la base de datos
DB_SETTINGS = {
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci',
    'engine': 'InnoDB'
}

