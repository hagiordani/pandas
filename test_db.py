from config import DB_CONFIG
import mysql.connector

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    print("✅ Conexión exitosa a la base de datos")
    
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    
    print(f"✅ Tablas encontradas ({len(tables)}):")
    for table in tables:
        print(f"   - {list(table.values())[0]}")
    
    cursor.close()
    conn.close()
    
except mysql.connector.Error as e:
    print(f"❌ Error de conexión: {e}")
