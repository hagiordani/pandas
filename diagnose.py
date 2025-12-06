"""
Script de diagnóstico para el servidor Flask
"""

import socket
import subprocess
import os

def check_port(port=8091):
    """Verifica si el puerto está abierto"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    result = sock.connect_ex(('0.0.0.0', port))
    sock.close()
    return result == 0

def get_server_info():
    """Obtiene información del servidor"""
    info = {
        'hostname': socket.gethostname(),
        'ip': socket.gethostbyname(socket.gethostname()),
        'external_ip': subprocess.getoutput('curl -s ifconfig.me'),
        'python_version': subprocess.getoutput('python3 --version'),
        'flask_version': subprocess.getoutput('python3 -c "import flask; print(flask.__version__)"')
    }
    return info

def main():
    print("🔍 Diagnóstico del Servidor Flask")
    print("=" * 50)
    
    # Información del servidor
    info = get_server_info()
    print(f"Hostname: {info['hostname']}")
    print(f"IP Interna: {info['ip']}")
    print(f"IP Externa: {info['external_ip']}")
    print(f"Python: {info['python_version']}")
    print(f"Flask: {info['flask_version']}")
    
    # Verificar puerto
    print("\n🔌 Verificando puerto 8091...")
    if check_port(8091):
        print("✅ Puerto 8091 está escuchando")
    else:
        print("❌ Puerto 8091 NO está escuchando")
        
        # Verificar procesos
        print("\n📊 Procesos en ejecución:")
        processes = subprocess.getoutput('ps aux | grep -E "(flask|python.*app)" | grep -v grep')
        if processes:
            print(processes)
        else:
            print("No hay procesos Flask/Python ejecutándose")
    
    print("\n🌐 Prueba de conectividad:")
    print(f"Desde el servidor: curl http://localhost:8091")
    print(f"Desde externo: http://{info['external_ip']}:8091")
    
    # Verificar configuraciones de red
    print("\n📡 Configuración de red:")
    print("Interfaces de red:")
    print(subprocess.getoutput('ip addr show | grep "inet "'))

if __name__ == '__main__':
    main()
