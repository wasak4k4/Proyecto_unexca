import sys
import os

# Agregamos la carpeta 'backend' al path del sistema para poder importar 'app'
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from app import app

if __name__ == "__main__":
    app.run(debug=True, port=5000)
