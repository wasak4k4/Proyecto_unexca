from flask import Flask, request, redirect, render_template, session
from werkzeug.security import check_password_hash
import sqlite3
import os
import unicodedata

# Función para normalizar texto removiendo tildes/acentos
def normalizar_texto(texto):
    """Remove accents/tildes from text for normalization."""
    if not texto:
        return texto
    # Normalize to NFD (decomposed form), then filter out combining marks
    normalized = unicodedata.normalize('NFD', str(texto))
    return ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')

# Rutas base
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(BASE_DIR, "../frontend")
DB_NAME = os.path.join(BASE_DIR, "database.db")
STUDENTS_DB = os.path.join(BASE_DIR, "base_datos_estudiantes.db")

# Configuración de Flask
app = Flask(
    __name__,
    template_folder=FRONTEND_DIR,
    static_folder=FRONTEND_DIR,
    static_url_path=""
)

app.secret_key = "ClaveSecretaCambiarLuegoXD"

# ---------- FUNCIONES DE BASE DE DATOS ----------
def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def get_students_db():
    conn = sqlite3.connect(STUDENTS_DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()

def init_students_db():
    conn = sqlite3.connect(STUDENTS_DB)
    
    # Tabla principal de estudiantes
    conn.execute("""
    CREATE TABLE IF NOT EXISTS estudiantes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        apellido TEXT NOT NULL,
        fecha_nacimiento TEXT NOT NULL,
        telefono TEXT NOT NULL,
        correo TEXT NOT NULL,
        carrera TEXT NOT NULL,
        semestre INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Tabla de semestres cursados por estudiante
    conn.execute("""
    CREATE TABLE IF NOT EXISTS semesters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        semestre INTEGER NOT NULL,
        año INTEGER NOT NULL,
        estado TEXT DEFAULT 'activo',
        FOREIGN KEY (student_id) REFERENCES estudiantes(id)
    )
    """)
    
    # Tabla de materias por semestre
    conn.execute("""
    CREATE TABLE IF NOT EXISTS subjects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        semester_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        nota_final REAL,
        FOREIGN KEY (semester_id) REFERENCES semesters(id)
    )
    """)
    
    # Tabla de evaluaciones por materia
    conn.execute("""
    CREATE TABLE IF NOT EXISTS evaluations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subject_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        nota REAL NOT NULL,
        porcentaje INTEGER NOT NULL,
        FOREIGN KEY (subject_id) REFERENCES subjects(id)
    )
    """)
    
    conn.commit()
    conn.close()

init_db()
init_students_db()

# ---------- RUTAS ----------
@app.route("/")
def home():
    if "user_id" not in session:
        return redirect("/login.html")
    return render_template("index.html")

@app.route("/login.html")
def login_page():
    return render_template("login.html")

# ---------- LOGIN ----------
@app.route("/login", methods=["POST"])
def login():
    email = request.form["email"]
    password = request.form["password"]

    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE email = ?", (email,)
    ).fetchone()
    conn.close()

    if user and check_password_hash(user["password_hash"], password):
        session["user_id"] = user["id"]
        return redirect("/")

    return redirect("/login.html")

# ---------- REGISTRO DE ESTUDIANTES (MODIFICACIÓN PERTINENTE) ----------
@app.route("/estudiantes/registrar", methods=["POST"])
def registrar_estudiante():
    if "user_id" not in session:
        return redirect("/login.html")

    nombre = request.form["nombre"]
    apellido = request.form["apellido"]
    fecha_nacimiento = request.form["fecha_nacimiento"]
    telefono = request.form["telefono"]
    correo = request.form["correo"]
    carrera = request.form["carrera"]
    semestre = request.form["semestre"]

    conn = get_students_db()
    conn.execute("""
        INSERT INTO estudiantes (
            nombre, apellido, fecha_nacimiento,
            telefono, correo, carrera, semestre
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        nombre, apellido, fecha_nacimiento,
        telefono, correo, carrera, semestre
    ))
    conn.commit()
    conn.close()

    return redirect("/base_de_datos.html")

# ---------- API PARA OBTENER ESTUDIANTES ----------
@app.route("/api/estudiantes", methods=["GET"])
def obtener_estudiantes():
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401

    # Obtener parámetros de query
    limit = request.args.get('limit', type=int)
    recent = request.args.get('recent', 'false').lower() == 'true'

    conn = get_students_db()
    
    query = """
        SELECT id, nombre, apellido, fecha_nacimiento, 
               telefono, correo, carrera, semestre, created_at
        FROM estudiantes
        ORDER BY created_at DESC
    """
    
    if limit and limit > 0:
        query += f" LIMIT {limit}"
    
    estudiantes = conn.execute(query).fetchall()
    conn.close()

    # Convertir a lista de diccionarios
    resultado = []
    for est in estudiantes:
        resultado.append({
            "id": est["id"],
            "nombre": est["nombre"],
            "apellido": est["apellido"],
            "fecha_nacimiento": est["fecha_nacimiento"],
            "telefono": est["telefono"],
            "correo": est["correo"],
            "carrera": est["carrera"],
            "semestre": est["semestre"],
            "created_at": est["created_at"]
        })

    return {"estudiantes": resultado}

# ---------- RUTA PARA BASE DE DATOS ----------
@app.route("/base_de_datos.html")
def base_de_datos():
    if "user_id" not in session:
        return redirect("/login.html")
    return render_template("base_de_datos.html")

# ---------- API PARA OBTENER DETALLES DE ESTUDIANTE ----------
@app.route("/api/estudiantes/<int:student_id>/detalle", methods=["GET"])
def obtener_detalle_estudiante(student_id):
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401

    conn = get_students_db()
    
    # Obtener información básica del estudiante
    estudiante = conn.execute("""
        SELECT id, nombre, apellido, fecha_nacimiento, 
               telefono, correo, carrera
        FROM estudiantes
        WHERE id = ?
    """, (student_id,)).fetchone()
    
    if not estudiante:
        conn.close()
        return {"error": "Estudiante no encontrado"}, 404
    
    # Obtener semestres del estudiante
    semestres = conn.execute("""
        SELECT id, semestre, año, estado
        FROM semesters
        WHERE student_id = ?
        ORDER BY año DESC, semestre DESC
    """, (student_id,)).fetchall()
    
    resultado = {
        "estudiante": {
            "id": estudiante["id"],
            "nombre": estudiante["nombre"],
            "apellido": estudiante["apellido"],
            "fecha_nacimiento": estudiante["fecha_nacimiento"],
            "telefono": estudiante["telefono"],
            "correo": estudiante["correo"],
            "carrera": estudiante["carrera"]
        },
        "semestres": []
    }
    
    # Para cada semestre, obtener materias y evaluaciones
    for semestre in semestres:
        materias = conn.execute("""
            SELECT id, nombre, nota_final
            FROM subjects
            WHERE semester_id = ?
        """, (semestre["id"],)).fetchall()
        
        materias_list = []
        for materia in materias:
            evaluaciones = conn.execute("""
                SELECT nombre, nota, porcentaje
                FROM evaluations
                WHERE subject_id = ?
            """, (materia["id"],)).fetchall()
            
            evaluaciones_list = [
                {
                    "nombre": ev["nombre"],
                    "nota": ev["nota"],
                    "porcentaje": ev["porcentaje"]
                }
                for ev in evaluaciones
            ]
            
            materias_list.append({
                "nombre": materia["nombre"],
                "nota_final": materia["nota_final"],
                "evaluaciones": evaluaciones_list
            })
        
        resultado["semestres"].append({
            "semestre": semestre["semestre"],
            "año": semestre["año"],
            "estado": semestre["estado"],
            "materias": materias_list
        })
    
    conn.close()
    return resultado

# ---------- API PARA ACTUALIZAR ESTUDIANTE ----------
@app.route("/api/estudiantes/<int:student_id>", methods=["PUT"])
def actualizar_estudiante(student_id):
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401

    data = request.get_json()
    
    conn = get_students_db()
    
    # Verificar que el estudiante existe
    estudiante = conn.execute(
        "SELECT id FROM estudiantes WHERE id = ?", (student_id,)
    ).fetchone()
    
    if not estudiante:
        conn.close()
        return {"error": "Estudiante no encontrado"}, 404
    
    # Actualizar información del estudiante
    conn.execute("""
        UPDATE estudiantes 
        SET nombre = ?, apellido = ?, fecha_nacimiento = ?,
            telefono = ?, correo = ?, carrera = ?, semestre = ?
        WHERE id = ?
    """, (
        data.get("nombre"),
        data.get("apellido"),
        data.get("fecha_nacimiento"),
        data.get("telefono"),
        data.get("correo"),
        data.get("carrera"),
        data.get("semestre"),
        student_id
    ))
    
    conn.commit()
    conn.close()
    
    return {"success": True, "message": "Estudiante actualizado correctamente"}

# ---------- API PARA OBTENER MATERIAS DE UN ESTUDIANTE ----------
@app.route("/api/estudiantes/<int:student_id>/materias", methods=["GET"])
def obtener_materias_estudiante(student_id):
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401
    
    conn = get_students_db()
    
    # Obtener todas las materias del estudiante con su semestre
    materias = conn.execute("""
        SELECT s.id as subject_id, s.nombre as materia, sem.semestre, sem.año
        FROM subjects s
        JOIN semesters sem ON s.semester_id = sem.id
        WHERE sem.student_id = ?
        ORDER BY sem.año DESC, sem.semestre DESC, s.nombre
    """, (student_id,)).fetchall()
    
    conn.close()
    
    resultado = [
        {
            "id": m["subject_id"],
            "nombre": m["materia"],
            "semestre": m["semestre"],
            "año": m["año"]
        }
        for m in materias
    ]
    
    return {"materias": resultado}

# ---------- API PARA AGREGAR EVALUACIÓN ----------
@app.route("/api/evaluaciones", methods=["POST"])
def agregar_evaluacion():
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401
    
    data = request.get_json()
    
    subject_id = data.get("subject_id")
    nombre = data.get("nombre")
    nota = data.get("nota")
    porcentaje = data.get("porcentaje")
    
    if not all([subject_id, nombre, nota is not None, porcentaje]):
        return {"error": "Faltan campos requeridos"}, 400
    
    conn = get_students_db()
    
    # Verificar que la materia existe
    materia = conn.execute(
        "SELECT id FROM subjects WHERE id = ?", (subject_id,)
    ).fetchone()
    
    if not materia:
        conn.close()
        return {"error": "Materia no encontrada"}, 404
    
    # Insertar la nueva evaluación
    conn.execute("""
        INSERT INTO evaluations (subject_id, nombre, nota, porcentaje)
        VALUES (?, ?, ?, ?)
    """, (subject_id, nombre, nota, porcentaje))
    
    conn.commit()
    conn.close()
    
    return {"success": True, "message": "Evaluación agregada correctamente"}

# ---------- API PARA ELIMINAR ESTUDIANTE ----------
@app.route("/api/estudiantes/<int:student_id>", methods=["DELETE"])
def eliminar_estudiante(student_id):
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401
    
    conn = get_students_db()
    
    # Verificar que el estudiante existe
    estudiante = conn.execute(
        "SELECT id FROM estudiantes WHERE id = ?", (student_id,)
    ).fetchone()
    
    if not estudiante:
        conn.close()
        return {"error": "Estudiante no encontrado"}, 404
    
    # Obtener todos los semestres del estudiante
    semestres = conn.execute(
        "SELECT id FROM semesters WHERE student_id = ?", (student_id,)
    ).fetchall()
    
    # Para cada semestre, eliminar materias y sus evaluaciones
    for semestre in semestres:
        materias = conn.execute(
            "SELECT id FROM subjects WHERE semester_id = ?", (semestre["id"],)
        ).fetchall()
        
        for materia in materias:
            # Eliminar evaluaciones de la materia
            conn.execute("DELETE FROM evaluations WHERE subject_id = ?", (materia["id"],))
        
        # Eliminar materias del semestre
        conn.execute("DELETE FROM subjects WHERE semester_id = ?", (semestre["id"],))
    
    # Eliminar semestres del estudiante
    conn.execute("DELETE FROM semesters WHERE student_id = ?", (student_id,))
    
    # Finalmente eliminar el estudiante
    conn.execute("DELETE FROM estudiantes WHERE id = ?", (student_id,))
    
    conn.commit()
    conn.close()
    
    return {"success": True, "message": "Estudiante eliminado correctamente"}

# ---------- API PARA ESTADÍSTICAS DEL DASHBOARD ----------
@app.route("/api/estadisticas", methods=["GET"])
def obtener_estadisticas():
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401

    conn = get_students_db()
    
    # 1. Total de estudiantes
    total_estudiantes = conn.execute(
        "SELECT COUNT(*) as total FROM estudiantes"
    ).fetchone()["total"]
    
    # 2. Estudiantes por carrera
    estudiantes_por_carrera = conn.execute("""
        SELECT carrera, COUNT(*) as cantidad 
        FROM estudiantes 
        GROUP BY carrera 
        ORDER BY cantidad DESC
    """).fetchall()
    
    # 3. Estudiantes por semestre
    estudiantes_por_semestre = conn.execute("""
        SELECT semestre, COUNT(*) as cantidad 
        FROM estudiantes 
        GROUP BY semestre 
        ORDER BY semestre
    """).fetchall()
    
    # 4. Promedio general por carrera
    promedio_por_carrera = conn.execute("""
        SELECT e.carrera, AVG(s.nota_final) as promedio
        FROM estudiantes e
        LEFT JOIN semesters sem ON e.id = sem.student_id
        LEFT JOIN subjects s ON sem.id = s.semester_id
        WHERE s.nota_final IS NOT NULL
        GROUP BY e.carrera
    """).fetchall()
    
    # 5. Materias con promedios más bajos (más difíciles)
    materias_dificiles = conn.execute("""
        SELECT s.nombre, AVG(s.nota_final) as promedio, COUNT(*) as estudiantes
        FROM subjects s
        WHERE s.nota_final IS NOT NULL
        GROUP BY s.nombre
        HAVING COUNT(*) >= 1
        ORDER BY promedio ASC
        LIMIT 5
    """).fetchall()
    
    # 6. Distribución de notas (rangos)
    distribucion_notas = conn.execute("""
        SELECT 
            CASE 
                WHEN nota_final >= 18 THEN 'Excelente (18-20)'
                WHEN nota_final >= 15 THEN 'Bueno (15-17)'
                WHEN nota_final >= 10 THEN 'Aprobado (10-14)'
                ELSE 'Reprobado (0-9)'
            END as rango,
            COUNT(*) as cantidad
        FROM subjects
        WHERE nota_final IS NOT NULL
        GROUP BY rango
        ORDER BY nota_final DESC
    """).fetchall()
    
    # 7. Total de materias registradas
    total_materias = conn.execute(
        "SELECT COUNT(*) as total FROM subjects"
    ).fetchone()["total"]
    
    # 8. Total de evaluaciones
    total_evaluaciones = conn.execute(
        "SELECT COUNT(*) as total FROM evaluations"
    ).fetchone()["total"]
    
    # 9. Promedio general del sistema
    promedio_general = conn.execute(
        "SELECT AVG(nota_final) as promedio FROM subjects WHERE nota_final IS NOT NULL"
    ).fetchone()["promedio"]
    
    conn.close()
    
    return {
        "total_estudiantes": total_estudiantes,
        "total_materias": total_materias,
        "total_evaluaciones": total_evaluaciones,
        "promedio_general": round(promedio_general, 2) if promedio_general else 0,
        "estudiantes_por_carrera": [
            {"carrera": row["carrera"], "cantidad": row["cantidad"]}
            for row in estudiantes_por_carrera
        ],
        "estudiantes_por_semestre": [
            {"semestre": row["semestre"], "cantidad": row["cantidad"]}
            for row in estudiantes_por_semestre
        ],
        "promedio_por_carrera": [
            {"carrera": row["carrera"], "promedio": round(row["promedio"], 2) if row["promedio"] else 0}
            for row in promedio_por_carrera
        ],
        "materias_dificiles": [
            {"nombre": row["nombre"], "promedio": round(row["promedio"], 2), "estudiantes": row["estudiantes"]}
            for row in materias_dificiles
        ],
        "distribucion_notas": [
            {"rango": row["rango"], "cantidad": row["cantidad"]}
            for row in distribucion_notas
        ]
    }

# ---------- RUTA PARA MIGRACIÓN ----------
@app.route("/migracion.html")
def migracion():
    if "user_id" not in session:
        return redirect("/login.html")
    return render_template("migracion.html")

# ---------- API PARA PREVIEW DE ARCHIVO ----------
@app.route("/api/migracion/preview", methods=["POST"])
def preview_migracion():
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401
    
    if 'file' not in request.files:
        return {"error": "No se envió ningún archivo"}, 400
    
    file = request.files['file']
    file_type = request.form.get('type', 'csv')
    
    if file.filename == '':
        return {"error": "Nombre de archivo vacío"}, 400
    
    try:
        import pandas as pd
        import io
        
        # Leer según tipo de archivo
        if file_type == 'excel':
            df = pd.read_excel(file, engine='openpyxl')
        elif file_type == 'csv':
            # Intentar detectar el delimitador
            content = file.read().decode('utf-8', errors='replace')
            file.seek(0)
            
            # Detectar si es CSV o TXT con diferentes delimitadores
            if '\t' in content[:1000]:
                df = pd.read_csv(io.StringIO(content), sep='\t')
            elif ';' in content[:1000]:
                df = pd.read_csv(io.StringIO(content), sep=';')
            else:
                df = pd.read_csv(io.StringIO(content))
        elif file_type == 'dbf':
            # Para archivos DBF (FoxPro)
            try:
                from dbfread import DBF
                import tempfile
                import os
                
                # Guardar temporalmente el archivo
                tmp_path = None
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.dbf') as tmp:
                        tmp_path = tmp.name
                        file.save(tmp_path)
                    
                    # Leer el DBF después de cerrar el archivo temporal
                    table = DBF(tmp_path, encoding='latin-1')
                    df = pd.DataFrame(iter(table))
                finally:
                    # Asegurar que el archivo se elimine después de leerlo
                    if tmp_path and os.path.exists(tmp_path):
                        try:
                            os.unlink(tmp_path)
                        except:
                            pass  # Ignorar errores de eliminación en Windows
            except ImportError:
                return {"error": "Librería dbfread no instalada. Ejecuta: pip install dbfread"}, 400
        else:
            return {"error": "Tipo de archivo no soportado"}, 400
        
        # Limpiar nombres de columnas
        df.columns = [str(col).strip() for col in df.columns]
        
        # Convertir a formato JSON
        rows = df.head(100).to_dict(orient='records')  # Solo primeras 100 filas
        columns = list(df.columns)
        
        return {
            "columns": columns,
            "rows": rows,
            "total": len(df)
        }
        
    except Exception as e:
        return {"error": f"Error al procesar archivo: {str(e)}"}, 400

# ---------- API PARA EJECUTAR MIGRACIÓN ----------
@app.route("/api/migracion/ejecutar", methods=["POST"])
def ejecutar_migracion():
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401
    
    data = request.get_json()
    rows = data.get('data', [])
    mapping = data.get('mapping', {})
    options = data.get('options', {})
    
    success_count = 0
    skipped_count = 0
    error_count = 0
    
    conn = get_students_db()
    
    # Obtener correos existentes para detectar duplicados
    existing_emails = set()
    if options.get('skipDuplicates', True):
        result = conn.execute("SELECT correo FROM estudiantes").fetchall()
        existing_emails = {row['correo'].lower() for row in result}
    
    migration_mode = options.get('migrationMode', 'basic')
    
    if migration_mode == 'complete':
        # Modo completo: agrupar por estudiante y procesar datos académicos
        from collections import defaultdict
        
        students_data = defaultdict(lambda: {
            'info': None,
            'semesters': defaultdict(lambda: {
                'subjects': defaultdict(list)
            })
        })
        
        # Agrupar filas por estudiante (correo)
        for row in rows:
            try:
                correo = str(row.get(mapping.get('correo', ''), '')).strip().lower()
                if not correo:
                    continue
                
                # Extraer info del estudiante (solo la primera vez)
                if students_data[correo]['info'] is None:
                    nombre = row.get(mapping.get('nombre', ''), '').strip() if options.get('trimSpaces') else row.get(mapping.get('nombre', ''), '')
                    apellido = row.get(mapping.get('apellido', ''), '').strip() if options.get('trimSpaces') else row.get(mapping.get('apellido', ''), '')
                    fecha_nacimiento = str(row.get(mapping.get('fecha_nacimiento', ''), '')).strip()
                    telefono = str(row.get(mapping.get('telefono', ''), '')).strip()
                    carrera = normalizar_texto(row.get(mapping.get('carrera', ''), '').strip() if options.get('trimSpaces') else row.get(mapping.get('carrera', ''), ''))
                    semestre_num = row.get(mapping.get('semestre', ''), 1)
                    
                    try:
                        semestre_num = int(float(str(semestre_num)))
                    except:
                        semestre_num = 1
                    
                    students_data[correo]['info'] = {
                        'nombre': nombre,
                        'apellido': apellido,
                        'fecha_nacimiento': fecha_nacimiento,
                        'telefono': telefono,
                        'correo': correo,
                        'carrera': carrera,
                        'semestre': semestre_num
                    }
                
                # Extraer datos académicos si existen
                semestre_num = row.get(mapping.get('semestre', ''), 1)
                semestre_anio = row.get(mapping.get('semestre_anio', ''), 2024)
                materia = str(row.get(mapping.get('materia', ''), '')).strip()
                evaluacion = str(row.get(mapping.get('evaluacion', ''), '')).strip()
                nota = row.get(mapping.get('nota', ''), 0)
                porcentaje = row.get(mapping.get('porcentaje', ''), 0)
                
                try:
                    semestre_num = int(float(str(semestre_num)))
                except:
                    semestre_num = 1
                
                try:
                    semestre_anio = int(float(str(semestre_anio)))
                except:
                    semestre_anio = 2024
                
                try:
                    nota = float(str(nota))
                except:
                    nota = 0
                
                try:
                    porcentaje = int(float(str(porcentaje)))
                except:
                    porcentaje = 0
                
                if materia and evaluacion:
                    sem_key = (semestre_num, semestre_anio)
                    students_data[correo]['semesters'][sem_key]['subjects'][materia].append({
                        'evaluacion': evaluacion,
                        'nota': nota,
                        'porcentaje': porcentaje
                    })
            except Exception as e:
                error_count += 1
                continue
        
        # Insertar estudiantes con datos académicos
        for correo, data in students_data.items():
            try:
                info = data['info']
                if not info or not all([info['nombre'], info['apellido'], info['correo'], info['carrera']]):
                    error_count += 1
                    continue
                
                # Validar duplicados
                if options.get('skipDuplicates') and correo in existing_emails:
                    skipped_count += 1
                    continue
                
                # Insertar estudiante
                cursor = conn.execute("""
                    INSERT INTO estudiantes (
                        nombre, apellido, fecha_nacimiento,
                        telefono, correo, carrera, semestre
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (info['nombre'], info['apellido'], info['fecha_nacimiento'], 
                      info['telefono'], info['correo'], info['carrera'], info['semestre']))
                
                student_id = cursor.lastrowid
                
                # Insertar semestres, materias y evaluaciones
                for (sem_num, sem_anio), sem_data in data['semesters'].items():
                    # Insertar semestre
                    cursor = conn.execute("""
                        INSERT INTO semesters (student_id, semestre, año, estado)
                        VALUES (?, ?, ?, 'activo')
                    """, (student_id, sem_num, sem_anio))
                    semester_id = cursor.lastrowid
                    
                    for materia_nombre, evaluaciones in sem_data['subjects'].items():
                        # Calcular nota final
                        nota_final = 0
                        total_porcentaje = 0
                        for ev in evaluaciones:
                            nota_final += ev['nota'] * (ev['porcentaje'] / 100)
                            total_porcentaje += ev['porcentaje']
                        
                        if total_porcentaje > 0 and total_porcentaje != 100:
                            nota_final = (nota_final / total_porcentaje) * 100
                        
                        # Insertar materia
                        cursor = conn.execute("""
                            INSERT INTO subjects (semester_id, nombre, nota_final)
                            VALUES (?, ?, ?)
                        """, (semester_id, materia_nombre, round(nota_final, 2)))
                        subject_id = cursor.lastrowid
                        
                        # Insertar evaluaciones
                        for ev in evaluaciones:
                            conn.execute("""
                                INSERT INTO evaluations (subject_id, nombre, nota, porcentaje)
                                VALUES (?, ?, ?, ?)
                            """, (subject_id, ev['evaluacion'], ev['nota'], ev['porcentaje']))
                
                existing_emails.add(correo)
                success_count += 1
                
            except Exception as e:
                error_count += 1
                continue
    else:
        # Modo básico: solo estudiantes sin datos académicos
        for row in rows:
            try:
                # Extraer datos según mapeo
                nombre = row.get(mapping.get('nombre', ''), '').strip() if options.get('trimSpaces') else row.get(mapping.get('nombre', ''), '')
                apellido = row.get(mapping.get('apellido', ''), '').strip() if options.get('trimSpaces') else row.get(mapping.get('apellido', ''), '')
                fecha_nacimiento = str(row.get(mapping.get('fecha_nacimiento', ''), '')).strip()
                telefono = str(row.get(mapping.get('telefono', ''), '')).strip()
                correo = str(row.get(mapping.get('correo', ''), '')).strip().lower()
                carrera = row.get(mapping.get('carrera', ''), '').strip() if options.get('trimSpaces') else row.get(mapping.get('carrera', ''), '')
                semestre = row.get(mapping.get('semestre', ''), 1)
                
                # Validar campos requeridos
                if not all([nombre, apellido, correo, carrera]):
                    error_count += 1
                    continue
                
                # Validar duplicados
                if options.get('skipDuplicates') and correo in existing_emails:
                    skipped_count += 1
                    continue
                
                # Convertir semestre a entero
                try:
                    semestre = int(semestre)
                except (ValueError, TypeError):
                    semestre = 1
                
                # Insertar estudiante
                conn.execute("""
                    INSERT INTO estudiantes (
                        nombre, apellido, fecha_nacimiento,
                        telefono, correo, carrera, semestre
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (nombre, apellido, fecha_nacimiento, telefono, correo, carrera, semestre))
                
                existing_emails.add(correo)
                success_count += 1
                
            except Exception as e:
                error_count += 1
                continue
    
    conn.commit()
    
    # Guardar en historial
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS migration_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT NOT NULL,
                archivo TEXT NOT NULL,
                tipo TEXT NOT NULL,
                registros INTEGER NOT NULL,
                exitosos INTEGER NOT NULL,
                omitidos INTEGER NOT NULL,
                errores INTEGER NOT NULL,
                usuario TEXT NOT NULL
            )
        """)
        
        import datetime
        conn.execute("""
            INSERT INTO migration_history (fecha, archivo, tipo, registros, exitosos, omitidos, errores, usuario)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "Archivo importado",
            "excel",
            len(rows),
            success_count,
            skipped_count,
            error_count,
            "Dr. Roberto Sánchez"
        ))
        conn.commit()
    except:
        pass
    
    conn.close()
    
    return {
        "success": True,
        "total": len(rows),
        "success": success_count,
        "skipped": skipped_count,
        "errors": error_count
    }

# ---------- API PARA HISTORIAL DE MIGRACIONES ----------
@app.route("/api/migracion/historial", methods=["GET"])
def historial_migracion():
    if "user_id" not in session:
        return {"error": "No autorizado"}, 401
    
    conn = get_students_db()
    
    try:
        # Verificar si existe la tabla
        conn.execute("""
            CREATE TABLE IF NOT EXISTS migration_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT NOT NULL,
                archivo TEXT NOT NULL,
                tipo TEXT NOT NULL,
                registros INTEGER NOT NULL,
                exitosos INTEGER NOT NULL,
                omitidos INTEGER NOT NULL,
                errores INTEGER NOT NULL,
                usuario TEXT NOT NULL
            )
        """)
        
        historial = conn.execute("""
            SELECT fecha, archivo, tipo, registros, exitosos, omitidos, errores, usuario
            FROM migration_history
            ORDER BY id DESC
            LIMIT 10
        """).fetchall()
        
        conn.close()
        
        return {
            "historial": [
                {
                    "fecha": row["fecha"],
                    "archivo": row["archivo"],
                    "tipo": row["tipo"],
                    "registros": row["registros"],
                    "exitosos": row["exitosos"],
                    "omitidos": row["omitidos"],
                    "errores": row["errores"],
                    "usuario": row["usuario"]
                }
                for row in historial
            ]
        }
    except Exception as e:
        conn.close()
        return {"historial": []}

# ---------- LOGOUT ----------
@app.route("/logout")
def logout():
    session.pop("user_id", None)
    return redirect("/login.html")

# ---------- EJECUCIÓN ----------
if __name__ == "__main__":
    app.run(debug=True)
