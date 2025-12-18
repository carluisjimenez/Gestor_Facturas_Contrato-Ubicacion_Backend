from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename, safe_join
from flask_apscheduler import APScheduler
import pandas as pd
import pdfplumber
import os
import re
import shutil
import glob
import zipfile
import time

app = Flask(__name__)
CORS(app)

# Configuración de carpetas
BASE_TEMP_FOLDER = 'temp'
EXCEL_FOLDER = os.path.join(BASE_TEMP_FOLDER, 'excel')
PDF_FOLDER = os.path.join(BASE_TEMP_FOLDER, 'pdf')

# Crear carpetas si no existen
for folder in [EXCEL_FOLDER, PDF_FOLDER]:
    os.makedirs(folder, exist_ok=True)

# Configuración del Programador (Scheduler) para limpieza automática
class Config:
    SCHEDULER_API_ENABLED = True

app.config.from_object(Config())
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

# Almacenamiento global para el mapeo de contratos
CONTRACT_MAP = {}

def clear_folders():
    """Elimina todos los archivos de las carpetas temporales."""
    for folder in [EXCEL_FOLDER, PDF_FOLDER]:
        if os.path.exists(folder):
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f'Error al eliminar {file_path}. Motivo: {e}')
    
    # También eliminar archivos zip residuales en la raíz de temp
    for zip_file in glob.glob(os.path.join(BASE_TEMP_FOLDER, "*.zip")):
        try:
            os.remove(zip_file)
        except:
            pass

@scheduler.task('interval', id='cleanup_task', hours=1, misfire_grace_time=900)
def scheduled_cleanup():
    """Tarea programada para limpiar archivos antiguos (más de 1 hora) cada hora."""
    print("Iniciando limpieza automática de archivos antiguos...")
    now = time.time()
    for root, dirs, files in os.walk(BASE_TEMP_FOLDER):
        for f in files:
            file_path = os.path.join(root, f)
            if os.stat(file_path).st_mtime < now - 3600:
                try:
                    os.remove(file_path)
                    print(f"Eliminado por antigüedad: {f}")
                except Exception as e:
                    print(f"No se pudo eliminar {f}: {e}")

def extract_contract_from_excel(file_path):
    global CONTRACT_MAP
    CONTRACT_MAP = {}
    try:
        df = pd.read_excel(file_path, sheet_name='Dueños 3rd Party')
        target_ref = next((c for c in df.columns if 'referencia' in str(c).lower()), None)
        if not target_ref:
            return False, "No se encontró la columna 'Referencia' en la hoja 'Dueños 3rd Party'."

        count = 0
        for _, row in df.iterrows():
            ref = str(row[target_ref])
            match = re.search(r'Contrato\s*(\d+)/([^/]+)', ref, re.IGNORECASE)
            if match:
                contract_num = match.group(1).strip()
                ubicacion = match.group(2).strip()
                CONTRACT_MAP[contract_num] = ubicacion
                count += 1
        return True, f"Procesado exitosamente. {count} mapeos creados."
    except Exception as e:
        return False, str(e)

def extract_contract_from_pdf(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if len(pdf.pages) > 0:
                page = pdf.pages[0]
                text = page.extract_text()
                if text:
                    candidates = re.findall(r'\b\d{6}\b', text)
                    return list(set(candidates))
        return []
    except Exception:
        return []

@app.route('/api/upload_excel', methods=['POST'])
def upload_excel():
    if 'excel' not in request.files:
        return jsonify({'error': 'No se encontró el archivo'}), 400
    file = request.files['excel']
    if file.filename == '':
        return jsonify({'error': 'No se seleccionó ningún archivo'}), 400
    
    if file:
        clear_folders()
        filename = secure_filename(file.filename)
        filepath = os.path.join(EXCEL_FOLDER, filename)
        file.save(filepath)
        
        success, message = extract_contract_from_excel(filepath)
        if success:
            return jsonify({'message': message, 'filename': filename}), 200
        else:
            return jsonify({'error': message}), 400

@app.route('/api/download_excel', methods=['GET'])
def download_excel():
    files = os.listdir(EXCEL_FOLDER)
    if not files:
        return jsonify({'error': 'No hay archivo Excel cargado'}), 404
    filename = files[0]
    return send_from_directory(EXCEL_FOLDER, filename, as_attachment=True)

@app.route('/api/process_pdfs', methods=['POST'])
def process_pdfs():
    if not CONTRACT_MAP:
        return jsonify({'error': 'Por favor sube el archivo Excel primero.'}), 400
        
    uploaded_files = request.files.getlist('pdfs')
    if not uploaded_files:
        return jsonify({'error': 'Sin archivos'}), 400

    results = []
    
    def process_single_pdf(path, original_filename_for_display):
        candidates = list(set(extract_contract_from_pdf(path)))
        new_name = original_filename_for_display
        status = "No encontrado"
        ubicacion = ""
        found_contract = None
        
        # Intentar renombrar según contrato encontrado
        for contract_num in candidates:
            if contract_num in CONTRACT_MAP:
                ubicacion = CONTRACT_MAP[contract_num]
                base_name = os.path.splitext(original_filename_for_display)[0]
                new_name = f"{ubicacion} - {base_name}.pdf"
                new_name = re.sub(r'[\\/*?:"<>|]', "", new_name)
                status = "Renombrado"
                found_contract = contract_num
                break
        
        if status != "Renombrado" and candidates:
            status = f"Contratos {', '.join(candidates)} no están en Excel"
        elif status != "Renombrado":
            status = "No se detectaron contratos de 6 dígitos"

        # Asegurar que el archivo termine en la raíz de PDF_FOLDER
        final_filename = os.path.basename(new_name)
        target_path = os.path.join(PDF_FOLDER, final_filename)
        
        # Si el archivo está en una subcarpeta (como al extraer de ZIP) o se cambió el nombre
        if os.path.abspath(path) != os.path.abspath(target_path):
            if os.path.exists(target_path):
                # Generar nombre único si ya existe
                target_path = os.path.join(PDF_FOLDER, f"{int(time.time())}_{final_filename}")
            
            try:
                shutil.move(path, target_path)
                path = target_path
            except Exception as e:
                print(f"Error al mover archivo: {e}")

        return {
            'original_name': original_filename_for_display,
            'new_name': os.path.basename(path),
            'status': status,
            'contract': found_contract or "N/A",
            'ubicacion': ubicacion or "N/A"
        }

    for file in uploaded_files:
        if file.filename == '': continue
        filename = secure_filename(file.filename)
        filepath = os.path.join(PDF_FOLDER, filename)
        file.save(filepath)
        
        if filename.lower().endswith('.zip'):
            try:
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    # Carpeta temporal única para extracción
                    temp_id = f"extract_{int(time.time() * 1000)}"
                    extract_path = os.path.join(PDF_FOLDER, temp_id)
                    os.makedirs(extract_path, exist_ok=True)
                    zip_ref.extractall(extract_path)
                    
                    for root, dirs, files in os.walk(extract_path):
                        for f in files:
                            if f.lower().endswith('.pdf'):
                                # procesar y mover a la raíz de PDF_FOLDER
                                results.append(process_single_pdf(os.path.join(root, f), f))
                    
                    # Limpiar carpeta de extracción una vez movidos los archivos
                    shutil.rmtree(extract_path)
                    os.remove(filepath)
            except Exception as e:
                results.append({'original_name': filename, 'status': f"Error ZIP: {str(e)}", 'new_name': filename})
        
        elif filename.lower().endswith('.pdf'):
            results.append(process_single_pdf(filepath, filename))

    return jsonify({'results': results})

@app.route('/api/files', methods=['GET'])
def list_files():
    files = glob.glob(os.path.join(PDF_FOLDER, '**/*.pdf'), recursive=True)
    file_list = [{'name': os.path.basename(f)} for f in files]
    file_list.sort(key=lambda x: x['name'])
    return jsonify(file_list)

@app.route('/api/download/<path:filename>', methods=['GET'])
def download_file(filename):
    for root, dirs, files in os.walk(PDF_FOLDER):
        if filename in files:
            return send_from_directory(root, filename, as_attachment=not (request.args.get('preview', 'false').lower() == 'true'))
    return jsonify({'error': 'Archivo no encontrado'}), 404

@app.route('/api/delete_all', methods=['DELETE'])
def delete_all():
    clear_folders()
    return jsonify({'message': 'Todos los archivos eliminados'}), 200

@app.route('/api/download_all', methods=['GET'])
def download_all():
    zip_filename = "facturas_renombradas.zip"
    zip_path = os.path.join(BASE_TEMP_FOLDER, zip_filename)
    if os.path.exists(zip_path): os.remove(zip_path)
    try:
        with zipfile.ZipFile(zip_path, 'w') as zip_file:
            for root, dirs, files in os.walk(PDF_FOLDER):
                for file in files:
                    if file.lower().endswith('.pdf'):
                        zip_file.write(os.path.join(root, file), file)
        return send_from_directory(BASE_TEMP_FOLDER, zip_filename, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/rename', methods=['POST'])
def rename_file():
    data = request.get_json()
    old_name, new_name = data.get('old_name'), data.get('new_name')
    if not old_name or not new_name: return jsonify({'error': 'Faltan nombres'}), 400
    if not new_name.lower().endswith('.pdf'): new_name += '.pdf'
    for root, dirs, files in os.walk(PDF_FOLDER):
        if old_name in files:
            try:
                os.rename(os.path.join(root, old_name), os.path.join(root, new_name))
                return jsonify({'message': 'Renombrado exitosamente'}), 200
            except Exception as e: return jsonify({'error': str(e)}), 500
    return jsonify({'error': 'Archivo no encontrado'}), 404

@app.route('/api/delete/<filename>', methods=['DELETE'])
def delete_single_file(filename):
    for root, dirs, files in os.walk(PDF_FOLDER):
        if filename in files:
            try:
                os.remove(os.path.join(root, filename))
                return jsonify({'message': 'Archivo eliminado'}), 200
            except Exception as e: return jsonify({'error': str(e)}), 500
    return jsonify({'error': 'Archivo no encontrado'}), 404

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
