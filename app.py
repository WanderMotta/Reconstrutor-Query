import os
import logging
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
import tempfile
from sql_parser import parse_sql_query, reconstruct_sql_query
from forms import SQLQueryForm

# Configuração de logging
logging.basicConfig(level=logging.DEBUG)

# Inicialização da aplicação Flask
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "default_secret_key")

# Rotas da aplicação
@app.route('/', methods=['GET'])
def index():
    """Página inicial da aplicação."""
    form = SQLQueryForm()
    return render_template('index.html', form=form)

@app.route('/parse_query', methods=['POST'])
def parse_query():
    """Analisa a consulta SQL e extrai as condições WHERE."""
    try:
        query = request.form.get('query', '')
        if not query.strip():
            return jsonify({'error': 'Consulta SQL vazia. Por favor, forneça uma consulta válida.'}), 400
        
        # Analisar a consulta SQL
        parsed_conditions = parse_sql_query(query)
        
        # Verifica se existe uma cláusula WHERE na consulta
        if not parsed_conditions:
            return jsonify({
                'status': 'no_where',
                'query': query,
                'message': 'Não foi possível identificar condições WHERE na consulta.'
            })
            
        # Retorna as condições extraídas em formato JSON
        return jsonify({
            'status': 'success',
            'query': query,
            'conditions': parsed_conditions
        })
    except Exception as e:
        logging.error(f"Erro ao analisar consulta: {str(e)}")
        return jsonify({'error': f'Erro ao analisar a consulta: {str(e)}'}), 500

@app.route('/upload_sql', methods=['POST'])
def upload_sql():
    """Processa o upload de um arquivo SQL."""
    try:
        if 'sqlFile' not in request.files:
            return jsonify({'error': 'Nenhum arquivo selecionado'}), 400
            
        file = request.files['sqlFile']
        
        if file.filename == '':
            return jsonify({'error': 'Nenhum arquivo selecionado'}), 400
            
        if file and file.filename.endswith('.sql'):
            # Lê o conteúdo do arquivo
            sql_content = file.read().decode('utf-8')
            return jsonify({'status': 'success', 'query': sql_content})
        else:
            return jsonify({'error': 'Arquivo inválido. Por favor, envie um arquivo .sql'}), 400
    except Exception as e:
        logging.error(f"Erro no upload de arquivo: {str(e)}")
        return jsonify({'error': f'Erro no processamento do arquivo: {str(e)}'}), 500

@app.route('/reconstruct_query', methods=['POST'])
def reconstruct_query():
    """Reconstrói a consulta SQL com as condições modificadas."""
    try:
        data = request.json
        original_query = data.get('original_query', '')
        modified_conditions = data.get('modified_conditions', [])
        
        # Log detalhado para depuração
        logging.debug(f"Recebido para reconstrução - Consulta original: {original_query}")
        logging.debug(f"Condições modificadas: {modified_conditions}")
        
        if not original_query or not modified_conditions:
            return jsonify({'error': 'Dados incompletos para reconstrução da consulta'}), 400
            
        # Reconstruir a consulta SQL
        reconstructed_query = reconstruct_sql_query(original_query, modified_conditions)
        
        # Log da consulta reconstruída
        logging.debug(f"Consulta reconstruída: {reconstructed_query}")
        
        return jsonify({
            'status': 'success',
            'reconstructed_query': reconstructed_query
        })
    except Exception as e:
        logging.error(f"Erro na reconstrução da consulta: {str(e)}")
        # Exibe o traceback para facilitar a depuração
        import traceback
        logging.error(traceback.format_exc())
        return jsonify({'error': f'Erro ao reconstruir a consulta: {str(e)}'}), 500

@app.route('/download_sql', methods=['POST'])
def download_sql():
    """Gera um arquivo SQL para download."""
    try:
        data = request.json
        query = data.get('query', '')
        
        if not query:
            return jsonify({'error': 'Consulta vazia. Não é possível gerar o arquivo.'}), 400
            
        # Cria um arquivo temporário
        fd, path = tempfile.mkstemp(suffix='.sql')
        
        with os.fdopen(fd, 'w') as tmp:
            tmp.write(query)
            
        return send_file(path, as_attachment=True, download_name='consulta_modificada.sql', mimetype='text/plain')
    except Exception as e:
        logging.error(f"Erro ao gerar arquivo para download: {str(e)}")
        return jsonify({'error': f'Erro ao gerar o arquivo: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
