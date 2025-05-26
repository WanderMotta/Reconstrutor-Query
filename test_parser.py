import json
import logging
from sql_parser import parse_sql_query, reconstruct_sql_query

# Configurar logging
logging.basicConfig(level=logging.DEBUG)

# Carregar o arquivo SQL
with open('teste_to_date.sql', 'r') as file:
    query = file.read()

# Analisar a consulta
conditions = parse_sql_query(query)

# Exibir as condições extraídas em formato JSON
print("Condições extraídas:")
print(json.dumps(conditions, indent=2, ensure_ascii=False))

# Reconstruir a consulta SQL
reconstructed_query = reconstruct_sql_query(query, conditions)

# Exibir a consulta reconstruída
print("\nConsulta reconstruída:")
print(reconstructed_query)