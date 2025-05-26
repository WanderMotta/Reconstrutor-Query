import sqlparse
import re
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

# Configuração de logging
logging.basicConfig(level=logging.DEBUG)

def parse_sql_query(query: str) -> List[Dict[str, Any]]:
    """
    Analisa uma consulta SQL e extrai as condições da cláusula WHERE.
    
    Args:
        query: String contendo a consulta SQL
    
    Returns:
        Lista de dicionários contendo informações sobre cada condição
    """
    try:
        # Normaliza a consulta (remove comentários, espaços extras, etc.)
        formatted_query = sqlparse.format(query, strip_comments=False, reindent=False)
        
        # Parse a consulta
        parsed = sqlparse.parse(formatted_query)
        
        if not parsed:
            logging.error("Não foi possível fazer o parsing da consulta")
            return []
            
        # Obtém a primeira declaração
        stmt = parsed[0]
        
        # Procura a cláusula WHERE usando a API do sqlparse
        where_tokens = []
        
        # Função recursiva para encontrar tokens WHERE em qualquer nível
        def find_where_tokens(token_list):
            found_tokens = []
            for token in token_list:
                if isinstance(token, sqlparse.sql.Where):
                    found_tokens.append(token)
                if hasattr(token, 'tokens'):
                    found_tokens.extend(find_where_tokens(token.tokens))
            return found_tokens
        
        # Procura cláusulas WHERE em qualquer nível da consulta
        where_tokens = find_where_tokens(stmt.tokens)
        
        if not where_tokens:
            logging.warning("Cláusula WHERE não encontrada na consulta")
            return []
            
        # Pega o primeiro token WHERE encontrado
        where_token = where_tokens[0]
        
        # Extrai a cláusula WHERE sem a palavra-chave WHERE
        where_clause = ''.join(str(t) for t in where_token.tokens[1:])
        
        # Extrai as condições
        return extract_conditions(where_clause, formatted_query)
    except Exception as e:
        logging.error(f"Erro ao analisar a consulta SQL: {str(e)}")
        return []

def extract_conditions(where_clause: str, full_query: str) -> List[Dict[str, Any]]:
    """
    Extrai condições individuais da cláusula WHERE.
    
    Args:
        where_clause: String contendo a cláusula WHERE
        full_query: String contendo a consulta SQL completa
    
    Returns:
        Lista de dicionários contendo informações sobre cada condição
    """
    conditions = []
    
    # Remove a palavra-chave WHERE se estiver presente no início
    where_clause = re.sub(r'^\s*WHERE\s+', '', where_clause, flags=re.IGNORECASE)
    
    # Antes de dividir em AND/OR, verifica se há um padrão especial de BETWEEN com TO_DATE
    to_date_pattern = r'([^\s]+)\s+(?:NOT\s+)?BETWEEN\s+to_date\s*\(\s*([^,\)]+)\s*,\s*\'([^\']+)\'\s*\)\s+AND\s+to_date\s*\(\s*([^,\)]+)\s*,\s*\'([^\']+)\'\s*\)'
    to_date_match = re.search(to_date_pattern, where_clause, re.IGNORECASE)
    
    if to_date_match:
        field = to_date_match.group(1).strip()
        start_param = to_date_match.group(2).strip()
        start_format = to_date_match.group(3).strip()
        end_param = to_date_match.group(4).strip()
        end_format = to_date_match.group(5).strip()
        
        condition_text = to_date_match.group(0)
        
        # Verifica se há NOT antes de BETWEEN
        is_not_between = 'NOT' in condition_text.upper().split('BETWEEN')[0]
        
        condition = {
            'id': 0,
            'field': field,
            'operator': 'NOT BETWEEN' if is_not_between else 'BETWEEN',
            'operator_desc': 'não entre' if is_not_between else 'entre',
            'value': [
                f"to_date({start_param}, '{start_format}')",
                f"to_date({end_param}, '{end_format}')"
            ],
            'original_value': [start_param, end_param],
            'type': 'date',
            'is_function': True,
            'function_name': 'to_date',
            'format': start_format
        }
        
        conditions.append(condition)
        
        # Divide o resto da cláusula WHERE pela condição que acabamos de processar
        remaining_clause = where_clause.replace(condition_text, '')
        if remaining_clause.strip():
            # Se houver mais condições, processá-las recursivamente
            if remaining_clause.strip().startswith('AND '):
                remaining_clause = remaining_clause.strip()[4:]  # Remove o "AND " do início
            elif remaining_clause.strip().startswith('OR '):
                remaining_clause = remaining_clause.strip()[3:]  # Remove o "OR " do início
                
            more_conditions = extract_conditions(remaining_clause, full_query)
            for i, cond in enumerate(more_conditions):
                cond['id'] = i + 1  # Atualize os IDs para continuarem a sequência
                conditions.append(cond)
    else:
        # Análise básica de condições AND e OR quando não há padrão especial
        and_parts = re.split(r'\sAND\s', where_clause, flags=re.IGNORECASE)
        
        condition_id = 0
        for and_part in and_parts:
            or_parts = re.split(r'\sOR\s', and_part, flags=re.IGNORECASE)
            for or_part in or_parts:
                condition = or_part.strip()
                if condition:
                    # Análise de diferentes tipos de operadores
                    parsed_condition = parse_condition(condition, condition_id)
                    if parsed_condition:
                        conditions.append(parsed_condition)
                        condition_id += 1
    
    return conditions

def parse_condition(condition: str, condition_id: int) -> Optional[Dict[str, Any]]:
    """
    Analisa uma condição individual e extrai campo, operador e valor.
    
    Args:
        condition: String contendo uma condição
        condition_id: Identificador único para a condição
    
    Returns:
        Dicionário contendo informações sobre a condição
    """
    # Lista de operadores suportados
    operators = {
        '=': 'igual',
        '!=': 'diferente',
        '<>': 'diferente',
        '>': 'maior',
        '<': 'menor',
        '>=': 'maior ou igual',
        '<=': 'menor ou igual',
        'LIKE': 'contém',
        'NOT LIKE': 'não contém',
        'IN': 'em',
        'NOT IN': 'não em',
        'BETWEEN': 'entre',
        'NOT BETWEEN': 'não entre',
        'IS NULL': 'é nulo',
        'IS NOT NULL': 'não é nulo'
    }
    
    # Caso especial para atendime.hr_atendimento between to_date(:data_inicio,'YYYY-MM-DD') and to_date(:data_fim,'YYYY-MM-DD')
    special_between_pattern = r'([^\s]+)\s+(?:NOT\s+)?BETWEEN\s+to_date\s*\(\s*([^,\)]+)\s*,\s*\'([^\']+)\'\s*\)\s+AND\s+to_date\s*\(\s*([^,\)]+)\s*,\s*\'([^\']+)\'\s*\)'
    special_between_match = re.match(special_between_pattern, condition, re.IGNORECASE)
    if special_between_match:
        field = special_between_match.group(1).strip()
        start_param = special_between_match.group(2).strip()
        start_format = special_between_match.group(3).strip()
        end_param = special_between_match.group(4).strip()
        end_format = special_between_match.group(5).strip()
        
        # Verifica se há NOT antes de BETWEEN
        is_not_between = 'NOT' in condition.upper().split('BETWEEN')[0]
        
        return {
            'id': condition_id,
            'field': field,
            'operator': 'NOT BETWEEN' if is_not_between else 'BETWEEN',
            'operator_desc': 'não entre' if is_not_between else 'entre',
            'value': [
                f"to_date({start_param}, '{start_format}')",
                f"to_date({end_param}, '{end_format}')"
            ],
            'original_value': [start_param, end_param],
            'type': 'date',
            'is_function': True,
            'function_name': 'to_date',
            'format': start_format  # Assumindo que os dois formatos são iguais
        }
    
    # Padrões de regex para diferentes tipos de condições
    patterns = [
        # BETWEEN normal
        (r'([^\s]+)\s+(?:NOT\s+)?BETWEEN\s+(.*?)\s+AND\s+(.*)', lambda m: {
            'id': condition_id,
            'field': m.group(1).strip(),
            'operator': 'NOT BETWEEN' if 'NOT' in m.group(0).upper() else 'BETWEEN',
            'value': [m.group(2).strip().strip("'\""), m.group(3).strip().strip("'\"")],
            'type': detect_value_type(m.group(2).strip().strip("'\""))
        }),
        # IN
        (r'([^\s]+)\s+(?:NOT\s+)?IN\s*\((.*)\)', lambda m: {
            'id': condition_id,
            'field': m.group(1).strip(),
            'operator': 'NOT IN' if 'NOT' in m.group(0).upper() else 'IN',
            'value': [v.strip().strip("'\"") for v in m.group(2).split(',')],
            'type': detect_value_type(m.group(2).split(',')[0].strip().strip("'\""))
        }),
        # IS NULL / IS NOT NULL
        (r'([^\s]+)\s+IS\s+(?:NOT\s+)?NULL', lambda m: {
            'id': condition_id,
            'field': m.group(1).strip(),
            'operator': 'IS NOT NULL' if 'NOT' in m.group(0).upper() else 'IS NULL',
            'value': None,
            'type': 'null'
        }),
        # Operadores padrão
        (r'([^\s]+)\s+(=|!=|<>|>|<|>=|<=|LIKE|NOT\s+LIKE)\s+(.*)', lambda m: {
            'id': condition_id,
            'field': m.group(1).strip(),
            'operator': m.group(2).strip().upper(),
            'value': m.group(3).strip().strip("'\""),
            'type': detect_value_type(m.group(3).strip().strip("'\""))
        }, re.IGNORECASE),
    ]
    
    for pattern, handler, *flags in patterns:
        match = re.match(pattern, condition, *flags) if flags else re.match(pattern, condition)
        if match:
            result = handler(match)
            # Adiciona a descrição amigável do operador
            result['operator_desc'] = operators.get(result['operator'].upper(), result['operator'])
            return result
    
    logging.warning(f"Não foi possível analisar a condição: {condition}")
    return None

def detect_value_type(value: str) -> str:
    """
    Detecta o tipo de um valor.
    
    Args:
        value: String contendo o valor a ser analisado
    
    Returns:
        String representando o tipo do valor ('number', 'date', 'text', 'null')
    """
    if value is None:
        return 'null'
    
    # Tenta converter para número
    try:
        float(value)
        return 'number'
    except ValueError:
        pass
    
    # Tenta converter para data
    date_patterns = [
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%Y/%m/%d',
        '%d-%m-%Y',
        '%Y-%m-%d %H:%M:%S',
        '%d/%m/%Y %H:%M:%S'
    ]
    
    for pattern in date_patterns:
        try:
            datetime.strptime(value, pattern)
            return 'date'
        except ValueError:
            pass
    
    # Se não for número nem data, é texto
    return 'text'

def reconstruct_sql_query(original_query: str, modified_conditions: List[Dict[str, Any]]) -> str:
    """
    Reconstrói a consulta SQL substituindo as condições originais pelas modificadas.
    
    Args:
        original_query: String contendo a consulta SQL original
        modified_conditions: Lista de condições modificadas
    
    Returns:
        String contendo a consulta SQL reconstruída
    """
    try:
        # Método simplificado que substitui toda a consulta SQL
        # Encontramos as partes da consulta antes e depois do WHERE
        
        # Normaliza a consulta para identificar mais facilmente o WHERE
        formatted_query = sqlparse.format(original_query, strip_comments=True)
        
        # Verificamos se a consulta contém WHERE - usamos regex mais simples
        match = re.search(r'(?i)(.*)\s+WHERE\s+(.*)', formatted_query, re.DOTALL)
        
        if not match:
            # Se não encontrarmos WHERE, retornamos a consulta original
            logging.warning("Não foi possível identificar a cláusula WHERE na consulta")
            return original_query
            
        # Constrói a nova cláusula WHERE com as condições modificadas
        new_where_clause = construct_where_clause(modified_conditions)
        
        # Reconstrói a consulta
        before_where = match.group(1)  # Parte antes do WHERE
        
        # Com o regex simplificado, não há grupo 3, então não há after_where
        reconstructed = f"{before_where} WHERE {new_where_clause}"
        
        logging.debug(f"Consulta reconstruída: {reconstructed}")
        return reconstructed
        
    except Exception as e:
        logging.error(f"Erro ao reconstruir a consulta SQL: {str(e)}")
        # Em caso de erro, retorna a consulta original com as condições
        # substituídas de forma simples (fallback)
        
        try:
            # Constrói a nova cláusula WHERE com as condições modificadas
            new_where_clause = construct_where_clause(modified_conditions)
            
            # Faz uma substituição simples de tudo após o WHERE
            pattern = r'(?i)(.*\s+WHERE\s+).*?(\s+GROUP BY.*|\s+ORDER BY.*|\s+HAVING.*|\s+LIMIT.*|;|$)'
            if re.search(pattern, original_query):
                return re.sub(pattern, fr'\1{new_where_clause}\2', original_query)
            else:
                # Se não encontrarmos o padrão completo, tentamos apenas substituir após o WHERE
                pattern = r'(?i)(.*\s+WHERE\s+).*?($)'
                return re.sub(pattern, fr'\1{new_where_clause}\2', original_query)
                
        except Exception as inner_e:
            logging.error(f"Erro no método de fallback: {str(inner_e)}")
            return original_query

def construct_where_clause(conditions: List[Dict[str, Any]]) -> str:
    """
    Constrói uma cláusula WHERE a partir das condições fornecidas.
    
    Args:
        conditions: Lista de condições
    
    Returns:
        String contendo a cláusula WHERE construída
    """
    where_parts = []
    
    for condition in conditions:
        field = condition.get('field', '')
        operator = condition.get('operator', '')
        value = condition.get('value')
        value_type = condition.get('type', 'text')
        is_function = condition.get('is_function', False)
        
        # Debug para verificar como os valores estão chegando
        logging.debug(f"Processando condição: campo={field}, operador={operator}, valor={value}, tipo={value_type}")
        
        if operator.upper() in ('IS NULL', 'IS NOT NULL'):
            where_parts.append(f"{field} {operator}")
        elif operator.upper() in ('BETWEEN', 'NOT BETWEEN') and isinstance(value, list) and len(value) == 2:
            if is_function:
                # Para condições especiais como to_date()
                where_parts.append(f"{field} {operator} {value[0]} AND {value[1]}")
            else:
                # Para BETWEEN normal
                formatted_values = [format_value(v, value_type) for v in value]
                where_parts.append(f"{field} {operator} {formatted_values[0]} AND {formatted_values[1]}")
        elif operator.upper() in ('IN', 'NOT IN') and isinstance(value, list):
            formatted_values = [format_value(v, value_type) for v in value]
            where_parts.append(f"{field} {operator} ({', '.join(formatted_values)})")
        else:
            formatted_value = format_value(value, value_type)
            where_parts.append(f"{field} {operator} {formatted_value}")
    
    # Log da cláusula WHERE construída
    result = " AND ".join(where_parts)
    logging.debug(f"Cláusula WHERE construída: {result}")
    return result

def format_value(value: Any, value_type: str) -> str:
    """
    Formata um valor de acordo com seu tipo para uso em uma consulta SQL.
    
    Args:
        value: Valor a ser formatado
        value_type: Tipo do valor ('number', 'date', 'text', 'null')
    
    Returns:
        String contendo o valor formatado
    """
    if value is None:
        return 'NULL'
    
    # Loga o valor e tipo para debug    
    logging.debug(f"Formatando valor: {value} (tipo: {value_type})")
    
    # Verifica se o valor já tem formato especial (como funções SQL)
    if isinstance(value, str) and 'to_date(' in value.lower():
        return value
        
    if value_type == 'number':
        # Garante que números vazios sejam NULL
        if value == '' or value is None:
            return 'NULL'
        try:
            # Tenta converter para garantir que é um número válido
            float(value)
            return str(value)
        except (ValueError, TypeError):
            # Se não for um número válido, retorna como texto
            logging.warning(f"Valor '{value}' não é um número válido. Retornando como texto.")
            return f"'{value}'"
    elif value_type == 'date':
        # Tratamento especial para datas vazias
        if value == '' or value is None:
            return 'NULL'
        # Garante que a data sempre tenha aspas simples
        if "'" not in value:
            return f"'{value}'"
        return value
    else:  # text
        # Garante que o texto sempre tenha aspas simples
        # e escapa aspas simples existentes
        if isinstance(value, str):
            value = value.replace("'", "''")  # Escapa aspas simples
        return f"'{value}'"
