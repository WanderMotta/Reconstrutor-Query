from flask_wtf import FlaskForm
from wtforms import TextAreaField, FileField
from wtforms.validators import DataRequired

class SQLQueryForm(FlaskForm):
    """Formulário para entrada de consultas SQL."""
    query = TextAreaField('Consulta SQL', validators=[DataRequired()])
    sql_file = FileField('Arquivo SQL')
