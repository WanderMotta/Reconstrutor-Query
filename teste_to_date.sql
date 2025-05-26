SELECT a.nome, b.codigo 
FROM tabela a 
INNER JOIN tabelab b ON a.id = b.id 
WHERE atendime.hr_atendimento between to_date(:data_inicio,'YYYY-MM-DD') and to_date(:data_fim,'YYYY-MM-DD')