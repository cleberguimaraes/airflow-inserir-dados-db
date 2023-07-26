from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from itertools import zip_longest
import csv
import mysql.connector
from itertools import zip_longest  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 17),
    'email': ['xxxxxxx@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'catchup': False
}

def extract_mysql_data():
    # Caminho para o arquivo CSV no mesmo servidor do Airflow
    caminho_arquivo_csv = '/home/dados/produtos.csv'

    # Lista para armazenar os dados do CSV
    dados_csv = []

    # Abre o arquivo CSV e lê os dados
    with open(caminho_arquivo_csv, "r") as arquivo_csv:
        leitor_csv = csv.reader(arquivo_csv)
        next(leitor_csv, None)  # Ignora o cabeçalho

        for linha in leitor_csv:
            dados_csv.append(linha)

    return dados_csv

def load_data_to_db(**context):
    dados_csv = context['task_instance'].xcom_pull(task_ids='extract_data_from_database')

    # Estabelece a conexão com o banco de dados
    conexao = mysql.connector.connect(
        host="192.168.xx.x",
        user="root",
        password="******",
        database="python"
    )

    # Cria o cursor para executar as operações no banco
    cursor = conexao.cursor()

    # Inicializa a variável para contar os registros inseridos
    registros_inseridos = 0

    for linha in dados_csv:
        # Substituir "sem informação" por None para o campo 'custo'
        linha = [valor if valor != "sem informação" else None for valor in linha]

        codigo, marca, tipo, categoria, preco_unitario, custo, obs = linha

    # Definir os nomes dos campos
    campos = ['codigo', 'marca', 'tipo', 'categoria', 'preco_unitario', 'custo', 'obs']

    for linha in dados_csv:
        # Verificar se há pelo menos cinco valores na linha (codigo, marca, tipo, categoria, preco_unitario)
        if len(linha) >= 5:
            
            # Preencher os campos ausentes com "sem informação" usando zip_longest
            linha_completa = list(zip_longest(campos, linha, fillvalue="sem informação"))

            

            # Desempacotar os campos
            codigo, marca, tipo, categoria, preco_unitario, custo, obs = [valor for campo, valor in linha_completa]

            # Resto do código...

            # Verifica se o registro já existe na tabela
            comando_sql_verifica = "SELECT codigo FROM cad_produto WHERE codigo = %s"
            cursor.execute(comando_sql_verifica, (codigo,))
            resultado = cursor.fetchone()

            if resultado:
                continue  # Pula para o próximo registro sem inserir no banco
            else:
                # Insere os dados na tabela cad_produto
                comando_sql_insercao = "INSERT INTO cad_produto (codigo, marca, tipo, categoria, preco_unitario, custo, obs) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                valores = (codigo, marca, tipo, categoria, preco_unitario, custo, obs)
                cursor.execute(comando_sql_insercao, valores)

                # Incrementa o contador de registros inseridos
                registros_inseridos += 1

    # Confirma a inserção dos dados no banco
    conexao.commit()

    # Fecha a conexão com o banco
    cursor.close()
    conexao.close()

    # Retorna a quantidade de registros inseridos
    return registros_inseridos

def notify_items_inserted(**context):
    ti = context['task_instance']
    registros_inseridos = ti.xcom_pull(task_ids='load_data_to_db')

    if registros_inseridos > 0:
        return 'items_inserted_task'
    else:
        return 'no_items_inserted_task'


def notify_no_items_inserted(**context):
    print("Nenhum item novo encontrado no arquivo CSV. Nenhum registro foi inserido no banco.")


with DAG('insert_db', default_args=default_args, schedule_interval='30 * * * *') as dag:
    task_extract_data_from_database = PythonOperator(
        task_id='extract_data_from_database',
        python_callable=extract_mysql_data
    )

    task_load_data_to_db = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_db,
        provide_context=True
    )

    task_notify_items_inserted = PythonOperator(
        task_id='items_inserted_task',
        python_callable=notify_items_inserted,
        provide_context=True
    )

    task_notify_no_items_inserted = PythonOperator(
        task_id='no_items_inserted_task',
        python_callable=notify_no_items_inserted,
        provide_context=True
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=notify_items_inserted,
        provide_context=True
    )

    # Define a ordem das tarefas na DAG
    
    task_extract_data_from_database >> task_load_data_to_db >> branch_task
    branch_task >> task_notify_items_inserted
    branch_task >> task_notify_no_items_inserted
