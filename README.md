# Desafio Airflow

Este desafio possui como objetivo a orquestração de dados no Airflow. Para isso, deve-se instalar o Airflow e subir uma DAG com três tasks. Aliás, sugere-se usar o Linux ou um ambiente remoto do Linux (WSL) disponível no Windows, pois o Windows não dá suporte para todos os pacotes que devem ser instalados, resultando na busca dos substitutos, o que aumenta a dificuldade na resolução do desafio.

## Passo 1 - Preparando o ambiente para instalação do Airflow

No VS Code, abra um terminal, crie a pasta **airflow_tooltorial** e abra-a. Basta escrever:

```
mkdir airflow_tooltorial
cd airflow_tooltorial
```

Feito isso, você criará um ambiente virtual do python e também um arquivo **.env** para criar a variável de ambiente necessária para rodar o Airflow.

```
python3 -m venv venv
touch .env
```

No arquivo **.env**, você exportará o Airflow_home. 

```
export AIRFLOW_HOME=/home/nome_do_usuário/airflow_tooltorial/
```

**Atenção:** Você deve adicionar o seu path da pasta airflow_tooltorial. Para encontrá-lo, você pode clicar com o botão direito no arquivo .env, copiar o caminho e colá-lo, apagando os itens após a última /.

Agora, você precisará ativar a **venv** e o **.env**. Para isso, você deve digitar no terminal:

```
source venv/bin/activate
source .env
```

## Passo 2 - Instalando o Airflow

Para instalar o Airflow, você precisa teclar no terminal:

```
AIRFLOW_VERSION=2.2.3
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Feita a instalação, você dará o comando para inicializar o banco de dados, criar um usuário, gerar uma senha e demais componentes.

```
airflow standalone
```

Se tudo der certo, visitando o **localhost:8080** no navegador, você deve conseguir acessar o Airflow realizando o login. O username é **admin** e a senha você identifica enquanto estiver rodando o **airflow standalone** ou a partir do arquivo **standalone_admin_password.txt**.

## Passo 3 - Criando a DAG com as três tasks

Crie uma pasta **dags** e com ela aberta, crie um arquivo .py para desenvolver a DAG.

```
mkdir dags
touch tarefas.py
```

Em **tarefas.py** comece a desenvolver a DAG. Faça as importações da classe DAG, dos operators e das bibliotecas necessárias.

```
from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import csv
import pandas as pd
import base64
```

Crie os argumentos que serão passados para cada operador.

```
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

Elabore as tasks. A primeira tarefa é ler a tabela **Order** do banco de dados **Northwind_small.sqlite** e a transformar em um aquivo .csv. Para isso, basta escrever:

```
def task1():

  conn = sqlite3.connect("/home/nome_do_usuário/airflow_tooltorial/data/Northwind_small.sqlite")
  cursor = conn.cursor()
  cursor.execute("""SELECT * FROM "Order";
  """)
  with open("output_orders.csv", 'w', newline='') as csv_file: 
      csv_writer = csv.writer(csv_file)
      csv_writer.writerow([i[0] for i in cursor.description]) 
      csv_writer.writerows(cursor)
  conn.close() 
```

**Atenção:** Para se conectar ao banco de dados da Northwind, você deve colocar o seu path do arquivo. Clique com o botão direito do mouse no banco de dados, copie o caminho e cole em ```conn = sqlite3.connect("seu_path_do_banco_de_dados")```.

A segunda tarefa é ler os dados da tabela **OrdersDetail** e realizar um ```join``` com o arquivo **output_orders.csv** criado. Além de gerar um arquivo **count.txt** com a soma da quantidade vendida com destino ao Rio de Janeiro. Use a função ```str()``` para converter o número em texto. Segue o código:

```
def task2():

    p1 = pd.read_csv("./output_orders.csv")
    conn = sqlite3.connect("/home/nome_do_usuário/airflow_tooltorial/data/Northwind_small.sqlite")
    p2 = pd.read_sql_query("select OrderId, Quantity from OrderDetail", conn)
    p3 = pd.merge(p1, p2, how='left', left_on='Id', right_on='OrderId')
    p4 = p3[p3['ShipCity'] == 'Rio de JANEIRO']
    p4['RioId'] = p4.index
    total = p3[p3['ShipCity']=='Rio de Janeiro']['Quantity'].sum()

    total_orders = open("count.txt", 'w')
    total_orders.write(str(total))
    conn.close() 
```

**Atenção:** Para se conectar ao banco de dados da Northwind, você deve colocar o seu path do arquivo. Clique com o botão direito do mouse no banco de dados, copie o caminho e cole em ```conn = sqlite3.connect("seu_path_do_banco_de_dados")```.

Já a tarefa 3 é a criação de uma variável no Airflow e a geração de um arquivo **final_output.txt** que conterá um texto codificado gerado automaticamente. O código a ser utilizado é:

```
def task3():
    
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
```

Para criar a variável, você deve acessar a plataforma do Airflow, ir em **Admin** --> **Variables** e clicar em **+**. No campo **Key**, escreva ```my_email``` e no **value** o seu e-mail. Clique em **Save**.

Após isso, você deve criar o objeto DAG do Airflow onde se define parâmetros para a respectiva DAG, como o identificador exclusivo, a data que ela começará a ser agendada, o intervalo de tempo que ela é acionada, dentre outros.

```
with DAG(
    'AtividadePrática',
    default_args=default_args,
    description='Atividade Prática - Módulo V',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
```

Então, você precisa adicionar as tarefas, escolhendo o operador.

```
    task1 = PythonOperator(
        task_id='transform_in_csv',
        python_callable=task1,
        provide_context=True
    )
    task2 = PythonOperator(
        task_id='orders_number',
        python_callable=task2,
        provide_context=True
    )

    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=task3,
        provide_context=True
    )
```

Por fim, você deve ordenar a execução das tasks. Como o exemplo acima segue uma lógica e possui certa dependência, a ordem a ser usada é a **task1**, **task2** e a **task3**,  neste último, **export_final_output**. 

```
task1 >> task2 >> export_final_output
```

## Verificando a execução da DAG no Airflow e os resultados alcançados

Você pode ver se a DAG apareceu no Airflow no menu DAGs da página. Se não tiver aparecido, você deve ir em **Browse** --> **DAG runs** e ativar o **Auto-Refresh**. Assim, possivelmente ela aparecerá. Para verificar se ela está rodando conforme o planejado, após o tempo de execução da DAG, os quadradinhos ao lado de cada task devem estar verde escuro. Além disso, você deverá ter os arquivos: **output_orders.csv**, **count.txt** e **final_output.txt**.

## Apenas executando o projeto

Caso você queira apenas executar o projeto, você pode clonar este repositório e seguir até o item ```airflow standalone``` do Passo 2, tendo cuidado com os pontos de atenção dos Passos 1 e 3 (configuração de paths).