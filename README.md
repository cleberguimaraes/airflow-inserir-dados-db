Projeto de Inserção de Dados em um Banco de Dados

Este é um projeto de exemplo que demonstra como usar o Apache Airflow para extrair dados de um arquivo CSV e inserir esses dados em um banco de dados MariaDB. Pode ser em um DB da sua escolha, e a programação da task você pode difinir.

Com essa abordagem, é possível otimizar o processo de criação de rotinas para inserção automática de informações provenientes de fontes de dados CSV em um data warehouse. Isso resulta em uma gestão mais eficiente dos dados, garantindo a atualização contínua do repositório central, de forma automatizada.

Pré-requisitos

Antes de executar o projeto, certifique-se de ter as seguintes dependências instaladas:

- Apache Airflow (versão X.X.X)
- Python (versão 3.x)
- MySQL (versão X.X.X)

Instalação

1. Clone o repositório para o seu ambiente local, ou baixe o arquivo e salve no diretório "DAGS" do Apache Airflow.

2. Crie um ambiente virtual (opcional, mas recomendado):

3. Instale as dependências:

Configuração

Antes de executar o projeto, você precisará configurar o Apache Airflow e o banco de dados MySQL.

1. Configurando o Apache Airflow:
   - Verifique o arquivo `airflow.cfg` para definir as configurações adequadas, como a localização do banco de dados de metadados e o local da pasta de DAGs (diretório "DAGS").

2. Configurando o banco de dados MySQL:
   - Certifique-se de ter um servidor MySQL em execução e crie um banco de dados chamado 'python'.
   - No arquivo `insert_db.py`, atualize as configurações de conexão do banco de dados na função `load_data_to_db()`.

Executando o Projeto

1. Inicialize o banco de dados de metadados do Apache Airflow.

2. Inicie o webserver do Apache Airflow.

3. Em outra janela do terminal, inicie o scheduler do Apache Airflow.

4. Acesse o painel do Apache Airflow em seu navegador.

5. Ative o DAG `insert_db` para começar a execução do projeto.

Estrutura do Projeto

- `dags/insert_db.py`: Define o DAG do Apache Airflow para extrair dados do arquivo CSV e carregá-los no banco de dados.
- `dados/produtos.csv`: Arquivo CSV contendo os dados a serem inseridos no banco de dados. 
obs: esse local pode ser na rede ou em outro servidor é só trocar o endereço no código.

Licença

Este projeto é licenciado sob a licença...




