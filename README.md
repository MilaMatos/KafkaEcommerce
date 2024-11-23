# Desafio Kafka e Spark Streaming

Este projeto tem como objetivo desenvolver uma solução de processamento de dados de e-commerce em tempo real, utilizando **Apache Kafka** para transmissão de dados, **Apache Spark Streaming** para processamento e **Apache Cassandra** para armazenamento dos dados processados. Toda a pipeline é implementada em Python usando **PySpark**.

## Descrição do Desafio

### Etapas do Desafio

1. **Instalar o Apache Kafka**: Configure o ambiente Kafka com o Zookeeper e crie um tópico específico chamado `ecommerce_sales` para transmitir os dados de vendas.

2. **Criar um Produtor de Dados de Vendas de E-commerce em Python**: Desenvolver um produtor que envia mensagens para o Kafka com dados de vendas de e-commerce. Cada mensagem contém:
   - **Order ID**: Identificador único do pedido.
   - **Client Document**: Documento de identificação do cliente.
   - **Products**: Lista de produtos contendo `product_name`, `quantity` e `price`.
   - **Total Value**: Valor total do pedido.
   - **Sale Datetime**: Data e hora da venda.

3. **Configurar o Apache Cassandra**: Instalar e configurar o Apache Cassandra. Criar um keyspace e uma tabela para armazenar os dados processados das vendas, projetados para permitir consultas eficientes.

4. **Criar um Consumidor Spark Streaming com PySpark**: Desenvolver um aplicativo PySpark que consome dados do Kafka, processa as mensagens e escreve os resultados no Cassandra. 
   - **Transformação**: Explode os produtos das vendas em registros individuais e agrega as vendas por produto antes de salvar no Cassandra.

---

## Estrutura do Projeto

- `producer.py`: Script Python que age como produtor de mensagens Kafka para dados de vendas de e-commerce.
- `pyspark_consumer.py`: Script PySpark que consome os dados do Kafka, processa e exibe os resultados no console.
- `pyspark_consumer_cassandra.py`: Script PySpark que consome dados do Kafka, processa e escreve os dados no Cassandra.
- `requirements.txt`: Arquivo contendo as dependências do projeto para configuração fácil do ambiente.
- `README.md`: Instruções e detalhes sobre o projeto.

---

## Pré-requisitos

- Python 3.10
- Apache Kafka e Zookeeper instalados e configurados
- Apache Cassandra instalado e configurado
- Apache Spark com PySpark
- Ambiente virtual configurado com as dependências do projeto

---

## Instruções de Execução

### 1. Configuração do Ambiente

1. **Clone este repositório**:
   ```bash
   git clone https://github.com/MilaMatos/KafkaEcommerce
   ```
2. **Crie e ative um ambiente virtual**:
   ```bash
   python3.10 -m venv venv
   source venv/bin/activate
   ```
3. **Instale as bibliotecas necessárias**:
   ```bash
   pip install -r requirements.txt
   ```

### 2. Inicializar Apache Kafka e Zookeeper

> **Note**: Certifique-se de que o Apache Kafka esteja instalado na sua máquina.

1. **Inicie o Zookeeper**:
   ```bash
   $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
   ```
2. **Em outro terminal, inicie o servidor Kafka**:
   ```bash
   $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
   ```
3. **Crie o tópico `ecommerce_sales`**:
   ```bash
   $KAFKA_HOME/bin/kafka-topics.sh --create --topic ecommerce_sales --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

### 3. Configurar Apache Cassandra

1. **Inicie o Cassandra**:
   ```bash
   sudo service cassandra start
   ```
2. **Acesse o Shell do Cassandra Query Language (cqlsh)**:
   ```bash
   cqlsh
   ```
3. **Crie o keyspace e a tabela**:
   ```sql
   CREATE KEYSPACE ecommerce WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

   USE ecommerce;

   CREATE TABLE ecommerce.product_sales (
    product_name text PRIMARY KEY,
    total_sales decimal);
   ```

### 4. Execute o Produtor de Dados de Vendas e o Consumidor Spark Streaming

1. **Execute o script  `producer.py` para gerar e enviar dados simulados de vendas ao Kafka**:
   ```bash
   python3 producer.py
   ```
2. **Execute o consumidor PySpark com os conectores do Cassandra e Kafka para processar as mensagens**:
   ```bash
   spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark_consumer_cassandra.py
   ```
   > **Note**: Certifique-se de que as versões dos conectores são compatíveis com suas versões do Spark e Scala.

### 5. Verifique os Dados no Cassandra

1. **Acesse o `cqlsh`**:
   ```bash
   cqlsh
   ```
2. **Faça uma consulta na tabela de vendas**:
   ```sql
   USE ecommerce;

   SELECT * FROM product_sales;
   ```
   Você verá os dados de vendas enviados do Kafka e processados pelo Spark Streaming.

---

## Conclusão

Este projeto demonstra a construção de uma pipeline de processamento de dados em tempo real utilizando tecnologias amplamente usadas na indústria para processar e analisar dados de vendas de e-commerce.

---

## Notas Adicionais

- **Parar os Serviços**: Para parar qualquer serviço (Zookeeper, Kafka, Cassandra, Produtor ou Consumidor), pressione `Ctrl + C` no terminal onde o serviço está sendo executado.
- **Monitoramento**: Use a interface Spark UI (geralmente acessível em `http://localhost:4040`) tpara monitorar o aplicativo Spark Streaming.
- **Tratamento de Erros**: Implemente tratamento de erros e logs nos seus scripts para capturar e resolver possíveis problemas durante a execução.

---

## Dependências do Projeto  

O arquivo `requirements.txt` inclui as seguintes dependências:

```
Package           Version
----------------  -----------
cassandra-driver  3.29.2
click             8.1.7
Faker             30.8.1
geomet            0.2.1.post1
kafka-python      2.0.2
py4j              0.10.9.7
pyspark           3.5.3
python-dateutil   2.9.0.post0
six               1.16.0
typing_extensions 4.12.2
```

Certifique-se de que estas dependências estejam instaladas no seu ambiente virtual.

---

