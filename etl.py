import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Numeric, DateTime, ForeignKey
from sqlalchemy.exc import ProgrammingError
import configparser
import secrets

# pegando os arquivos csv e criando DF;

clientes = pd.read_csv("olist_customers_dataset.csv")  # dimensao feita;
itens_pedidos = pd.read_csv("olist_order_items_dataset.csv")
pagamentos = pd.read_csv("olist_order_payments_dataset.csv")  # dimensao feita;
pedidos = pd.read_csv("olist_orders_dataset.csv")  # dimensao feita;
produtos_vendidos = pd.read_csv(
    "olist_products_dataset.csv")  # dimensao feita;
avaliacoes = pd.read_csv("olist_order_reviews_dataset.csv")

# CONFIGURANDO A DIM_PRODUTOS //

# dimensao produtos
dim_products = produtos_vendidos
# removendo valores duplicados;
dim_products = dim_products.drop_duplicates(subset=['product_id'])
# removendo colunas que não entrarão na minha dimensao para análise;
dim_products = dim_products.drop(columns=['product_name_lenght', 'product_description_lenght',
                                 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'])
# ARMAZENANDO OS VALORES NULOS DA TABELA "dim_produtos" EM OUTRO DF;
products_null = produtos_vendidos[produtos_vendidos.isnull().any(axis=1)]
# removendo valores nulos;
dim_products = dim_products.dropna()

# CONFIGURANDO A DIM_PEDIDOS //

# dimensao pedidos
dim_orders = pedidos
# removendo os valores duplicados
dim_orders = dim_orders.drop_duplicates(subset=['order_id'])
# removendo colunas que não entraram na minha dimensão para análise;
dim_orders = dim_orders.drop(columns=['order_status', 'order_id', 'order_estimated_delivery_date',
                             'order_delivered_customer_date', 'order_delivered_carrier_date', 'order_approved_at'])
# removendo os valores nulos;
dim_orders = dim_orders.dropna()

# CONFIGURANDO A DIM_CLIENTES //

# dimensao clientes;
dim_customers = clientes
# removendo valores nulos;
dim_customers = dim_customers.dropna()
# removendo valores duplicados;
dim_customers = dim_customers.drop_duplicates(subset=["customer_unique_id"])
# removendo a coluna customer_id  pois não irei usar nessa dimensão;
dim_customers = dim_customers.drop(columns=['customer_id'])

# CONFIGURANDO A FATO_PAYMENTS //

# copy() resolve problema com atribuição simples... o DataFrame pagamentos permanece inalterado após modificações em fato_payments;
fato_payments = pagamentos.copy()
# A explicação e o código abaixo foi uma ideia interessante para criar PK's no padrão do documento.
# CRIANDO UM ID UNICO PARA PODER REFERENCIAR NA MINHA TABELA FATO; USANDO A BIBLIOTECA SECRETS fato_payments['payments_id'] = [secrets.token_hex(16) for _ in range(len(fato_payments))]
# fato_payments['payments_id'].apply(lambda x: x[:32]) FAZENDO UMA FUNÇÃO LAMBDA PARA QUE A CHAVE TENHA UM TOTAL DE 32DIGITOS, PARA SEGUIR O PADRÃO DOS ID's;

# Somando o valor final de pagamento e agrupando pelo tipo de pagamento e armazenando em outra coluna;
fato_payments['value_type_payment'] = fato_payments.groupby(
    ['order_id', 'payment_type'])['payment_value'].transform('sum')
# Somando a quantidade de vezes que o pagamento foi feito de acordo com o tipo de pagamento e armazenando em outra coluna;
fato_payments['payment_sequential_per_type'] = fato_payments.groupby(
    ['order_id', 'payment_type'])['payment_sequential'].transform('count')
# Excluindo colunas de payment_value e payment_sequential pois não serão mais usada;
fato_payments = fato_payments.drop(
    columns=['payment_sequential', 'payment_value'])
# Excluindo as informações duplicadas pois elas foram agrupadas;
fato_payments = fato_payments.drop_duplicates(
    subset=['order_id', 'payment_type'])

# Unindo o DF de pedidos com a fato_sales para criação da tabela posteiormente;
fato_payments = pd.merge(fato_payments, pedidos, on='order_id', how='left')
# Excluindo colunas que não estarão na minha análise;
fato_payments = fato_payments.drop(columns=['order_status', 'order_purchase_timestamp', 'order_approved_at',
                                   'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'])

# Unindo a tabela de clientes à fato;
fato_payments = pd.merge(fato_payments, clientes, on='customer_id', how='left')
# Removendo as colunas de acordo com a modelagem;
fato_payments = fato_payments.drop(
    columns=['customer_zip_code_prefix', 'customer_city', 'customer_state'])

# Unnido a tabela de itens_pedidos à fato;
fato_payments = pd.merge(fato_payments, itens_pedidos,
                         on='order_id', how='left')
# Removendo as colunas de acordo com a modelagem;
fato_payments = fato_payments.drop(columns=[
                                   'order_item_id', 'seller_id', 'shipping_limit_date', 'price', 'freight_value'])
# Excluindo as informações duplicadas pois elas foram agrupadas;
fato_payments = fato_payments.drop_duplicates()

# Unindo a tabela de itens_pedidos à fato;
fato_payments = pd.merge(
    fato_payments, produtos_vendidos, on='product_id', how='left')
# Removendo as colunas de acordo com a modelagem;
fato_payments = fato_payments.drop(columns=['product_name_lenght', 'product_description_lenght',
                                   'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'])

# Armazendo os dados nulos da fato_payments para criação da tabela de inconsistência;
payments_null = fato_payments[fato_payments.isnull().any(axis=1)]

# Removendo os dados nulos;
fato_payments = fato_payments.dropna()
# Removendo as colunas que não usarei na análise;
fato_payments = fato_payments.drop(columns=['product_category_name'])

# CRIANDO UM NOVO ID para referenciar na fato e não ter problema com o ID duplicado;
fato_payments['payments_id'] = fato_payments['payments_id'] = range(
    1, len(fato_payments) + 1)

# CONFIGURANDO A FATO_SALES //

# Iniciando a tabela FATO com a junção das tabelas pedidos e pagamentos;
fato_sales = pd.merge(pedidos, pagamentos, on='order_id', how='left')
# Excluindo colunas que não usarei na minha análise;
fato_sales = fato_sales.drop(columns=['order_status', 'order_purchase_timestamp', 'order_approved_at',
                             'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date'])
# Criação da coluna paid_value que é o valor total do pedido;
fato_sales['paid_value'] = fato_sales.groupby(
    ['order_id'])['payment_value'].transform('sum')
# Excluindo colunas que não usarei na minha análise;
fato_sales = fato_sales.drop(columns=[
                             'payment_sequential', 'payment_type', 'payment_value', 'payment_installments'])
# Removendo linhas duplicadas
fato_sales = fato_sales.drop_duplicates()

# Unindo a fato com a tabela de itens_pedidos;
fato_sales = pd.merge(fato_sales, itens_pedidos, on='order_id', how='left')
# Excluindo colunas que não usarei na minha análise;
fato_sales = fato_sales.drop(
    columns=['seller_id', 'shipping_limit_date', 'order_item_id'])
# Criando uma coluna com o valor final do produto por pedido;
fato_sales['price_total'] = fato_sales.groupby(['order_id', 'product_id'])[
    'price'].transform('sum')
# Criando uma coluna com o valor final do frete por pedido;
fato_sales['freight_total'] = fato_sales.groupby(['order_id', 'product_id'])[
    'freight_value'].transform('sum')
# Contando a quantidade produtos por pedido e armazenando esse valor em outra coluna; decisão minha para evitar a quantidade de linhas que teria com a coluna "order_item_id" que é a sequencia que um produto se repete no pedido, agrupei todos os produtos e deixei a quantidade final;
fato_sales['itens_per_order'] = fato_sales.groupby(['order_id', 'product_id'])[
    'product_id'].transform('count')
# Excluindo as linhas duplicadas;
fato_sales = fato_sales.drop_duplicates(subset=['product_id', 'order_id'])

# Unido a tabela de avaliacoes à fato;
fato_sales = pd.merge(fato_sales, avaliacoes, on='order_id', how='left')
# Excluindo colunas que não usarei na minha análise;
fato_sales = fato_sales.drop(columns=[
                             'review_answer_timestamp', 'review_creation_date', 'review_comment_message', 'review_comment_title'])

# Unindo a tabela de produtos à fato;
fato_sales = pd.merge(fato_sales, produtos_vendidos,
                      on='product_id', how='left')
# Excluindo colunas que não usarei na minha análise;
fato_sales = fato_sales.drop(columns=['product_name_lenght', 'product_description_lenght', 'product_photos_qty',
                             'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'])

# Unido a tabela de clientes à fato;
fato_sales = pd.merge(fato_sales, clientes, on='customer_id', how='left')
# Removendo as colunas de acordo com a modelagem;
fato_sales = fato_sales.drop(
    columns=['customer_zip_code_prefix', 'customer_city', 'customer_state'])

# Armazendo os dados nulos da fato_sales para criação da tabela de inconsistência;
sales_null = fato_sales[fato_sales.isnull().any(axis=1)]

# Excluindo dados nulos
fato_sales = fato_sales.dropna()
# Excluindo colunas que não usarei na minha análise;
fato_sales = fato_sales.drop(columns=['product_category_name'])

# CRIANDO UM NOVO ID para referenciar na fato e não ter problema com o ID duplicado;
fato_sales['sales_id'] = fato_sales['sales_id'] = range(1, len(fato_sales) + 1)

# TABELAS DE INCONSISTENCIA

# ESSA SERIA UMA ALTERNATIVA PARA LEVAR ESSAS INFORMAÇÕES. MUDAR OS PRODUTOS SEM NOME PARA "DESCONHECIDO"
products_null = products_null.copy()
products_null.loc[:, 'product_category_name'] = products_null['product_category_name'].fillna(
    'Desconhecido')
# Excluindo colunas que não usarei na minha análise;
products_null = products_null.drop(columns=['product_name_lenght', 'product_description_lenght',
                                   'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'])
# CRIANDO UM NOVO ID para referenciar na fato e não ter problema com o ID duplicado;
products_null['products_null_id'] = products_null['products_null_id'] = range(
    1, len(products_null) + 1)

# CRIANDO UM NOVO ID para referenciar na fato e não ter problema com o ID duplicado;
sales_null['sales_null_id'] = sales_null['sales_null_id'] = range(
    1, len(sales_null) + 1)

# CRIANDO UM NOVO ID para referenciar na fato e não ter problema com o ID duplicado;
payments_null['payments_null_id'] = payments_null['payments_null_id'] = range(
    1, len(payments_null) + 1)

# CONEXÃO COM O BANCO DE DADOS

# função para ler o arquivo database.ini onde armazena as credenciais;


def read_db_config(filename='database.ini', section='postgresql'):
    parser = configparser.ConfigParser()
    parser.read(filename)

    db_conf = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_conf[param[0]] = param[1]
    return db_conf


db_conf = read_db_config()

# URL de conexão ao postgres usando o create_engine e usando as credenciais no database.ini (adiantando as melhores praticas de conexão)
engine_url = f"postgresql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/postgres"
engine = create_engine(engine_url)

# instanciando e nomeando o BD que vou usar;
db_name = 'desafio_sqlalchemy'

# CONEXÃO E CRIAÇÃO DO NOVO BANCO DE DADOS;
# a utilização do With permite que a conexão seja fechada automaticamente; então não preciso usar o conn.close() dessa forma;
with engine.connect() as conn:
    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
    try:
        conn.execute(f"CREATE DATABASE {db_name}")
        print(f"Banco de dados '{db_name}' criado com sucesso!")
    except ProgrammingError as e:
        print(f"Erro ao criar o banco de dados: {e}")
    # finally:   #DEIXANDO ESSE PARTE DO CÓDIGO PARA LEMBRAR DESSA OPÇÃO;
        # if conn is not None:
        # print(f"Conexão com o banco de dados '{db_name}' fechada!")
        # conn.close()

# criando uma nova url de conexão agora para acessar o novo banco de dados;
new_engine_url = f"postgresql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_name}"
new_engine = create_engine(new_engine_url)

# Criar tabelas no novo banco de dados
metadata = MetaData()

# Definição das tabelas
tabela_clientes = Table('dim_customers', metadata,
                        Column('customer_unique_id',
                               String(33), primary_key=True),
                        Column('customer_zip_code_prefix', Integer),
                        Column('customer_city', String(50)),
                        Column('customer_state', String(5))
                        )

tabela_pagamentos = Table('fato_payments', metadata,
                          Column('payments_id', String(10), primary_key=True),
                          Column('order_id', String(33), primary_key=True),
                          Column('customer_unique_id', String(33), ForeignKey(
                              'dim_customers.customer_unique_id')),
                          Column('customer_id', String(33),
                                 ForeignKey('dim_orders.customer_id')),
                          Column('product_id', String(33), ForeignKey(
                              'dim_products.product_id')),
                          Column('payment_type', String(50)),
                          Column('value_type_payment', Numeric(10, 2)),
                          Column('payment_installments', String(10)),
                          Column('payment_sequential_per_type', Integer)
                          )

tabela_pedidos = Table('dim_orders', metadata,
                       Column('customer_id', String(33), primary_key=True),
                       Column('order_purchase_timestamp', DateTime)
                       )

tabela_products = Table('dim_products', metadata,
                        Column('product_id', String(33), primary_key=True),
                        Column('product_category_name', String(50))
                        )

tabela_fato = Table('fato_sales', metadata,
                    Column('sales_id', String(33), primary_key=True),
                    Column('order_id', String(33), primary_key=True),
                    Column('review_id', String(33), primary_key=True),
                    Column('customer_id', String(33),
                           ForeignKey('dim_orders.customer_id')),
                    Column('customer_unique_id', String(33), ForeignKey(
                        'dim_customers.customer_unique_id')),
                    Column('product_id', String(33), ForeignKey(
                        'dim_products.product_id')),
                    Column('price', Numeric(10, 2)),
                    Column('freight_value', Numeric(10, 2)),
                    Column('itens_per_order', Integer),
                    Column('price_total', Numeric(10, 2)),
                    Column('freight_total', Numeric(10, 2)),
                    Column('paid_value', Numeric(10, 2)),
                    Column('review_score', String(10))
                    )
tabela_inco_prod = Table('products_null', metadata,
                         Column('products_null_id', String(
                             33), primary_key=True),
                         Column('product_id', String(33)),
                         Column('product_category_name', String(50))
                         )

tabela_inco_sales = Table('sales_null', metadata,
                          Column('sales_null_id', String(
                              33), primary_key=True),
                          Column('order_id', String(33)),
                          Column('review_id', String(33)),
                          Column('customer_id', String(33)),
                          Column('customer_unique_id', String(33)),
                          Column('product_id', String(33)),
                          Column('price', Numeric(10, 2)),
                          Column('freight_value', Numeric(10, 2)),
                          Column('price_total', Numeric(10, 2)),
                          Column('freight_total', Numeric(10, 2)),
                          Column('itens_per_order', Integer),
                          Column('paid_value', Numeric(10, 2)),
                          Column('review_score', String(10)),
                          Column('product_category_name', String(50))
                          )

tabela_inco_payments = Table('payments_null', metadata,
                             Column('payments_null_id', String(
                                 33), primary_key=True),
                             Column('order_id', String(33)),
                             Column('customer_id', String(33)),
                             Column('customer_unique_id', String(33)),
                             Column('product_id', String(33)),
                             Column('payment_installments', String(10)),
                             Column('payment_type', String(33)),
                             Column('value_type_payment', Numeric(10, 2)),
                             Column('product_category_name', String(50)),
                             Column('payment_sequential_per_type', Integer)
                             )

# Conectar ao novo banco de dados, criar as tabelas e alimentar com os dataframes;
with new_engine.connect() as conn:
    try:
        metadata.create_all(new_engine)
        print("Tabelas criadas com sucesso!")
        dim_customers.to_sql('dim_customers', con=new_engine,
                             if_exists='append', index=False)
        dim_orders.to_sql('dim_orders', con=new_engine,
                          if_exists='append', index=False)
        dim_products.to_sql('dim_products', con=new_engine,
                            if_exists='append', index=False)
        fato_payments.to_sql('fato_payments', con=new_engine,
                             if_exists='append', index=False)
        fato_sales.to_sql('fato_sales', con=new_engine,
                          if_exists='append', index=False)
        products_null.to_sql('products_null', con=new_engine,
                             if_exists='append', index=False)
        sales_null.to_sql('sales_null', con=new_engine,
                          if_exists='append', index=False)
        payments_null.to_sql('payments_null', con=new_engine,
                             if_exists='append', index=False)
        # LEMBRETE FALTA CRIAR AS TABELAS COM OS DADOS NULOS
        print("Dados inseridos com sucesso!")
    except ProgrammingError as err:
        print(f"Erro ao inserir dados na tabela {tabela_clientes}: {err}")

engine.dispose()
new_engine.dispose()
