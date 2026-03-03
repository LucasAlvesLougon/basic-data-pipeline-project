from loguru import logger

from processamento_dados import Dados

path_json = "data_raw/dados_empresaA.json"
path_csv = "data_raw/dados_empresaB.csv"
path_dados_combinados = "data_processed/dados_combinados.csv"

# EXTRACT
logger.info("__Extraindo os dados__")
dados_empresa_a = Dados(
    path=path_json, tipo_dados="json"
)
logger.info(f"Nome das colunas empresa A {dados_empresa_a.nome_colunas}")
logger.info(f"Quantidade de linhas empresa A {dados_empresa_a.qtd_linhas}")

dados_empresa_b = Dados(
    path=path_csv, tipo_dados="csv"
)
logger.info(f"Nome das colunas empresa B {dados_empresa_b.nome_colunas}")
logger.info(f"Quantidade de linhas empresa B {dados_empresa_b.qtd_linhas}")

# TRANSFORM
logger.info("__Transformando os dados__")
key_mapping = {
    "Nome do Item": "Nome do Produto",
    "Classificação do Produto": "Categoria do Produto",
    "Valor em Reais (R$)": "Preço do Produto",
    "Quantidade em Estoque": "Quantidade em Estoque",
    "Nome da Loja": "Filial",
    "Data da Venda": "Data da Venda"
}

dados_empresa_b.rename_columns(key_mapping=key_mapping)
logger.info(f"Nome das colunas empresa B {dados_empresa_b.nome_colunas}")
logger.info(f"Quantidade de linhas empresa B {dados_empresa_b.qtd_linhas}")

dados_fusao = Dados.join(dados_a=dados_empresa_a, dados_b=dados_empresa_b)
logger.info(f"Quantidade de dados após fusão {dados_fusao.qtd_linhas}")

# LOAD
logger.info("__Carregando os dados para o arquivo final__")
dados_fusao.salvando_dados(path=path_dados_combinados)