# Documentação do Pipeline de ETL - Data Warehouse COVID-19 (ES)

Este documento descreve detalhadamente o processo de **Extração, Transformação e Carga (ETL)** implementado para alimentar o Data Warehouse epidemiológico no **Projeto 3**.

---

## 1. Fontes de Dados e Extração
A fonte de dados principal consiste no arquivo de microdados de notificações de COVID-19 do estado do Espírito Santo:
*   **Arquivo de Origem:** `Projeto1/MICRODADOS.parquet` (compactado e otimizado a partir do `MICRODADOS.csv` de 1,95 GB).
*   **Registros totais:** 5.189.950 linhas.
*   **Método de Extração:** Leitura colunar de alta performance utilizando a engine nativa do **DuckDB**. O carregamento do arquivo parquet para a tabela de staging (`stg.notificacao_raw`) é concluído em aproximadamente 10 segundos.

---

## 2. Processo de Transformação

As seguintes regras de transformação e padronização foram aplicadas:

### A. Limpeza e Tratamento de Nulos
- Colunas descritivas (strings) com valores nulos ou vazios foram tratadas usando a função `COALESCE(NULLIF(TRIM(col), ''), 'Desconhecido')`, garantindo integridade referencial nas dimensões.
- Valores ausentes foram apontados para o registro coringa com chave surrogate `-1`.

### B. Mapeamento Regional (DIM_LOCALIDADE)
- Os municípios do Espírito Santo foram classificados em quatro **Superintendências Regionais de Saúde** (`regiao_es`) e três **Macrorregiões de Saúde** (`macrorregiao`) oficiais da Secretaria de Saúde do ES (SESA):
  - **Metropolitana** (Polo Cariacica): Grande Vitória e adjacências.
  - **Central** (Polo Colatina): Centro-norte e noroeste.
  - **Norte** (Polo São Mateus): Extremo norte.
  - **Sul** (Polo Cachoeiro): Região meridional do estado.

### C. Conversão de Idades (Expressões Regulares)
- A coluna original `IdadeNaDataNotificacao` continha strings descritivas e compostas, tais como `'65 anos, 8 meses, 6 dias'`, `'9 meses'` ou `'15 dias'`.
- Foi implementado um bloco condicional `CASE` utilizando expressões regulares (`regexp_matches` e `regexp_extract`) no DuckDB para:
  1. Extrair o número inteiro de anos para pacientes maiores de 1 ano.
  2. Mapear para `0` anos bebês menores de 1 ano (onde constam apenas 'meses' ou 'dias').
  3. Validar e converter para inteiros numéricos válidos (`SMALLINT`), evitando falhas de cast.

### D. Consolidação de Datas de Exames
- A data de coleta consolidada (`sk_data_coleta`) foi gerada extraindo a primeira data válida disponível entre as colunas: `DataColeta_RT_PCR`, `DataColetaTesteRapido`, `DataColetaSorologia` e `DataColetaSorologiaIGG`.

### E. Integridade Referencial Dinâmica para Tempo
- As chaves de tempo na tabela fato são resolvidas por meio de `LEFT JOIN` com a dimensão `dim_tempo` (em vez de parsing manual de strings), evitando violações de chaves estrangeiras causadas por datas inválidas ou históricas raras.

---

## 3. Slowly Changing Dimension (SCD) Tipo 2
Para rastrear o histórico de população municipal, a dimensão `dim_localidade` foi modelada como SCD Tipo 2:
- **Colunas de controle:** `data_inicio`, `data_fim` e `flag_atual`.
- **Chave Natural (NK):** `nk_municipio_bairro` (composto por `municipio || '|' || bairro`).
- **Simulação de Atualização:** O pipeline realiza uma carga inicial e em seguida simula uma atualização da população para Vitória (de 365 mil para 375 mil) e Serra (de 520 mil para 540 mil) vigentes a partir de `01/01/2022`. Os registros antigos foram expirados (`flag_atual = FALSE`, `data_fim = '2021-12-31'`) e novas linhas foram inseridas para armazenar os dados atualizados.
- **Resolução na Fato:** O JOIN entre a fato e a dimensão localidade filtra a data da notificação dentro da faixa de vigência da dimensão (`DataNotificacao >= data_inicio AND (data_fim IS NULL OR DataNotificacao <= data_fim)`), garantindo que as notificações anteriores a 2022 usem o SK da população antiga e as novas usem o SK atualizado.

---

## 4. Validação de Qualidade de Dados (Procedure)
Ao final de cada execução do pipeline, o script executa uma procedure de controle de qualidade com 3 baterias de testes:

1.  **Bateria 1: Registro Coringa:** Valida se as 7 dimensões contêm o registro `SK = -1` (Desconhecido).
2.  **Bateria 2: Chaves Órfãs:** Realiza queries de verificação cruzada para atestar que não há registros na tabela fato apontando para surrogate keys inexistentes nas dimensões.
3.  **Bateria 3: Controle de Carga:** Compara a contagem total de linhas da tabela fato principal (`dw.fato_notificacao_covid`) com a staging (`stg.notificacao_raw`) e valida se a soma do campo `qtd_notificacao` bate com a contagem da fato, garantindo que não houve perda de dados ou duplicação.
---

## 5. Log de Execução do Pipeline (Exemplo de Sucesso)
Abaixo está o log de execução do script `etl.py` no console, demonstrando o processamento completo e a passagem nas baterias de validação:

```text
=== Iniciando Pipeline de ETL - Projeto 3 (Otimizado) ===
Conectando ao banco de dados DuckDB em: projeto_final\dw_covid.db
Criando schemas...
Carregando e otimizando a tabela de staging (stg.notificacao_raw)...
Carga da staging concluída em 0:00:12.047949. Total de linhas: 5,189,950
Gerando dimensão DIM_TEMPO...
DIM_TEMPO carregada com 11,324 registros.
Gerando dimensão DIM_LOCALIDADE...
Aplicando simulação de SCD Tipo 2 na população de Vitória e Serra (Vigência: 01/01/2022)...
DIM_LOCALIDADE carregada com 8,249 registros.
Gerando e carregando demais dimensões...
Todas as dimensões carregadas com sucesso.
Carregando Tabela Fato dw.fato_notificacao_covid (com joins otimizados)...
Carga da fato_notificacao_covid concluída em 0:03:08.729453.
Carregando segunda Tabela Fato (dw.fato_exame)...
Carga da fato_exame concluída em 0:06:01.947755.
Criando tabela analítica consolidada (mart.mv_resumo_municipio_mes)...
Data Mart populado e indexado com sucesso.
Executando stored procedure de validação de qualidade (validar_carga)...
[OK] Todas as validações de qualidade (Baterias 1, 2 e 3) passaram com sucesso!
=== ETL Otimizado Concluído com Sucesso! Tempo total: 0:09:25.686939 ===
```
