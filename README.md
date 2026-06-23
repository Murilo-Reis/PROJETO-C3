# Projeto 3 - Integração do Streamlit ao Data Warehouse Epidemiológico

Este diretório contém a entrega final do **Projeto 3** para a disciplina de Business Intelligence. O objetivo do projeto é fechar o ciclo de desenvolvimento integrando o dashboard de visualização de COVID-19 (C1) com a modelagem do Data Warehouse analítico (C2), comparando empiricamente o desempenho entre a leitura direta de arquivos Parquet e as consultas SQL no DW.

---

## 1. Estrutura de Arquivos da Entrega

A entrega final está estruturada na pasta `projeto_final` conforme solicitado:

*   [`etl.py`](file:///d:/projeto3/projeto_final/etl.py): Script de pipeline de ETL (Extração, Transformação e Carga) automatizado que lê os dados brutos de staging, aplica transformações, cria o banco DuckDB, simula SCD Tipo 2, gera agregados do Data Mart e roda testes de qualidade.
*   [`dashboard.py`](file:///d:/projeto3/projeto_final/dashboard.py): Dashboard Streamlit interativo. Ele suporta **Dual-Mode**, permitindo alternar dinamicamente na barra lateral entre a consulta original (Parquet) e consultas analíticas SQL contra o DW, medindo e comparando o tempo de resposta das duas abordagens em tempo real.
*   [`generate_report_pdf.py`](file:///d:/projeto3/projeto_final/generate_report_pdf.py): Script de automação que executa testes científicos de benchmarking de performance, gera gráficos comparativos de resposta, desenha o diagrama do modelo dimensional e compila o relatório oficial em PDF.
*   [`dw_covid.db`](file:///d:/projeto3/projeto_final/dw_covid.db): Banco de dados DuckDB gerado que armazena fisicamente o Data Warehouse (Schemas `dw` e `mart` de acordo com a modelagem do Projeto 2).
*   [`relatorio_desempenho.pdf`](file:///d:/projeto3/projeto_final/relatorio_desempenho.pdf): Relatório impresso final em PDF detalhando a performance e trade-offs arquiteturais.
*   [`relatorio_desempenho.md`](file:///d:/projeto3/projeto_final/relatorio_desempenho.md): Versão em markdown do relatório de desempenho.
*   [`documentacao_etl.md`](file:///d:/projeto3/projeto_final/documentacao_etl.md): Documentação técnica completa explicando a origem, transformações, regras de validação e SCD 2 aplicada ao ETL.
*   [`modelo_dimensional.png`](file:///d:/projeto3/projeto_final/modelo_dimensional.png): Diagrama visual do modelo de DW (Esquema Estrela e tabelas fato/dimensão).
*   [`benchmark_results.png`](file:///d:/projeto3/projeto_final/benchmark_results.png): Gráfico comparativo de resposta gerado no benchmark científico.

---

## 2. Como Executar o Projeto

### Pré-requisitos
Certifique-se de que os pacotes necessários estão instalados em seu ambiente Python:
```bash
pip install pandas pyarrow duckdb streamlit matplotlib reportlab
```

### Passo 1: Executar o Pipeline de ETL
O script de ETL lê a base local `MICRODADOS.parquet`, cria os schemas, popula as dimensões, tabela fato principal, fato de exames, o data mart e valida a integridade:
```bash
python projeto_final/etl.py
```
*Isso gerará o arquivo físico do banco `projeto_final/dw_covid.db`.*

### Passo 2: Executar o Dashboard Streamlit
Inicie a interface interativa do dashboard:
```bash
streamlit run projeto_final/dashboard.py
```
*No painel lateral, você poderá filtrar os dados e alternar entre os modos para avaliar a diferença de performance live.*

### Passo 3: Gerar Gráficos e Relatório PDF
Se desejar re-executar os benchmarks de performance e atualizar o relatório PDF oficial:
```bash
python projeto_final/generate_report_pdf.py
```

---

## 3. Modelo Dimensional do DW
O Data Warehouse segue a modelagem estrela (Star Schema):
- **Tabela Fato Central:** `dw.fato_notificacao_covid` (com chaves estrangeiras resolvidas e medidas agregadas).
- **Segunda Fato:** `dw.fato_exame` (com grão de exames laboratoriais realizados, compartilhando dimensões conformadas com a fato principal).
- **Tabelas de Dimensão:** `dim_tempo`, `dim_localidade` (com SCD Tipo 2 para população), `dim_perfil_paciente`, `dim_classificacao`, `dim_sintomas` (junk), `dim_comorbidade` (junk) e `dim_teste`.
- **Camada Data Mart (mart):** Tabela pré-agregada física `mart.mv_resumo_municipio_mes` indexada para renderização instantânea do dashboard.
