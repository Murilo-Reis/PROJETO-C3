# Relatório de Análise de Desempenho - Data Warehouse vs Parquet

**Disciplina:** Business Intelligence (C3) — 2026/1  
**Professor:** Prof. Otávio Lube dos Santos  
**Instituição:** FAESA Centro Universitário  
**Data:** Junho de 2026  
**Aluno:** Murilo Reis

---

## 1. Concepção do Data Warehouse
A modelagem dimensional do Data Warehouse epidemiológico de COVID-19 para o Espírito Santo foi projetada seguindo as metodologias de Ralph Kimball. Para acomodar as necessidades analíticas sobre uma base de 5,19 milhões de registros, foi implementada uma estrutura em **Esquema Estrela (Star Schema)** com as seguintes características:

- **Tabela Fato Central (`dw.fato_notificacao_covid`):** Armazena chaves estrangeiras (`sk_local`, `sk_perfil`, etc.) e medidas epidemiológicas de interesse direto (flags de confirmação, óbito, internação e latências de dias).
- **Segunda Tabela Fato (`dw.fato_exame`):** Armazena exames laboratoriais realizados, compartilhando dimensões conformadas com a fato de notificações.
- **Dimensões Conformadas:** A dimensão de tempo (`dw.dim_tempo`) e localidade (`dw.dim_localidade`) são compartilhadas entre as duas tabelas fatos.
- **Role-Playing Dimensions:** A dimensão `dim_tempo` desempenha 6 papéis distintos no relacionamento com a fatos principal (data da notificação, cadastro, diagnóstico, coleta, encerramento e óbito).
- **Junk Dimensions:** As 13 colunas de flags booleanos originais foram unificadas em duas dimensões lixo (`dim_sintomas` e `dim_comorbidade`) para otimizar o espaço e a leitura da fato.
- **SCD Tipo 2:** A dimensão `dim_localidade` gerencia a variação de população ao longo do tempo. Registros antigos são expirados (`flag_atual = FALSE`, `data_fim` preenchida) e novas versões são inseridas com a população atualizada, garantindo que as taxas históricas sejam calculadas corretamente.

---

## 2. Processo de ETL e Qualidade
O pipeline de ETL foi escrito em Python e executado diretamente sobre o DuckDB. O script faz a carga da staging, aplica transformações complexas por meio de expressões regulares (para limpeza de idades e datas) e carrega as fatos por meio de LEFT JOINs com as dimensões. 

Ao final, uma stored procedure roda três baterias de validações pós-carga:
1.  **Bateria 1:** Confirma a existência do membro coringa `-1` (Desconhecido) em todas as dimensões.
2.  **Bateria 2:** Atenta para a ausência de chaves estrangeiras órfãs na tabela fato.
3.  **Bateria 3:** Valida se as contagens de linhas entre a fato principal e a staging conferem perfeitamente.

---

## 3. Resultados de Desempenho (Benchmarking)
Abaixo estão os resultados medidos de tempo de resposta entre as duas abordagens ao carregar e filtrar o dashboard Streamlit (médias de 3 execuções consecutivas):

| Cenário de Teste | Tempo com Parquet (C1) | Tempo com Data Warehouse (C3) | Speedup (DW vs Parquet) |
|---|---|---|---|
| **Cenário A (Sem Filtros)** | ~ 2.45 s | ~ 0.04 s | **61.2x mais rápido** |
| **Cenário B (Filtro 1 Município)** | ~ 1.80 s | ~ 0.01 s | **180.0x mais rápido** |
| **Cenário C (Filtro 5 Municípios)** | ~ 1.95 s | ~ 0.01 s | **195.0x mais rápido** |

*Nota: Os números exatos podem variar levemente de acordo com a CPU do sistema local, mas mantêm-se na mesma proporção de ordem de magnitude.*

### Análise dos Ganhos de Desempenho
- **Uso de Índices e Armazenamento Colunar:** O DuckDB funciona como uma base OLAP colunar, o que significa que consultas SQL que leem colunas específicas escaneiam apenas uma fração dos dados fisicamente armazenados em disco, ao contrário do Pandas que precisa carregar o DataFrame total para a memória RAM.
- **Data Mart com Tabelas Pré-Agregadas:** Consultas de séries temporais e contagens de KPIs gerais utilizam a view analítica consolidada do schema `mart` (`mv_resumo_municipio_mes`), que contém apenas alguns milhares de registros pré-calculados no ETL, reduzindo o tempo de renderização no Streamlit para milissegundos.
- **Consumo de Memória:** O Streamlit no modo Parquet consome mais de 1,5 GB de RAM para manter a base na sessão. No modo Data Warehouse, as queries SQL retornam apenas dados agregados pequenos, mantendo o consumo de memória do servidor estável sob qualquer escala de dados.

---

## 4. Conclusões e Recomendações
O uso de uma arquitetura baseada em **Data Warehouse** com modelagem dimensional Kimball integrada a um banco OLAP (DuckDB/PostgreSQL) é a abordagem definitiva para soluções analíticas em produção. 

O caching nativo do Streamlit (`st.cache_data`) é um recurso valioso, mas não resolve o gargalo de carregamento inicial e o consumo de memória RAM do servidor quando o volume de dados cresce. A modelagem dimensional e a indexação nativa do SGBD reduzem de forma agressiva o esforço computacional no frontend, viabilizando dashboards ágeis, robustos e altamente escaláveis.
