import os
import sqlite3
import datetime
import unicodedata
import pandas as pd
import duckdb

def remove_accents(input_str):
    if not input_str:
        return ""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def run_etl():
    print("=== Iniciando Pipeline de ETL - Projeto 3 (Otimizado) ===")
    start_time = datetime.datetime.now()
    
    # 1. Conexão ao DuckDB
    db_path = os.path.join("projeto_final", "dw_covid.db")
    print(f"Conectando ao banco de dados DuckDB em: {db_path}")
    conn = duckdb.connect(db_path)
    
    # 2. Criação dos Schemas
    print("Criando schemas...")
    conn.execute("CREATE SCHEMA IF NOT EXISTS stg;")
    conn.execute("DROP SCHEMA IF EXISTS dw CASCADE;")
    conn.execute("DROP SCHEMA IF EXISTS mart CASCADE;")
    conn.execute("CREATE SCHEMA dw;")
    conn.execute("CREATE SCHEMA mart;")
    
    # 3. Carga e Otimização da Tabela de Staging
    print("Carregando e otimizando a tabela de staging (stg.notificacao_raw)...")
    stg_start = datetime.datetime.now()
    conn.execute("DROP TABLE IF EXISTS stg.notificacao_raw;")
    
    # Pré-calculamos tipos e higienizações no staging para acelerar os joins analíticos posteriores
    conn.execute("""
        CREATE TABLE stg.notificacao_raw AS 
        SELECT 
            TRY_CAST(DataNotificacao AS DATE) AS DataNotificacao_date,
            TRY_CAST(DataCadastro AS DATE) AS DataCadastro_date,
            TRY_CAST(DataDiagnostico AS DATE) AS DataDiagnostico_date,
            TRY_CAST(COALESCE(DataColeta_RT_PCR, DataColetaTesteRapido, DataColetaSorologia, DataColetaSorologiaIGG) AS DATE) AS DataColeta_date,
            TRY_CAST(DataEncerramento AS DATE) AS DataEncerramento_date,
            TRY_CAST(DataObito AS DATE) AS DataObito_date,
            
            UPPER(COALESCE(NULLIF(TRIM(Municipio), ''), 'DESCONHECIDO')) AS Municipio_clean,
            UPPER(COALESCE(NULLIF(TRIM(Bairro), ''), 'DESCONHECIDO')) AS Bairro_clean,
            
            COALESCE(NULLIF(TRIM(Sexo), ''), 'Desconhecido') AS Sexo_clean,
            COALESCE(NULLIF(TRIM(FaixaEtaria), ''), 'Desconhecido') AS FaixaEtaria_clean,
            COALESCE(NULLIF(TRIM(RacaCor), ''), 'Desconhecido') AS RacaCor_clean,
            COALESCE(NULLIF(TRIM(Escolaridade), ''), 'Desconhecido') AS Escolaridade_clean,
            COALESCE(NULLIF(TRIM(Gestante), ''), 'Desconhecido') AS Gestante_clean,
            COALESCE(NULLIF(TRIM(ProfissionalSaude), ''), 'Desconhecido') AS ProfissionalSaude_clean,
            
            COALESCE(NULLIF(TRIM(Classificacao), ''), 'Desconhecido') AS Classificacao_clean,
            COALESCE(NULLIF(TRIM(Evolucao), ''), 'Desconhecido') AS Evolucao_clean,
            COALESCE(NULLIF(TRIM(CriterioConfirmacao), ''), 'Desconhecido') AS CriterioConfirmacao_clean,
            COALESCE(NULLIF(TRIM(StatusNotificacao), ''), 'Desconhecido') AS StatusNotificacao_clean,
            
            COALESCE(NULLIF(TRIM(Febre), ''), 'Desconhecido') AS Febre_clean,
            COALESCE(NULLIF(TRIM(DificuldadeRespiratoria), ''), 'Desconhecido') AS DificuldadeRespiratoria_clean,
            COALESCE(NULLIF(TRIM(Tosse), ''), 'Desconhecido') AS Tosse_clean,
            COALESCE(NULLIF(TRIM(Coriza), ''), 'Desconhecido') AS Coriza_clean,
            COALESCE(NULLIF(TRIM(DorGarganta), ''), 'Desconhecido') AS DorGarganta_clean,
            COALESCE(NULLIF(TRIM(Diarreia), ''), 'Desconhecido') AS Diarreia_clean,
            COALESCE(NULLIF(TRIM(Cefaleia), ''), 'Desconhecido') AS Cefaleia_clean,
            
            COALESCE(NULLIF(TRIM(ComorbidadePulmao), ''), 'Desconhecido') AS ComorbidadePulmao_clean,
            COALESCE(NULLIF(TRIM(ComorbidadeCardio), ''), 'Desconhecido') AS ComorbidadeCardio_clean,
            COALESCE(NULLIF(TRIM(ComorbidadeRenal), ''), 'Desconhecido') AS ComorbidadeRenal_clean,
            COALESCE(NULLIF(TRIM(ComorbidadeDiabetes), ''), 'Desconhecido') AS ComorbidadeDiabetes_clean,
            COALESCE(NULLIF(TRIM(ComorbidadeTabagismo), ''), 'Desconhecido') AS ComorbidadeTabagismo_clean,
            COALESCE(NULLIF(TRIM(ComorbidadeObesidade), ''), 'Desconhecido') AS ComorbidadeObesidade_clean,
            
            COALESCE(NULLIF(TRIM(TipoTesteRapido), ''), 'Desconhecido') AS TipoTesteRapido_clean,
            COALESCE(NULLIF(TRIM(ResultadoRT_PCR), ''), 'Desconhecido') AS ResultadoRT_PCR_clean,
            COALESCE(NULLIF(TRIM(ResultadoTesteRapido), ''), 'Desconhecido') AS ResultadoTesteRapido_clean,
            COALESCE(NULLIF(TRIM(ResultadoSorologia), ''), 'Desconhecido') AS ResultadoSorologia_clean,
            
            CASE
                WHEN IdadeNaDataNotificacao IS NULL THEN NULL
                WHEN regexp_matches(IdadeNaDataNotificacao, '^[0-9]+$') THEN CAST(IdadeNaDataNotificacao AS SMALLINT)
                WHEN regexp_matches(IdadeNaDataNotificacao, 'ano') THEN CAST(regexp_extract(IdadeNaDataNotificacao, '([0-9]+)', 1) AS SMALLINT)
                WHEN regexp_matches(IdadeNaDataNotificacao, 'mes|dia|meses|dias') THEN 0
                ELSE TRY_CAST(regexp_extract(IdadeNaDataNotificacao, '([0-9]+)', 1) AS SMALLINT)
            END AS Idade_clean,
            
            FicouInternado,
            Evolucao,
            Classificacao,
            ResultadoSorologia,
            ResultadoRT_PCR,
            ResultadoTesteRapido
        FROM 'Projeto1/MICRODADOS.parquet';
    """)
    total_linhas_staging = conn.execute("SELECT COUNT(*) FROM stg.notificacao_raw;").fetchone()[0]
    print(f"Carga da staging concluída em {datetime.datetime.now() - stg_start}. Total de linhas: {total_linhas_staging:,}")
    
    # 4. Geração e Carga da DIM_TEMPO
    print("Gerando dimensão DIM_TEMPO...")
    start_date = datetime.date(2000, 1, 1)
    end_date = datetime.date(2030, 12, 31)
    delta = datetime.timedelta(days=1)
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += delta
        
    df_tempo = pd.DataFrame({
        'sk_tempo': [int(d.strftime('%Y%m%d')) for d in dates],
        'data': dates,
        'dia': [d.day for d in dates],
        'mes': [d.month for d in dates],
        'ano': [d.year for d in dates],
        'trimestre': [(d.month - 1) // 3 + 1 for d in dates],
        'semana_epidemiologica': [d.isocalendar()[1] for d in dates],
        'ano_mes': [d.strftime('%Y-%m') for d in dates]
    })
    
    months_pt = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho',
                 7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}
    weekdays_pt = {0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira', 3: 'Quinta-feira',
                   4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'}
    
    df_tempo['nome_mes'] = df_tempo['mes'].map(months_pt)
    df_tempo['dia_semana'] = df_tempo['data'].apply(lambda d: weekdays_pt[d.weekday()])
    
    # Adicionar linha de desconhecido (-1)
    row_desconhecido = pd.DataFrame([{
        'sk_tempo': -1,
        'data': None,
        'dia': 0,
        'mes': 0,
        'ano': 0,
        'trimestre': 0,
        'nome_mes': 'Desconhecido',
        'dia_semana': 'Desconhecido',
        'semana_epidemiologica': 0,
        'ano_mes': 'Desconhecido'
    }])
    df_tempo = pd.concat([row_desconhecido, df_tempo], ignore_index=True)
    
    conn.execute("""
        CREATE TABLE dw.dim_tempo (
            sk_tempo INTEGER PRIMARY KEY,
            data DATE,
            dia INTEGER,
            mes INTEGER,
            ano INTEGER,
            trimestre INTEGER,
            nome_mes VARCHAR,
            dia_semana VARCHAR,
            semana_epidemiologica INTEGER,
            ano_mes VARCHAR
        );
    """)
    
    conn.register("df_tempo_view", df_tempo)
    conn.execute("INSERT INTO dw.dim_tempo SELECT * FROM df_tempo_view;")
    conn.unregister("df_tempo_view")
    print(f"DIM_TEMPO carregada com {len(df_tempo):,} registros.")
    
    # 5. Geração e Carga da DIM_LOCALIDADE
    print("Gerando dimensão DIM_LOCALIDADE...")
    municipios_es = {
        'SERRA': ('Metropolitana', 'Grande Vitória'),
        'VILA VELHA': ('Metropolitana', 'Grande Vitória'),
        'CARIACICA': ('Metropolitana', 'Grande Vitória'),
        'VITORIA': ('Metropolitana', 'Grande Vitória'),
        'VIANA': ('Metropolitana', 'Grande Vitória'),
        'GUARAPARI': ('Metropolitana', 'Grande Vitória'),
        'FUNDAO': ('Metropolitana', 'Grande Vitória'),
        
        'AFONSO CLAUDIO': ('Metropolitana', 'Metropolitana'),
        'ALFREDO CHAVES': ('Metropolitana', 'Metropolitana'),
        'ANCHIETA': ('Metropolitana', 'Metropolitana'),
        'BREJETUBA': ('Metropolitana', 'Metropolitana'),
        'CONCEICAO DO CASTELO': ('Metropolitana', 'Metropolitana'),
        'DOMINGOS MARTINS': ('Metropolitana', 'Metropolitana'),
        'ITAGUACU': ('Metropolitana', 'Metropolitana'),
        'ITARANA': ('Metropolitana', 'Metropolitana'),
        'LARANJA DA TERRA': ('Metropolitana', 'Metropolitana'),
        'MARECHAL FLORIANO': ('Metropolitana', 'Metropolitana'),
        'PIUMA': ('Metropolitana', 'Metropolitana'),
        'RIO NOVO DO SUL': ('Metropolitana', 'Metropolitana'),
        'SANTA LEOPOLDINA': ('Metropolitana', 'Metropolitana'),
        'SANTA MARIA DE JETIBA': ('Metropolitana', 'Metropolitana'),
        'SANTA TERESA': ('Metropolitana', 'Metropolitana'),
        'VENDA NOVA DO IMIGRANTE': ('Metropolitana', 'Metropolitana'),
        
        'ARACRUZ': ('Central', 'Central Norte'),
        'IBIRACU': ('Central', 'Central Norte'),
        'JOAO NEIVA': ('Central', 'Central Norte'),
        'LINHARES': ('Central', 'Central Norte'),
        'SOORETAMA': ('Central', 'Central Norte'),
        'RIO BANANAL': ('Central', 'Central Norte'),
        'COLATINA': ('Central', 'Central Norte'),
        'BAIXO GUANDU': ('Central', 'Central Norte'),
        'PANCAS': ('Central', 'Central Norte'),
        'MARILANDIA': ('Central', 'Central Norte'),
        'SAO ROQUE DO CANAA': ('Central', 'Central Norte'),
        'SÃO ROQUE DO CANAA': ('Central', 'Central Norte'),
        'GOVERNADOR LINDENBERG': ('Central', 'Central Norte'),
        'SAO DOMINGOS DO NORTE': ('Central', 'Central Norte'),
        'SÃO DOMINGOS DO NORTE': ('Central', 'Central Norte'),
        'ALTO RIO NOVO': ('Central', 'Central Norte'),
        'MANTENOPOLIS': ('Central', 'Central Norte'),
        'SAO GABRIEL DA PALHA': ('Central', 'Central Norte'),
        'SÃO GABRIEL DA PALHA': ('Central', 'Central Norte'),
        'VILA VALERIO': ('Central', 'Central Norte'),
        'AGUIA BRANCA': ('Central', 'Central Norte'),
        
        'SAO MATEUS': ('Norte', 'Central Norte'),
        'SÃO MATEUS': ('Norte', 'Central Norte'),
        'CONCEICAO DA BARRA': ('Norte', 'Central Norte'),
        'PINHEIROS': ('Norte', 'Central Norte'),
        'PEDRO CANARIO': ('Norte', 'Central Norte'),
        'MONTANHA': ('Norte', 'Central Norte'),
        'MUCURICI': ('Norte', 'Central Norte'),
        'PONTO BELO': ('Norte', 'Central Norte'),
        'JAGUARE': ('Norte', 'Central Norte'),
        'BOA ESPERANCA': ('Norte', 'Central Norte'),
        'ECOPORANGA': ('Norte', 'Central Norte'),
        'BARRA DE SAO FRANCISCO': ('Norte', 'Central Norte'),
        'BARRA DE SÃO FRANCISCO': ('Norte', 'Central Norte'),
        'AGUA DOCE DO NORTE': ('Norte', 'Central Norte'),
        'VILA PAVAO': ('Norte', 'Central Norte'),
        'NOVA VENECIA': ('Norte', 'Central Norte'),
        
        'CACHOEIRO DE ITAPEMIRIM': ('Sul', 'Sul'),
        'ALEGRE': ('Sul', 'Sul'),
        'APIACA': ('Sul', 'Sul'),
        'ATILIO VIVACQUA': ('Sul', 'Sul'),
        'BOM JESUS DO NORTE': ('Sul', 'Sul'),
        'DIVINO DE SAO LOURENCO': ('Sul', 'Sul'),
        'DORES DO RIO PRETO': ('Sul', 'Sul'),
        'GUACUI': ('Sul', 'Sul'),
        'IBITIRAMA': ('Sul', 'Sul'),
        'IUNA': ('Sul', 'Sul'),
        'IRUPI': ('Sul', 'Sul'),
        'ITAPEMIRIM': ('Sul', 'Sul'),
        'JERONIMO MONTEIRO': ('Sul', 'Sul'),
        'MARATAIZES': ('Sul', 'Sul'),
        'MARATAÍZES': ('Sul', 'Sul'),
        'MIMOSO DO SUL': ('Sul', 'Sul'),
        'MUNIZ FREIRE': ('Sul', 'Sul'),
        'MUQUI': ('Sul', 'Sul'),
        'PRESIDENTE KENNEDY': ('Sul', 'Sul'),
        'SAO JOSE DO CALCADO': ('Sul', 'Sul'),
        'SÃO JOSÉ DO CALÇADO': ('Sul', 'Sul'),
        'VARGEM ALTA': ('Sul', 'Sul'),
        'CASTELO': ('Sul', 'Sul'),
        'ICONHA': ('Sul', 'Sul'),
    }
    municipios_es_clean = {remove_accents(k): v for k, v in municipios_es.items()}
    
    populations = {
        'SERRA': 520000,
        'VILA VELHA': 500000,
        'CARIACICA': 380000,
        'VITORIA': 365000,
        'CACHOEIRO DE ITAPEMIRIM': 210000,
        'LINHARES': 170000,
        'SAO MATEUS': 130000,
        'GUARAPARI': 125000,
        'COLATINA': 120000
    }
    populations_clean = {remove_accents(k): v for k, v in populations.items()}
    
    df_loc_raw = conn.execute("SELECT DISTINCT Municipio_clean, Bairro_clean FROM stg.notificacao_raw;").fetchdf()
    
    loc_rows = []
    loc_rows.append({
        'sk_local': -1,
        'municipio': 'DESCONHECIDO',
        'bairro': 'DESCONHECIDO',
        'uf': 'DESCONHECIDO',
        'regiao_es': 'DESCONHECIDO',
        'macrorregiao': 'DESCONHECIDO',
        'populacao_municipio': 0,
        'data_inicio': datetime.date(2000, 1, 1),
        'data_fim': None,
        'flag_atual': True,
        'nk_municipio_bairro': 'DESCONHECIDO|DESCONHECIDO'
    })
    
    sk_counter = 1
    for idx, row in df_loc_raw.iterrows():
        mun = row['Municipio_clean']
        bai = row['Bairro_clean']
        if mun == 'DESCONHECIDO' and bai == 'DESCONHECIDO':
            continue
            
        mun_clean = remove_accents(mun)
        
        if mun_clean in municipios_es_clean:
            reg, macro = municipios_es_clean[mun_clean]
            uf = 'ES'
        else:
            reg, macro = 'Outros', 'Outros'
            uf = 'Outros'
            
        pop = populations_clean.get(mun_clean, 50000)
        
        loc_rows.append({
            'sk_local': sk_counter,
            'municipio': mun,
            'bairro': bai,
            'uf': uf,
            'regiao_es': reg,
            'macrorregiao': macro,
            'populacao_municipio': pop,
            'data_inicio': datetime.date(2000, 1, 1),
            'data_fim': None,
            'flag_atual': True,
            'nk_municipio_bairro': f"{mun}|{bai}"
        })
        sk_counter += 1
        
    df_localidade = pd.DataFrame(loc_rows)
    
    # Simular SCD Tipo 2
    print("Aplicando simulação de SCD Tipo 2 na população de Vitória e Serra (Vigência: 01/01/2022)...")
    change_date = datetime.date(2022, 1, 1)
    close_date = datetime.date(2021, 12, 31)
    new_rows = []
    for idx, row in df_localidade.iterrows():
        mun = row['municipio']
        if mun == 'VITORIA' and row['flag_atual'] == True:
            df_localidade.at[idx, 'data_fim'] = close_date
            df_localidade.at[idx, 'flag_atual'] = False
            
            new_row = row.copy()
            new_row['sk_local'] = sk_counter
            new_row['populacao_municipio'] = 375000
            new_row['data_inicio'] = change_date
            new_row['data_fim'] = None
            new_row['flag_atual'] = True
            new_rows.append(new_row)
            sk_counter += 1
            
        elif mun == 'SERRA' and row['flag_atual'] == True:
            df_localidade.at[idx, 'data_fim'] = close_date
            df_localidade.at[idx, 'flag_atual'] = False
            
            new_row = row.copy()
            new_row['sk_local'] = sk_counter
            new_row['populacao_municipio'] = 540000
            new_row['data_inicio'] = change_date
            new_row['data_fim'] = None
            new_row['flag_atual'] = True
            new_rows.append(new_row)
            sk_counter += 1
            
    if new_rows:
        df_localidade = pd.concat([df_localidade, pd.DataFrame(new_rows)], ignore_index=True)
        
    conn.execute("""
        CREATE TABLE dw.dim_localidade (
            sk_local INTEGER PRIMARY KEY,
            municipio VARCHAR,
            bairro VARCHAR,
            uf VARCHAR,
            regiao_es VARCHAR,
            macrorregiao VARCHAR,
            populacao_municipio INTEGER,
            data_inicio DATE,
            data_fim DATE,
            flag_atual BOOLEAN,
            nk_municipio_bairro VARCHAR
        );
    """)
    conn.register("df_loc_view", df_localidade)
    conn.execute("INSERT INTO dw.dim_localidade SELECT * FROM df_loc_view;")
    conn.unregister("df_loc_view")
    print(f"DIM_LOCALIDADE carregada com {len(df_localidade):,} registros.")
    
    # 6. Geração e Carga das Demais Dimensões
    print("Gerando e carregando demais dimensões...")
    
    # 6.1 DIM_PERFIL_PACIENTE
    conn.execute("""
        CREATE TABLE dw.dim_perfil_paciente (
            sk_perfil INTEGER PRIMARY KEY,
            sexo VARCHAR,
            faixa_etaria VARCHAR,
            raca_cor VARCHAR,
            escolaridade VARCHAR,
            gestante VARCHAR,
            profissional_saude VARCHAR
        );
    """)
    conn.execute("INSERT INTO dw.dim_perfil_paciente VALUES (-1, 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido');")
    conn.execute("""
        INSERT INTO dw.dim_perfil_paciente
        SELECT 
            row_number() OVER () AS sk_perfil,
            Sexo_clean, FaixaEtaria_clean, RacaCor_clean, Escolaridade_clean, Gestante_clean, ProfissionalSaude_clean
        FROM (
            SELECT DISTINCT Sexo_clean, FaixaEtaria_clean, RacaCor_clean, Escolaridade_clean, Gestante_clean, ProfissionalSaude_clean 
            FROM stg.notificacao_raw
        ) WHERE NOT (Sexo_clean = 'Desconhecido' AND FaixaEtaria_clean = 'Desconhecido' AND RacaCor_clean = 'Desconhecido' AND Escolaridade_clean = 'Desconhecido' AND Gestante_clean = 'Desconhecido' AND ProfissionalSaude_clean = 'Desconhecido');
    """)
    
    # 6.2 DIM_CLASSIFICACAO
    conn.execute("""
        CREATE TABLE dw.dim_classificacao (
            sk_class INTEGER PRIMARY KEY,
            classificacao VARCHAR,
            evolucao VARCHAR,
            criterio_confirmacao VARCHAR,
            status_notificacao VARCHAR
        );
    """)
    conn.execute("INSERT INTO dw.dim_classificacao VALUES (-1, 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido');")
    conn.execute("""
        INSERT INTO dw.dim_classificacao
        SELECT 
            row_number() OVER () AS sk_class,
            Classificacao_clean, Evolucao_clean, CriterioConfirmacao_clean, StatusNotificacao_clean
        FROM (
            SELECT DISTINCT Classificacao_clean, Evolucao_clean, CriterioConfirmacao_clean, StatusNotificacao_clean 
            FROM stg.notificacao_raw
        ) WHERE NOT (Classificacao_clean = 'Desconhecido' AND Evolucao_clean = 'Desconhecido' AND CriterioConfirmacao_clean = 'Desconhecido' AND StatusNotificacao_clean = 'Desconhecido');
    """)
    
    # 6.3 DIM_SINTOMAS
    conn.execute("""
        CREATE TABLE dw.dim_sintomas (
            sk_sint INTEGER PRIMARY KEY,
            febre VARCHAR,
            dif_respiratoria VARCHAR,
            tosse VARCHAR,
            coriza VARCHAR,
            dor_garganta VARCHAR,
            diarreia VARCHAR,
            cefaleia VARCHAR
        );
    """)
    conn.execute("INSERT INTO dw.dim_sintomas VALUES (-1, 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido');")
    conn.execute("""
        INSERT INTO dw.dim_sintomas
        SELECT 
            row_number() OVER () AS sk_sint,
            Febre_clean, DificuldadeRespiratoria_clean, Tosse_clean, Coriza_clean, DorGarganta_clean, Diarreia_clean, Cefaleia_clean
        FROM (
            SELECT DISTINCT Febre_clean, DificuldadeRespiratoria_clean, Tosse_clean, Coriza_clean, DorGarganta_clean, Diarreia_clean, Cefaleia_clean 
            FROM stg.notificacao_raw
        ) WHERE NOT (Febre_clean = 'Desconhecido' AND DificuldadeRespiratoria_clean = 'Desconhecido' AND Tosse_clean = 'Desconhecido' AND Coriza_clean = 'Desconhecido' AND DorGarganta_clean = 'Desconhecido' AND Diarreia_clean = 'Desconhecido' AND Cefaleia_clean = 'Desconhecido');
    """)
    
    # 6.4 DIM_COMORBIDADE
    conn.execute("""
        CREATE TABLE dw.dim_comorbidade (
            sk_como INTEGER PRIMARY KEY,
            com_pulmao VARCHAR,
            com_cardio VARCHAR,
            com_renal VARCHAR,
            com_diabetes VARCHAR,
            com_tabagismo VARCHAR,
            com_obesidade VARCHAR
        );
    """)
    conn.execute("INSERT INTO dw.dim_comorbidade VALUES (-1, 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido');")
    conn.execute("""
        INSERT INTO dw.dim_comorbidade
        SELECT 
            row_number() OVER () AS sk_como,
            ComorbidadePulmao_clean, ComorbidadeCardio_clean, ComorbidadeRenal_clean, ComorbidadeDiabetes_clean, ComorbidadeTabagismo_clean, ComorbidadeObesidade_clean
        FROM (
            SELECT DISTINCT ComorbidadePulmao_clean, ComorbidadeCardio_clean, ComorbidadeRenal_clean, ComorbidadeDiabetes_clean, ComorbidadeTabagismo_clean, ComorbidadeObesidade_clean 
            FROM stg.notificacao_raw
        ) WHERE NOT (ComorbidadePulmao_clean = 'Desconhecido' AND ComorbidadeCardio_clean = 'Desconhecido' AND ComorbidadeRenal_clean = 'Desconhecido' AND ComorbidadeDiabetes_clean = 'Desconhecido' AND ComorbidadeTabagismo_clean = 'Desconhecido' AND ComorbidadeObesidade_clean = 'Desconhecido');
    """)
    
    # 6.5 DIM_TESTE
    conn.execute("""
        CREATE TABLE dw.dim_teste (
            sk_teste INTEGER PRIMARY KEY,
            tipo_teste_rapido VARCHAR,
            resultado_rt_pcr VARCHAR,
            resultado_teste_rap VARCHAR,
            resultado_sorologia VARCHAR
        );
    """)
    conn.execute("INSERT INTO dw.dim_teste VALUES (-1, 'Desconhecido', 'Desconhecido', 'Desconhecido', 'Desconhecido');")
    conn.execute("""
        INSERT INTO dw.dim_teste
        SELECT 
            row_number() OVER () AS sk_teste,
            TipoTesteRapido_clean, ResultadoRT_PCR_clean, ResultadoTesteRapido_clean, ResultadoSorologia_clean
        FROM (
            SELECT DISTINCT TipoTesteRapido_clean, ResultadoRT_PCR_clean, ResultadoTesteRapido_clean, ResultadoSorologia_clean 
            FROM stg.notificacao_raw
        ) WHERE NOT (TipoTesteRapido_clean = 'Desconhecido' AND ResultadoRT_PCR_clean = 'Desconhecido' AND ResultadoTesteRapido_clean = 'Desconhecido' AND ResultadoSorologia_clean = 'Desconhecido');
    """)
    print("Todas as dimensões carregadas com sucesso.")
    
    # 7. Carga da Tabela Fato Principal: fato_notificacao_covid
    print("Carregando Tabela Fato dw.fato_notificacao_covid (com joins otimizados)...")
    fato_start = datetime.datetime.now()
    conn.execute("""
        CREATE TABLE dw.fato_notificacao_covid (
            sk_data_notificacao INTEGER REFERENCES dw.dim_tempo(sk_tempo),
            sk_data_cadastro INTEGER REFERENCES dw.dim_tempo(sk_tempo),
            sk_data_diagnostico INTEGER REFERENCES dw.dim_tempo(sk_tempo),
            sk_data_coleta INTEGER REFERENCES dw.dim_tempo(sk_tempo),
            sk_data_Campanhas INTEGER, -- Mantendo campos anteriores para compatibilidade
            sk_data_encerramento INTEGER REFERENCES dw.dim_tempo(sk_tempo),
            sk_data_obito INTEGER REFERENCES dw.dim_tempo(sk_tempo),
            sk_local INTEGER REFERENCES dw.dim_localidade(sk_local),
            sk_perfil INTEGER REFERENCES dw.dim_perfil_paciente(sk_perfil),
            sk_class INTEGER REFERENCES dw.dim_classificacao(sk_class),
            sk_sint INTEGER REFERENCES dw.dim_sintomas(sk_sint),
            sk_como INTEGER REFERENCES dw.dim_comorbidade(sk_como),
            sk_teste INTEGER REFERENCES dw.dim_teste(sk_teste),
            flag_confirmado SMALLINT,
            flag_obito_covid SMALLINT,
            flag_internado SMALLINT,
            flag_cura SMALLINT,
            idade_anos SMALLINT,
            dias_notif_encerramento INTEGER,
            dias_notif_obito INTEGER,
            qtd_notificacao INTEGER
        );
    """)
    
    # Join otimizado: as chaves já estão limpas e padronizadas no staging
    conn.execute("""
        INSERT INTO dw.fato_notificacao_covid
        SELECT
            COALESCE(t_notif.sk_tempo, -1) AS sk_data_notificacao,
            COALESCE(t_cad.sk_tempo, -1) AS sk_data_cadastro,
            COALESCE(t_diag.sk_tempo, -1) AS sk_data_diagnostico,
            COALESCE(t_col.sk_tempo, -1) AS sk_data_coleta,
            NULL AS sk_data_Campanhas,
            COALESCE(t_enc.sk_tempo, -1) AS sk_data_encerramento,
            COALESCE(t_ob.sk_tempo, -1) AS sk_data_obito,
            COALESCE(loc.sk_local, -1) AS sk_local,
            COALESCE(perf.sk_perfil, -1) AS sk_perfil,
            COALESCE(cla.sk_class, -1) AS sk_class,
            COALESCE(sint.sk_sint, -1) AS sk_sint,
            COALESCE(como.sk_como, -1) AS sk_como,
            COALESCE(tst.sk_teste, -1) AS sk_teste,
            CASE WHEN r.Classificacao_clean = 'Confirmados' THEN 1 ELSE 0 END AS flag_confirmado,
            CASE WHEN r.Evolucao_clean LIKE '%Óbito pelo COVID%' THEN 1 ELSE 0 END AS flag_obito_covid,
            CASE WHEN r.FicouInternado = 'Sim' THEN 1 ELSE 0 END AS flag_internado,
            CASE WHEN r.Evolucao_clean = 'Cura' THEN 1 ELSE 0 END AS flag_cura,
            r.Idade_clean AS idade_anos,
            CASE 
                WHEN r.DataEncerramento_date IS NOT NULL AND r.DataNotificacao_date IS NOT NULL
                THEN CAST(date_diff('day', r.DataNotificacao_date, r.DataEncerramento_date) AS INTEGER)
                ELSE NULL
            END AS dias_notif_encerramento,
            CASE 
                WHEN r.DataObito_date IS NOT NULL AND r.DataNotificacao_date IS NOT NULL
                THEN CAST(date_diff('day', r.DataNotificacao_date, r.DataObito_date) AS INTEGER)
                ELSE NULL
            END AS dias_notif_obito,
            1 AS qtd_notificacao
        FROM stg.notificacao_raw r
        LEFT JOIN dw.dim_localidade loc
            ON r.Municipio_clean = loc.municipio
           AND r.Bairro_clean = loc.bairro
           AND (r.DataNotificacao_date >= loc.data_inicio OR r.DataNotificacao_date IS NULL)
           AND (loc.data_fim IS NULL OR r.DataNotificacao_date <= loc.data_fim)
        LEFT JOIN dw.dim_perfil_paciente perf
            ON r.Sexo_clean = perf.sexo
           AND r.FaixaEtaria_clean = perf.faixa_etaria
           AND r.RacaCor_clean = perf.raca_cor
           AND r.Escolaridade_clean = perf.escolaridade
           AND r.Gestante_clean = perf.gestante
           AND r.ProfissionalSaude_clean = perf.profissional_saude
        LEFT JOIN dw.dim_classificacao cla
            ON r.Classificacao_clean = cla.classificacao
           AND r.Evolucao_clean = cla.evolucao
           AND r.CriterioConfirmacao_clean = cla.criterio_confirmacao
           AND r.StatusNotificacao_clean = cla.status_notificacao
        LEFT JOIN dw.dim_sintomas sint
            ON r.Febre_clean = sint.febre
           AND r.DificuldadeRespiratoria_clean = sint.dif_respiratoria
           AND r.Tosse_clean = sint.tosse
           AND r.Coriza_clean = sint.coriza
           AND r.DorGarganta_clean = sint.dor_garganta
           AND r.Diarreia_clean = sint.diarreia
           AND r.Cefaleia_clean = sint.cefaleia
        LEFT JOIN dw.dim_comorbidade como
            ON r.ComorbidadePulmao_clean = como.com_pulmao
           AND r.ComorbidadeCardio_clean = como.com_cardio
           AND r.ComorbidadeRenal_clean = como.com_renal
           AND r.ComorbidadeDiabetes_clean = como.com_diabetes
           AND r.ComorbidadeTabagismo_clean = como.com_tabagismo
           AND r.ComorbidadeObesidade_clean = como.com_obesidade
        LEFT JOIN dw.dim_teste tst
            ON r.TipoTesteRapido_clean = tst.tipo_teste_rapido
           AND r.ResultadoRT_PCR_clean = tst.resultado_rt_pcr
           AND r.ResultadoTesteRapido_clean = tst.resultado_teste_rap
           AND r.ResultadoSorologia_clean = tst.resultado_sorologia
        LEFT JOIN dw.dim_tempo t_notif ON r.DataNotificacao_date = t_notif.data
        LEFT JOIN dw.dim_tempo t_cad ON r.DataCadastro_date = t_cad.data
        LEFT JOIN dw.dim_tempo t_diag ON r.DataDiagnostico_date = t_diag.data
        LEFT JOIN dw.dim_tempo t_col ON r.DataColeta_date = t_col.data
        LEFT JOIN dw.dim_tempo t_enc ON r.DataEncerramento_date = t_enc.data
        LEFT JOIN dw.dim_tempo t_ob ON r.DataObito_date = t_ob.data;
    """)
    print(f"Carga da fato_notificacao_covid concluída em {datetime.datetime.now() - fato_start}.")
    
    # 8. Carga da Segunda Tabela Fato: fato_exame
    print("Carregando segunda Tabela Fato (dw.fato_exame)...")
    exame_start = datetime.datetime.now()
    conn.execute("""
        CREATE TABLE dw.fato_exame (
            sk_fato_exame BIGINT PRIMARY KEY,
            sk_data_coleta INTEGER REFERENCES dw.dim_tempo(sk_tempo),
            sk_data_resultado INTEGER REFERENCES dw.dim_tempo(sk_tempo),
            sk_local INTEGER REFERENCES dw.dim_localidade(sk_local),
            sk_perfil INTEGER REFERENCES dw.dim_perfil_paciente(sk_perfil),
            sk_teste INTEGER REFERENCES dw.dim_teste(sk_teste),
            qtd_exames SMALLINT,
            flag_positivo SMALLINT,
            dias_coleta_result INTEGER
        );
    """)
    
    conn.execute("""
        INSERT INTO dw.fato_exame
        SELECT
            row_number() OVER () AS sk_fato_exame,
            COALESCE(t_col.sk_tempo, -1) AS sk_data_coleta,
            -1 AS sk_data_resultado,
            COALESCE(loc.sk_local, -1) AS sk_local,
            COALESCE(perf.sk_perfil, -1) AS sk_perfil,
            COALESCE(tst.sk_teste, -1) AS sk_teste,
            1 AS qtd_exames,
            CASE 
                WHEN r.ResultadoRT_PCR_clean = 'POSITIVO' OR r.ResultadoTesteRapido_clean = 'POSITIVO' OR r.ResultadoSorologia_clean = 'POSITIVO'
                THEN 1 
                ELSE 0 
            END AS flag_positivo,
            NULL AS dias_coleta_result
        FROM stg.notificacao_raw r
        LEFT JOIN dw.dim_localidade loc
            ON r.Municipio_clean = loc.municipio
           AND r.Bairro_clean = loc.bairro
           AND (r.DataNotificacao_date >= loc.data_inicio OR r.DataNotificacao_date IS NULL)
           AND (loc.data_fim IS NULL OR r.DataNotificacao_date <= loc.data_fim)
        LEFT JOIN dw.dim_perfil_paciente perf
            ON r.Sexo_clean = perf.sexo
           AND r.FaixaEtaria_clean = perf.faixa_etaria
           AND r.RacaCor_clean = perf.raca_cor
           AND r.Escolaridade_clean = perf.escolaridade
           AND r.Gestante_clean = perf.gestante
           AND r.ProfissionalSaude_clean = perf.profissional_saude
        LEFT JOIN dw.dim_teste tst
            ON r.TipoTesteRapido_clean = tst.tipo_teste_rapido
           AND r.ResultadoRT_PCR_clean = tst.resultado_rt_pcr
           AND r.ResultadoTesteRapido_clean = tst.resultado_teste_rap
           AND r.ResultadoSorologia_clean = tst.resultado_sorologia
        LEFT JOIN dw.dim_tempo t_col ON r.DataColeta_date = t_col.data
        WHERE r.ResultadoRT_PCR_clean != 'Desconhecido' OR r.ResultadoTesteRapido_clean != 'Desconhecido' OR r.ResultadoSorologia_clean != 'Desconhecido';
    """)
    print(f"Carga da fato_exame concluída em {datetime.datetime.now() - exame_start}.")
    
    # 9. Criação da Materialized View
    print("Criando tabela analítica consolidada (mart.mv_resumo_municipio_mes)...")
    conn.execute("""
        CREATE TABLE mart.mv_resumo_municipio_mes AS
        SELECT
            l.municipio,
            t.ano_mes,
            SUM(f.flag_confirmado)   AS confirmados,
            SUM(f.flag_obito_covid)  AS obitos,
            SUM(f.flag_internado)    AS internacoes,
            SUM(f.qtd_notificacao)   AS notificacoes
        FROM dw.fato_notificacao_covid f
        JOIN dw.dim_localidade l ON l.sk_local = f.sk_local
        JOIN dw.dim_tempo      t ON t.sk_tempo = f.sk_data_notificacao
        GROUP BY l.municipio, t.ano_mes;
    """)
    conn.execute("CREATE INDEX idx_mv_municipio_mes ON mart.mv_resumo_municipio_mes(municipio, ano_mes);")
    print("Data Mart populado e indexado com sucesso.")
    
    # 10. Validação de Qualidade de Dados (Procedure)
    print("Executando stored procedure de validação de qualidade (validar_carga)...")
    validar_carga(conn)
    
    conn.close()
    print(f"=== ETL Otimizado Concluído com Sucesso! Tempo total: {datetime.datetime.now() - start_time} ===")

def validar_carga(conn):
    erros = []
    
    dimensoes = [
        ("dw.dim_localidade", "sk_local"),
        ("dw.dim_perfil_paciente", "sk_perfil"),
        ("dw.dim_classificacao", "sk_class"),
        ("dw.dim_sintomas", "sk_sint"),
        ("dw.dim_comorbidade", "sk_como"),
        ("dw.dim_teste", "sk_teste"),
        ("dw.dim_tempo", "sk_tempo")
    ]
    
    for tabela, col in dimensoes:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tabela} WHERE {col} = -1;").fetchone()[0]
        if cnt == 0:
            erros.append(f"[ERRO] Tabela {tabela} não contém o membro desconhecido (-1).")
            
    orfaos_local = conn.execute("""
        SELECT COUNT(*) 
        FROM dw.fato_notificacao_covid f 
        LEFT JOIN dw.dim_localidade d ON d.sk_local = f.sk_local 
        WHERE d.sk_local IS NULL;
    """).fetchone()[0]
    
    if orfaos_local > 0:
        erros.append(f"[ERRO] Encontradas {orfaos_local} chaves órfãs na fato para dim_localidade.")
        
    orfaos_tempo = conn.execute("""
        SELECT COUNT(*) 
        FROM dw.fato_notificacao_covid f 
        LEFT JOIN dw.dim_tempo d ON d.sk_tempo = f.sk_data_notificacao 
        WHERE d.sk_tempo IS NULL;
    """).fetchone()[0]
    
    if orfaos_tempo > 0:
        erros.append(f"[ERRO] Encontradas {orfaos_tempo} chaves órfãs na fato para dim_tempo.")
        
    cnt_fato = conn.execute("SELECT COUNT(*) FROM dw.fato_notificacao_covid;").fetchone()[0]
    cnt_staging = conn.execute("SELECT COUNT(*) FROM stg.notificacao_raw;").fetchone()[0]
    
    if cnt_fato != cnt_staging:
        erros.append(f"[ERRO] Divergência na contagem de registros: Fato={cnt_fato:,}, Staging={cnt_staging:,}.")
        
    sum_qtd = conn.execute("SELECT SUM(qtd_notificacao) FROM dw.fato_notificacao_covid;").fetchone()[0]
    if sum_qtd != cnt_fato:
        erros.append(f"[ERRO] Soma de qtd_notificacao ({sum_qtd:,}) diverge da contagem da fato ({cnt_fato:,}).")
        
    if not erros:
        print("[OK] Todas as validações de qualidade (Baterias 1, 2 e 3) passaram com sucesso!")
    else:
        print("FALHAS NA VALIDAÇÃO DE QUALIDADE:")
        for erro in erros:
            print(erro)
        raise ValueError("Falha na validação dos dados pós-ETL.")

if __name__ == "__main__":
    run_etl()
