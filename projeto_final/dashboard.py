import os
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import duckdb
import time

st.set_page_config(page_title="Dashboard Covid-19: Parquet vs DW", layout="wide", page_icon="🦠")

# Estilo premium moderno
st.markdown("""
<style>
    .metric-container {
        background: linear-gradient(135deg, #1f2937, #111827);
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        margin-bottom: 20px;
        border: 1px solid #374151;
        transition: transform 0.2s;
    }
    .metric-container:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 12px rgba(0,0,0,0.5);
    }
    .metric-title {
        color: #9CA3AF;
        font-size: 1.1rem;
        font-weight: 500;
        margin-bottom: 8px;
    }
    .metric-value {
        color: #F9FAFB;
        font-size: 2.2rem;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

# Ajuste de caminhos relativos
parquet_path = 'MICRODADOS.parquet'
if not os.path.exists(parquet_path):
    parquet_path = 'Projeto1/MICRODADOS.parquet'
if not os.path.exists(parquet_path):
    parquet_path = '../Projeto1/MICRODADOS.parquet'

db_path = 'projeto_final/dw_covid.db'
if not os.path.exists(db_path):
    db_path = 'dw_covid.db'
if not os.path.exists(db_path):
    db_path = '../projeto_final/dw_covid.db'

@st.cache_data
def get_filter_options():
    if os.path.exists(db_path):
        conn = duckdb.connect(db_path)
        muns = conn.execute("SELECT DISTINCT municipio FROM dw.dim_localidade WHERE municipio != 'DESCONHECIDO' ORDER BY municipio;").df()['municipio'].tolist()
        classes = conn.execute("SELECT DISTINCT classificacao FROM dw.dim_classificacao WHERE classificacao != 'Desconhecido' ORDER BY classificacao;").df()['classificacao'].tolist()
        conn.close()
        return muns, classes
    else:
        try:
            df_temp = pd.read_parquet(parquet_path, columns=['Municipio', 'Classificacao'])
            muns = sorted(df_temp['Municipio'].dropna().unique().tolist())
            classes = sorted(df_temp['Classificacao'].dropna().unique().tolist())
            return muns, classes
        except Exception:
            return [], []

muns_list, classes_list = get_filter_options()

# --- SIDEBAR: Filtros ---
st.sidebar.header("Filtros Interativos")
selected_muns = st.sidebar.multiselect("Filtrar por Municípios:", options=muns_list)
selected_classes = st.sidebar.multiselect("Filtrar por Classificação:", options=classes_list)

st.sidebar.markdown("---")
st.sidebar.header("Configuração Arquitetural")
approach = st.sidebar.radio(
    "Abordagem de Consulta:",
    [
        "Abordagem C1 (Leitura Direta de Parquet)",
        "Abordagem C3 (Consultas SQL no Data Warehouse)"
    ]
)

# Funções de Leitura
def load_parquet_indicators(selected_muns, selected_classes):
    t0 = time.time()
    cols = [
        'Municipio', 'Classificacao', 'FaixaEtaria', 'Evolucao', 'DataNotificacao', 
        'Febre', 'DificuldadeRespiratoria', 'Tosse', 'Coriza', 'DorGarganta', 'Diarreia', 'Cefaleia',
        'ComorbidadePulmao', 'ComorbidadeCardio', 'ComorbidadeRenal', 'ComorbidadeDiabetes', 
        'ComorbidadeTabagismo', 'ComorbidadeObesidade'
    ]
    df = pd.read_parquet(parquet_path, columns=cols)
    
    # Otimização categórica
    for col in cols:
        if col != 'DataNotificacao':
            df[col] = df[col].astype('category')
    df['DataNotificacao'] = pd.to_datetime(df['DataNotificacao'], errors='coerce')
    
    filtered_df = df
    if selected_muns:
        filtered_df = filtered_df[filtered_df['Municipio'].isin(selected_muns)]
    if selected_classes:
        filtered_df = filtered_df[filtered_df['Classificacao'].isin(selected_classes)]
        
    # KPIs
    total = len(filtered_df)
    suspeitos = len(filtered_df[filtered_df['Classificacao'] == 'Suspeito'])
    confirmados = len(filtered_df[filtered_df['Classificacao'] == 'Confirmados'])
    
    # Gráficos
    class_counts = filtered_df['Classificacao'].value_counts()
    top_mun = filtered_df['Municipio'].value_counts().head(10)
    faixa_counts = filtered_df['FaixaEtaria'].value_counts()
    
    # Frequência dos Sintomas
    symptom_columns = ['Febre', 'DificuldadeRespiratoria', 'Tosse', 'Coriza', 'DorGarganta', 'Diarreia', 'Cefaleia']
    symptom_counts = {col: (filtered_df[col] == 'Sim').sum() for col in symptom_columns}
    df_symp = pd.Series(symptom_counts).sort_values(ascending=True)
    
    # Comorbidades em Óbitos
    df_obitos = filtered_df[filtered_df['Evolucao'].astype(str).str.contains('bito pelo COVID', na=False)]
    comorb_cols = ['ComorbidadePulmao', 'ComorbidadeCardio', 'ComorbidadeRenal', 'ComorbidadeDiabetes', 'ComorbidadeTabagismo', 'ComorbidadeObesidade']
    comorb_counts = {col: (df_obitos[col] == 'Sim').sum() for col in comorb_cols}
    df_comorb = pd.Series(comorb_counts).sort_values(ascending=True)
    
    # Evolução temporal
    temporal_df = filtered_df.dropna(subset=['DataNotificacao']).copy()
    temporal_df['MesAno'] = temporal_df['DataNotificacao'].dt.to_period('M').dt.to_timestamp()
    evolucao = temporal_df.groupby('MesAno').size()
    
    # Letalidade
    df_conf = filtered_df[filtered_df['Classificacao'] == 'Confirmados']
    top_5_mun = df_conf['Municipio'].value_counts().head(5).index
    letalidade_data = []
    if not df_conf.empty and len(top_5_mun) > 0:
        df_top5 = df_conf[df_conf['Municipio'].isin(top_5_mun)]
        for mun in top_5_mun:
            tot_conf = (df_top5['Municipio'] == mun).sum()
            tot_obitos = ((df_top5['Municipio'] == mun) & (df_top5['Evolucao'].astype(str).str.contains('bito pelo COVID', na=False))).sum()
            taxa = (tot_obitos / tot_conf) * 100 if tot_conf > 0 else 0
            letalidade_data.append({
                'Município': mun,
                'Casos Confirmados': int(tot_conf),
                'Óbitos COVID-19': int(tot_obitos),
                'Taxa de Letalidade (%)': f"{taxa:.2f}%"
            })
    df_letalidade = pd.DataFrame(letalidade_data)
    
    elapsed = time.time() - t0
    return elapsed, total, suspeitos, confirmados, class_counts, top_mun, faixa_counts, df_symp, df_comorb, evolucao, df_letalidade

def load_dw_indicators(selected_muns, selected_classes):
    t0 = time.time()
    conn = duckdb.connect(db_path)
    
    # Cláusulas WHERE
    where_clauses = []
    if selected_muns:
        muns_str = ", ".join([f"'{m}'" for m in selected_muns])
        where_clauses.append(f"l.municipio IN ({muns_str})")
    if selected_classes:
        classes_str = ", ".join([f"'{c}'" for c in selected_classes])
        where_clauses.append(f"c.classificacao IN ({classes_str})")
        
    where_str = ""
    if where_clauses:
        where_str = "WHERE " + " AND ".join(where_clauses)
        
    # 1. KPIs
    kpi_query = f"""
        SELECT 
            SUM(f.qtd_notificacao) AS total,
            SUM(CASE WHEN c.classificacao = 'Suspeito' THEN f.qtd_notificacao ELSE 0 END) AS suspeitos,
            SUM(CASE WHEN c.classificacao = 'Confirmados' THEN f.qtd_notificacao ELSE 0 END) AS confirmados
        FROM dw.fato_notificacao_covid f
        JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
        JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
        {where_str}
    """
    kpi_res = conn.execute(kpi_query).fetchone()
    total = kpi_res[0] or 0
    suspeitos = kpi_res[1] or 0
    confirmados = kpi_res[2] or 0
    
    # 2. Classificação
    class_query = f"""
        SELECT c.classificacao, SUM(f.qtd_notificacao) AS total
        FROM dw.fato_notificacao_covid f
        JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
        JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
        {where_str}
        GROUP BY c.classificacao
    """
    df_class = conn.execute(class_query).fetchdf()
    class_counts = pd.Series(df_class['total'].values, index=df_class['classificacao'].values)
    
    # 3. Top 10 Municípios (usando a view agregada se não houver filtro de classificação)
    if not selected_classes:
        # Se não há filtro de classificação, podemos usar a materialized view consolidada do mart para ganho extremo de performance!
        mart_where = ""
        if selected_muns:
            muns_str = ", ".join([f"'{m}'" for m in selected_muns])
            mart_where = f"WHERE municipio IN ({muns_str})"
            
        mun_query = f"""
            SELECT municipio, SUM(notificacoes) AS total
            FROM mart.mv_resumo_municipio_mes
            {mart_where}
            GROUP BY municipio ORDER BY total DESC LIMIT 10
        """
    else:
        mun_query = f"""
            SELECT l.municipio, SUM(f.qtd_notificacao) AS total
            FROM dw.fato_notificacao_covid f
            JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
            JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
            {where_str}
            GROUP BY l.municipio ORDER BY total DESC LIMIT 10
        """
    df_mun = conn.execute(mun_query).fetchdf()
    top_mun = pd.Series(df_mun['total'].values, index=df_mun['municipio'].values)
    
    # 4. Faixa Etária
    faixa_query = f"""
        SELECT p.faixa_etaria, SUM(f.qtd_notificacao) AS total
        FROM dw.fato_notificacao_covid f
        JOIN dw.dim_perfil_paciente p ON f.sk_perfil = p.sk_perfil
        JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
        JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
        {where_str}
        GROUP BY p.faixa_etaria
    """
    df_faixa = conn.execute(faixa_query).fetchdf()
    faixa_counts = pd.Series(df_faixa['total'].values, index=df_faixa['faixa_etaria'].values)
    
    # 5. Sintomas
    sint_query = f"""
        SELECT 
            SUM(CASE WHEN s.febre = 'Sim' THEN 1 ELSE 0 END) AS Febre,
            SUM(CASE WHEN s.dif_respiratoria = 'Sim' THEN 1 ELSE 0 END) AS DificuldadeRespiratoria,
            SUM(CASE WHEN s.tosse = 'Sim' THEN 1 ELSE 0 END) AS Tosse,
            SUM(CASE WHEN s.coriza = 'Sim' THEN 1 ELSE 0 END) AS Coriza,
            SUM(CASE WHEN s.dor_garganta = 'Sim' THEN 1 ELSE 0 END) AS DorGarganta,
            SUM(CASE WHEN s.diarreia = 'Sim' THEN 1 ELSE 0 END) AS Diarreia,
            SUM(CASE WHEN s.cefaleia = 'Sim' THEN 1 ELSE 0 END) AS Cefaleia
        FROM dw.fato_notificacao_covid f
        JOIN dw.dim_sintomas s ON f.sk_sint = s.sk_sint
        JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
        JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
        {where_str}
    """
    df_sint = conn.execute(sint_query).fetchdf()
    df_symp = df_sint.iloc[0].sort_values(ascending=True)
    
    # 6. Comorbidades em Óbitos
    como_where = "WHERE c.evolucao LIKE '%Óbito pelo COVID%'"
    if selected_muns:
        muns_str = ", ".join([f"'{m}'" for m in selected_muns])
        como_where += f" AND l.municipio IN ({muns_str})"
    if selected_classes:
        classes_str = ", ".join([f"'{c}'" for c in selected_classes])
        como_where += f" AND c.classificacao IN ({classes_str})"
        
    comorb_query = f"""
        SELECT 
            SUM(CASE WHEN cb.com_pulmao = 'Sim' THEN 1 ELSE 0 END) AS ComorbidadePulmao,
            SUM(CASE WHEN cb.com_cardio = 'Sim' THEN 1 ELSE 0 END) AS ComorbidadeCardio,
            SUM(CASE WHEN cb.com_renal = 'Sim' THEN 1 ELSE 0 END) AS ComorbidadeRenal,
            SUM(CASE WHEN cb.com_diabetes = 'Sim' THEN 1 ELSE 0 END) AS ComorbidadeDiabetes,
            SUM(CASE WHEN cb.com_tabagismo = 'Sim' THEN 1 ELSE 0 END) AS ComorbidadeTabagismo,
            SUM(CASE WHEN cb.com_obesidade = 'Sim' THEN 1 ELSE 0 END) AS ComorbidadeObesidade
        FROM dw.fato_notificacao_covid f
        JOIN dw.dim_comorbidade cb ON f.sk_como = cb.sk_como
        JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
        JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
        {como_where}
    """
    df_como = conn.execute(comorb_query).fetchdf()
    df_comorb = df_como.iloc[0].sort_values(ascending=True)
    
    # 7. Evolução temporal (usando Data Mart do mart se possível)
    if not selected_classes:
        mart_where = ""
        if selected_muns:
            muns_str = ", ".join([f"'{m}'" for m in selected_muns])
            mart_where = f"WHERE municipio IN ({muns_str})"
        temp_query = f"""
            SELECT ano_mes, SUM(notificacoes) AS total
            FROM mart.mv_resumo_municipio_mes
            {mart_where}
            GROUP BY ano_mes ORDER BY ano_mes
        """
    else:
        temp_query = f"""
            SELECT t.ano_mes, SUM(f.qtd_notificacao) AS total
            FROM dw.fato_notificacao_covid f
            JOIN dw.dim_tempo t ON f.sk_data_notificacao = t.sk_tempo
            JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
            JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
            {where_str} AND t.sk_tempo != -1
            GROUP BY t.ano_mes ORDER BY t.ano_mes
        """
    df_temp = conn.execute(temp_query).fetchdf()
    df_temp['MesAno'] = pd.to_datetime(df_temp['ano_mes'] + '-01')
    evolucao = pd.Series(df_temp['total'].values, index=df_temp['MesAno'].values)
    
    # 8. Letalidade dos Top 5 Municípios
    letal_where = "WHERE c.classificacao = 'Confirmados'"
    if selected_muns:
        muns_str = ", ".join([f"'{m}'" for m in selected_muns])
        letal_where += f" AND l.municipio IN ({muns_str})"
        
    letal_query = f"""
        WITH top_muns AS (
            SELECT l.municipio, COUNT(*) AS confirmados
            FROM dw.fato_notificacao_covid f
            JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
            JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
            {letal_where}
            GROUP BY l.municipio
            ORDER BY confirmados DESC
            LIMIT 5
        )
        SELECT 
            l.municipio AS "Município",
            SUM(CASE WHEN c.classificacao = 'Confirmados' THEN 1 ELSE 0 END) AS "Casos Confirmados",
            SUM(CASE WHEN c.classificacao = 'Confirmados' AND c.evolucao LIKE '%Óbito pelo COVID%' THEN 1 ELSE 0 END) AS "Óbitos COVID-19"
        FROM dw.fato_notificacao_covid f
        JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
        JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
        WHERE l.municipio IN (SELECT municipio FROM top_muns)
        GROUP BY l.municipio;
    """
    df_let = conn.execute(letal_query).fetchdf()
    if not df_let.empty:
        df_let['Taxa de Letalidade (%)'] = (df_let['Óbitos COVID-19'] / df_let['Casos Confirmados'] * 100).round(2).astype(str) + '%'
    df_letalidade = df_let
    
    conn.close()
    elapsed = time.time() - t0
    return elapsed, total, suspeitos, confirmados, class_counts, top_mun, faixa_counts, df_symp, df_comorb, evolucao, df_letalidade

# Executar Consulta baseada na abordagem
if approach == "Abordagem C1 (Leitura Direta de Parquet)":
    if not os.path.exists(parquet_path):
        st.error("Arquivo Parquet não encontrado! Execute o ETL primeiro para criar a staging/DW.")
        st.stop()
    with st.spinner("Lendo arquivo Parquet e aplicando transformações em Pandas..."):
        elapsed, total, suspeitos, confirmados, class_counts, top_mun, faixa_counts, df_symp, df_comorb, evolucao, df_letalidade = load_parquet_indicators(selected_muns, selected_classes)
else:
    if not os.path.exists(db_path):
        st.error("Banco de dados do Data Warehouse não encontrado! Execute o script de ETL primeiro.")
        st.stop()
    with st.spinner("Executando consultas SQL otimizadas no DuckDB (Data Warehouse)..."):
        elapsed, total, suspeitos, confirmados, class_counts, top_mun, faixa_counts, df_symp, df_comorb, evolucao, df_letalidade = load_dw_indicators(selected_muns, selected_classes)

# --- CABEÇALHO ---
st.title("🦠 Dashboard Analítico: Notificações de Covid-19")
st.markdown("**Integração Data Warehouse & Streamlit (C3)**")

# Painel de Benchmark
benchmark_col1, benchmark_col2 = st.columns([3, 1])
with benchmark_col1:
    st.info(f"⚡ **Tempo de resposta da consulta atual ({approach}):** `{elapsed:.4f} segundos`")
with benchmark_col2:
    if approach == "Abordagem C3 (Consultas SQL no Data Warehouse)":
        st.success("🚀 DW: Otimizado com Índices!")
    else:
        st.warning("⚠️ Parquet: Carga total na memória!")

# --- KPIs ---
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f'<div class="metric-container"><div class="metric-title">📊 Total de Notificações</div><div class="metric-value">{total:,}</div></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="metric-container"><div class="metric-title">👀 Casos Suspeitos</div><div class="metric-value">{suspeitos:,}</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-container"><div class="metric-title">✅ Casos Confirmados</div><div class="metric-value">{confirmados:,}</div></div>', unsafe_allow_html=True)

# --- GRÁFICOS ROW 1 ---
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.markdown("### Distribuição por Classificação")
    if not class_counts.empty:
        df_resumo = pd.DataFrame({
            'Classificação': class_counts.index,
            'Notificações': class_counts.values
        })
        st.bar_chart(df_resumo.set_index('Classificação'), horizontal=True, color="#3B82F6")
    else:
        st.info("Sem dados de classificação.")

with col_chart2:
    st.markdown("### Top 10 Municípios com Mais Notificações")
    if not top_mun.empty:
        df_mun = pd.DataFrame({
            'Município': top_mun.index,
            'Notificações': top_mun.values
        }).sort_values('Notificações', ascending=True)
        st.bar_chart(df_mun.set_index('Município'), horizontal=True, color="#10B981")
    else:
        st.info("Sem dados de municípios.")

# --- GRÁFICOS ROW 2 ---
st.markdown("---")
col_chart3, col_chart4 = st.columns(2)

with col_chart3:
    st.markdown("### Notificações por Faixa Etária")
    if not faixa_counts.empty:
        df_faixa = pd.DataFrame({
            'Faixa Etária': faixa_counts.index,
            'Notificações': faixa_counts.values
        }).sort_values('Notificações', ascending=True)
        st.bar_chart(df_faixa.set_index('Faixa Etária'), horizontal=True, color="#F59E0B")
    else:
        st.info("Sem dados para faixa etária.")

with col_chart4:
    st.markdown("### Frequência dos Sintomas (Casos com 'Sim')")
    if not df_symp.empty and df_symp.sum() > 0:
        st.bar_chart(pd.DataFrame({'Sintoma': df_symp.index, 'Registros': df_symp.values}).set_index('Sintoma'), horizontal=True, color="#EC4899")
    else:
        st.info("Sem dados de sintomas.")

# --- GRÁFICOS ROW 3 ---
st.markdown("---")
col_chart5, col_chart6 = st.columns(2)

with col_chart5:
    st.markdown("### Comorbidades Presentes em Óbitos por COVID-19")
    if not df_comorb.empty and df_comorb.sum() > 0:
        st.bar_chart(pd.DataFrame({'Comorbidade': df_comorb.index, 'Óbitos': df_comorb.values}).set_index('Comorbidade'), horizontal=True, color="#EF4444")
    else:
        st.info("Sem dados de comorbidades.")

with col_chart6:
    st.markdown("### Evolução Temporal das Notificações")
    if not evolucao.empty:
        st.line_chart(evolucao, color="#8B5CF6")
    else:
        st.info("Sem dados temporais disponíveis.")

# --- LETALIDADE TABLE ---
st.markdown("---")
st.markdown("### Taxa de Letalidade - Top 5 Municípios com Mais Casos Confirmados")
if not df_letalidade.empty:
    st.dataframe(df_letalidade, use_container_width=True, hide_index=True)
else:
    st.info("Sem dados para letalidade.")
