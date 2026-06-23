import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Configuração da página
st.set_page_config(page_title="Dashboard Covid-19", layout="wide", page_icon="🦠")

# Custom CSS para aparência estética moderna
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
    .chart-container {
        background-color: #1F2937;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        border: 1px solid #374151;
        margin-bottom: 20px;
    }
</style>
""", unsafe_allow_html=True)

# Função para carregar os dados em Cache
@st.cache_data(show_spinner="Carregando dados. Isso pode demorar na primeira vez...")
def load_data():
    # Colunas todas que mapeiam as análises do Notebook
    cols = [
        'Municipio', 'Classificacao', 'FaixaEtaria', 'Evolucao', 'DataNotificacao', 
        'Febre', 'DificuldadeRespiratoria', 'Tosse', 'Coriza', 'DorGarganta', 'Diarreia', 'Cefaleia',
        'ComorbidadePulmao', 'ComorbidadeCardio', 'ComorbidadeRenal', 'ComorbidadeDiabetes', 
        'ComorbidadeTabagismo', 'ComorbidadeObesidade'
    ]
    df = pd.read_parquet('MICRODADOS.parquet', columns=cols)
    
    # Otimização de Memória absurda para o Streamlit (Trocando strings genéricas por Categories)
    for col in cols:
        if col != 'DataNotificacao':
            df[col] = df[col].astype('category')
            
    # Preparando os formatos de data para as análises temporais
    df['DataNotificacao'] = pd.to_datetime(df['DataNotificacao'], errors='coerce')
    
    return df

st.title("🦠 Dashboard Analítico: Notificações de Covid-19")
st.markdown("**Bem-vindo!** Aqui você pode explorar interativamente os dados das notificações de Covid-19 utilizando a barra lateral para aplicar filtros no **banco de dados completo**.")

# Carregar dados
try:
    df = load_data()
except FileNotFoundError:
    st.error("Arquivo 'MICRODADOS.parquet' não encontrado. Por favor, execute o script de conversão do CSV primeiro.")
    st.stop()

# --- SIDEBAR: Filtros ---
st.sidebar.header("Filtros Interativos")

# Seleção de Municípios
municipios_list = df['Municipio'].dropna().unique().tolist()
municipios_list.sort()
municipio_filter = st.sidebar.multiselect("Filtrar por Municípios:", options=municipios_list)

# Seleção de Classificação
classificacao_list = df['Classificacao'].dropna().unique().tolist()
classificacao_list.sort()
classificacao_filter = st.sidebar.multiselect("Filtrar por Classificação:", options=classificacao_list)

# Aplicação dos filtros
filtered_df = df.copy()

if municipio_filter:
    filtered_df = filtered_df[filtered_df['Municipio'].isin(municipio_filter)]
    
if classificacao_filter:
    filtered_df = filtered_df[filtered_df['Classificacao'].isin(classificacao_filter)]

# --- MAIN: KPIs ---
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f'<div class="metric-container"><div class="metric-title">📊 Total de Notificações</div><div class="metric-value">{len(filtered_df):,}</div></div>', unsafe_allow_html=True)
    
with col2:
    suspeitos = len(filtered_df[filtered_df['Classificacao'] == 'Suspeito'])
    st.markdown(f'<div class="metric-container"><div class="metric-title">👀 Casos Suspeitos</div><div class="metric-value">{suspeitos:,}</div></div>', unsafe_allow_html=True)

with col3:
    confirmados = len(filtered_df[filtered_df['Classificacao'] == 'Confirmados'])
    st.markdown(f'<div class="metric-container"><div class="metric-title">✅ Casos Confirmados</div><div class="metric-value">{confirmados:,}</div></div>', unsafe_allow_html=True)

# --- MAIN: Gráficos Originais (Classificação e Top Municípios) ---
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.markdown("### Distribuição por Classificação")
    frequencia_abs = filtered_df['Classificacao'].value_counts()
    
    if not frequencia_abs.empty:
        df_resumo = pd.DataFrame({
            'Classificação': frequencia_abs.index,
            'Notificações': frequencia_abs.values
        })
        st.bar_chart(df_resumo.set_index('Classificação'), horizontal=True, color="#3B82F6")
    else:
        st.info("Não há dados de classificação com os filtros atuais.")

with col_chart2:
    st.markdown("### Top 10 Municípios com Mais Notificações")
    top_municipios = filtered_df['Municipio'].value_counts().head(10)
    
    if not top_municipios.empty:
        df_mun = pd.DataFrame({
            'Município': top_municipios.index,
            'Notificações': top_municipios.values
        })
        df_mun = df_mun.sort_values('Notificações', ascending=True)
        st.bar_chart(df_mun.set_index('Município'), horizontal=True, color="#10B981")
    else:
        st.info("Não há dados de municípios com os filtros atuais.")

# --- MAIN: Novos Gráficos Adicionais ---
st.markdown("---")
st.markdown("## Análises Adicionais do Dataset")

col_chart3, col_chart4 = st.columns(2)

with col_chart3:
    st.markdown("### Notificações por Faixa Etária")
    faixa_etaria_counts = filtered_df['FaixaEtaria'].value_counts()
    
    if not faixa_etaria_counts.empty:
        df_faixa = pd.DataFrame({
            'Faixa Etária': faixa_etaria_counts.index,
            'Notificações': faixa_etaria_counts.values
        }).sort_values('Notificações', ascending=True)
        st.bar_chart(df_faixa.set_index('Faixa Etária'), horizontal=True, color="#F59E0B")
    else:
        st.info("Sem dados para faixa etária.")

with col_chart4:
    st.markdown("### Frequência dos Sintomas (Casos com 'Sim')")
    symptom_columns = ['Febre', 'DificuldadeRespiratoria', 'Tosse', 'Coriza', 'DorGarganta', 'Diarreia', 'Cefaleia']
    symptom_counts = {col: (filtered_df[col] == 'Sim').sum() for col in symptom_columns}
    df_symp = pd.Series(symptom_counts).sort_values(ascending=True)
    if not df_symp.empty and df_symp.sum() > 0:
        st.bar_chart(pd.DataFrame({'Sintoma': df_symp.index, 'Registros': df_symp.values}).set_index('Sintoma'), horizontal=True, color="#EC4899")
    else:
        st.info("Sem dados de sintomas para os filtros atuais.")

st.markdown("---")
col_chart5, col_chart6 = st.columns(2)

with col_chart5:
    st.markdown("### Comorbidades Presentes em Óbitos por COVID-19")
    # Utilizamos str.contains ou comparativo exato por ser categórico
    df_obitos = filtered_df[filtered_df['Evolucao'].astype(str).str.contains('bito pelo COVID', na=False)]
    comorb_cols = ['ComorbidadePulmao', 'ComorbidadeCardio', 'ComorbidadeRenal', 'ComorbidadeDiabetes', 'ComorbidadeTabagismo', 'ComorbidadeObesidade']
    comorb_counts = {col: (df_obitos[col] == 'Sim').sum() for col in comorb_cols}
    df_comorb = pd.Series(comorb_counts).sort_values(ascending=True)
    
    if not df_comorb.empty and df_comorb.sum() > 0:
        st.bar_chart(pd.DataFrame({'Comorbidade': df_comorb.index, 'Óbitos': df_comorb.values}).set_index('Comorbidade'), horizontal=True, color="#EF4444")
    else:
        st.info("Sem óbitos confirmados ou sem dados registrados na seleção atual.")

with col_chart6:
    st.markdown("### Evolução Temporal das Notificações")
    if not filtered_df['DataNotificacao'].isna().all():
        temporal_df = filtered_df.dropna(subset=['DataNotificacao']).copy()
        # Converte para Período Mês e depois timestamp pro index
        temporal_df['MesAno'] = temporal_df['DataNotificacao'].dt.to_period('M').dt.to_timestamp()
        evolucao = temporal_df.groupby('MesAno').size()
        
        if not evolucao.empty:
            st.line_chart(evolucao, color="#8B5CF6")
        else:
            st.info("Sem dados temporais suficientes.")
    else:
        st.info("Sem dados temporais disponíveis no filtro atual.")

st.markdown("---")
st.markdown("### Taxa de Letalidade - Top 5 Municípios com Mais Casos Confirmados")

# Realiza a extração dos 5 Municípios na Base Filtrada e calcula a Letalidade
df_conf = filtered_df[filtered_df['Classificacao'] == 'Confirmados']
top_5_mun = df_conf['Municipio'].value_counts().head(5).index

if not df_conf.empty and len(top_5_mun) > 0:
    df_top5 = df_conf[df_conf['Municipio'].isin(top_5_mun)]
    
    # Montar Dataset
    letalidade_data = []
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
    # Mostra a tabela na tela de forma otimizada
    st.dataframe(df_letalidade, use_container_width=True, hide_index=True)
else:
    st.info("Não há casos classificados como 'Confirmados' nesses filtros para processar letalidade.")
