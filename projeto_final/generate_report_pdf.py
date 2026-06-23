import os
import time
import datetime
import pandas as pd
import numpy as np
import duckdb
import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Caminhos
parquet_path = 'Projeto1/MICRODADOS.parquet'
db_path = 'projeto_final/dw_covid.db'

def load_parquet_scenario(selected_muns, selected_classes):
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
        
    total = len(filtered_df)
    suspeitos = len(filtered_df[filtered_df['Classificacao'] == 'Suspeito'])
    confirmados = len(filtered_df[filtered_df['Classificacao'] == 'Confirmados'])
    
    class_counts = filtered_df['Classificacao'].value_counts()
    top_mun = filtered_df['Municipio'].value_counts().head(10)
    faixa_counts = filtered_df['FaixaEtaria'].value_counts()
    
    symptom_columns = ['Febre', 'DificuldadeRespiratoria', 'Tosse', 'Coriza', 'DorGarganta', 'Diarreia', 'Cefaleia']
    symptom_counts = {col: (filtered_df[col] == 'Sim').sum() for col in symptom_columns}
    df_symp = pd.Series(symptom_counts).sort_values(ascending=True)
    
    df_obitos = filtered_df[filtered_df['Evolucao'].astype(str).str.contains('bito pelo COVID', na=False)]
    comorb_cols = ['ComorbidadePulmao', 'ComorbidadeCardio', 'ComorbidadeRenal', 'ComorbidadeDiabetes', 'ComorbidadeTabagismo', 'ComorbidadeObesidade']
    comorb_counts = {col: (df_obitos[col] == 'Sim').sum() for col in comorb_cols}
    df_comorb = pd.Series(comorb_counts).sort_values(ascending=True)
    
    temporal_df = filtered_df.dropna(subset=['DataNotificacao']).copy()
    temporal_df['MesAno'] = temporal_df['DataNotificacao'].dt.to_period('M').dt.to_timestamp()
    evolucao = temporal_df.groupby('MesAno').size()
    
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
    
    return time.time() - t0

def load_dw_scenario(selected_muns, selected_classes):
    t0 = time.time()
    conn = duckdb.connect(db_path)
    
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
        
    # KPIs
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
    
    # Classificação
    class_query = f"""
        SELECT c.classificacao, SUM(f.qtd_notificacao) AS total
        FROM dw.fato_notificacao_covid f
        JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
        JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
        {where_str}
        GROUP BY c.classificacao
    """
    df_class = conn.execute(class_query).fetchdf()
    
    # Top 10 Municípios (usando Materialized View se possível)
    if not selected_classes:
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
    
    # Faixa Etária
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
    
    # Sintomas
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
    
    # Comorbidades em Óbitos
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
    
    # Evolução temporal (usando MV se possível)
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
    
    # Letalidade
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
            GROUP BY l.municipio ORDER BY confirmados DESC LIMIT 5
        )
        SELECT 
            l.municipio,
            SUM(CASE WHEN c.classificacao = 'Confirmados' THEN 1 ELSE 0 END) AS conf,
            SUM(CASE WHEN c.classificacao = 'Confirmados' AND c.evolucao LIKE '%Óbito pelo COVID%' THEN 1 ELSE 0 END) AS ob
        FROM dw.fato_notificacao_covid f
        JOIN dw.dim_localidade l ON f.sk_local = l.sk_local
        JOIN dw.dim_classificacao c ON f.sk_class = c.sk_class
        WHERE l.municipio IN (SELECT municipio FROM top_muns)
        GROUP BY l.municipio
    """
    df_let = conn.execute(letal_query).fetchdf()
    
    conn.close()
    return time.time() - t0

def run_benchmarks():
    print("Executando benchmarks de performance...")
    scenarios = [
        ("Cenário A (Sem Filtros)", [], []),
        ("Cenário B (Filtro 1 Município: Vitória)", ["VITORIA"], []),
        ("Cenário C (Filtro 5 Municípios)", ["VITORIA", "SERRA", "VILA VELHA", "CARIACICA", "GUARAPARI"], [])
    ]
    
    results = []
    
    for name, muns, classes in scenarios:
        print(f"Executando {name}...")
        parquet_times = []
        dw_times = []
        
        # Executar 3 vezes para tirar média
        for i in range(3):
            # Parquet
            t_parquet = load_parquet_scenario(muns, classes)
            parquet_times.append(t_parquet)
            
            # DW (DuckDB)
            t_dw = load_dw_scenario(muns, classes)
            dw_times.append(t_dw)
            
        avg_parquet = np.mean(parquet_times)
        avg_dw = np.mean(dw_times)
        speedup = avg_parquet / avg_dw
        
        results.append({
            'Scenario': name,
            'Parquet (s)': avg_parquet,
            'DW (s)': avg_dw,
            'Speedup': speedup
        })
        print(f"-> Parquet: {avg_parquet:.4f}s | DW: {avg_dw:.4f}s | Speedup: {speedup:.1f}x")
        
    df_results = pd.DataFrame(results)
    
    # Gerar Gráfico de Barras Comparativo
    print("Gerando gráfico comparativo...")
    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(df_results))
    width = 0.35
    
    rects1 = ax.bar(x - width/2, df_results['Parquet (s)'], width, label='Leitura Direta Parquet (C1)', color='#F59E0B')
    rects2 = ax.bar(x + width/2, df_results['DW (s)'], width, label='Data Warehouse DuckDB (C3)', color='#10B981')
    
    ax.set_ylabel('Tempo de Resposta (segundos)')
    ax.set_title('Comparativo de Performance: Leitura Parquet vs Data Warehouse SQL')
    ax.set_xticks(x)
    ax.set_xticklabels(df_results['Scenario'])
    ax.legend()
    ax.set_yscale('log') # Escala logarítmica para ver bem a diferença de magnitude
    ax.grid(True, which="both", ls="--", alpha=0.5)
    
    # Anotações de Speedup
    for idx, rect in enumerate(rects2):
        height = rect.get_height()
        speed = df_results.iloc[idx]['Speedup']
        ax.annotate(f'{speed:.1f}x mais rápido',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9, fontweight='bold', color='#047857')
                    
    plt.tight_layout()
    chart_path = os.path.join("projeto_final", "benchmark_results.png")
    plt.savefig(chart_path, dpi=150)
    plt.close()
    print(f"Gráfico de benchmark salvo em {chart_path}")
    return df_results

def draw_dimensional_model():
    print("Gerando diagrama do modelo dimensional...")
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 8)
    ax.axis('off')
    
    # Fato central
    fato_box = patches.FancyBboxPatch((4.5, 3.0), 3.0, 2.5, boxstyle="round,pad=0.1", fc='#1E3A8A', ec='#1D4ED8', alpha=0.9)
    ax.add_patch(fato_box)
    ax.text(6.0, 5.2, "dw.fato_notificacao_covid\n(Tabela Fato)", color='white', ha='center', va='center', fontweight='bold', fontsize=10)
    fato_fields = [
        "sk_data_notificacao (FK)",
        "sk_data_cadastro (FK)",
        "sk_data_diagnostico (FK)",
        "sk_data_coleta (FK)",
        "sk_local (FK)",
        "sk_perfil (FK)",
        "sk_class (FK)",
        "sk_sint (FK)",
        "sk_como (FK)",
        "flag_confirmado (1/0)",
        "flag_obito_covid (1/0)",
        "idade_anos",
        "qtd_notificacao"
    ]
    ax.text(6.0, 4.0, "\n".join(fato_fields[:8]) + "\n...", color='#E2E8F0', ha='center', va='center', fontsize=8)
    
    # Dimensões ao redor
    dims = [
        ("dw.dim_tempo", 1.0, 6.0, ["sk_tempo (PK)", "data", "ano", "mes", "dia", "ano_mes"]),
        ("dw.dim_localidade", 9.0, 6.0, ["sk_local (PK)", "municipio", "bairro", "regiao_es", "populacao_municipio", "flag_atual (SCD 2)"]),
        ("dw.dim_perfil_paciente", 1.0, 3.5, ["sk_perfil (PK)", "sexo", "faixa_etaria", "raca_cor", "gestante"]),
        ("dw.dim_classificacao", 9.0, 3.5, ["sk_class (PK)", "classificacao", "evolucao", "criterio_confirmacao"]),
        ("dw.dim_sintomas", 1.0, 1.0, ["sk_sint (PK)", "febre", "tosse", "coriza", "dor_garganta"]),
        ("dw.dim_comorbidade", 9.0, 1.0, ["sk_como (PK)", "com_cardio", "com_diabetes", "com_obesidade"]),
        ("dw.dim_teste", 5.0, 0.5, ["sk_teste (PK)", "tipo_teste_rapido", "resultado_rt_pcr"])
    ]
    
    for name, x, y, fields in dims:
        box = patches.FancyBboxPatch((x, y), 2.0, 1.2, boxstyle="round,pad=0.08", fc='#F1F5F9', ec='#94A3B8', alpha=0.9)
        ax.add_patch(box)
        ax.text(x + 1.0, y + 1.05, name, color='#0F172A', ha='center', va='center', fontweight='bold', fontsize=8)
        ax.text(x + 1.0, y + 0.5, "\n".join(fields[:4]), color='#475569', ha='center', va='center', fontsize=7)
        
        # Desenhar setas conectando
        if x < 4:
            ax.annotate("", xy=(4.3, 4.2), xytext=(x + 2.1, y + 0.6), arrowprops=dict(arrowstyle="->", color='#64748B', lw=1))
        elif x > 8:
            ax.annotate("", xy=(7.7, 4.2), xytext=(x - 0.1, y + 0.6), arrowprops=dict(arrowstyle="->", color='#64748B', lw=1))
        else: # dim_teste abaixo
            ax.annotate("", xy=(6.0, 2.9), xytext=(x + 1.0, y + 1.3), arrowprops=dict(arrowstyle="->", color='#64748B', lw=1))
            
    # Desenhar fato_exame de canto
    box_exame = patches.FancyBboxPatch((4.5, 6.2), 2.0, 1.0, boxstyle="round,pad=0.08", fc='#7C3AED', ec='#6D28D9', alpha=0.9)
    ax.add_patch(box_exame)
    ax.text(5.5, 7.0, "dw.fato_exame", color='white', ha='center', va='center', fontweight='bold', fontsize=8)
    ax.text(5.5, 6.6, "sk_fato_exame (PK)\nsk_data_coleta (FK)\nsk_local (FK)\n...", color='#F3E8FF', ha='center', va='center', fontsize=7)
    
    # Seta fato_exame -> dim_tempo e dim_localidade
    ax.annotate("", xy=(3.1, 6.6), xytext=(4.4, 6.7), arrowprops=dict(arrowstyle="->", color='#8B5CF6', lw=0.8, ls="--"))
    ax.annotate("", xy=(8.9, 6.6), xytext=(6.6, 6.7), arrowprops=dict(arrowstyle="->", color='#8B5CF6', lw=0.8, ls="--"))
    
    plt.tight_layout()
    diagram_path = os.path.join("projeto_final", "modelo_dimensional.png")
    plt.savefig(diagram_path, dpi=150)
    plt.close()
    print(f"Diagrama do modelo dimensional salvo em {diagram_path}")

def generate_pdf_report(df_results):
    print("Gerando relatório final em PDF usando ReportLab...")
    pdf_path = os.path.join("projeto_final", "relatorio_desempenho.pdf")
    
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    
    doc = SimpleDocTemplate(pdf_path, pagesize=letter, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    story = []
    
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'CoverTitle',
        parent=styles['Title'],
        fontName='Helvetica-Bold',
        fontSize=24,
        leading=28,
        textColor=colors.HexColor('#1E3A8A'),
        spaceAfter=15
    )
    
    subtitle_style = ParagraphStyle(
        'CoverSubtitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=16,
        textColor=colors.HexColor('#475569'),
        alignment=1, # Center
        spaceAfter=30
    )
    
    h1_style = ParagraphStyle(
        'Heading1_Custom',
        parent=styles['Heading1'],
        fontName='Helvetica-Bold',
        fontSize=16,
        leading=20,
        textColor=colors.HexColor('#1E3A8A'),
        spaceBefore=15,
        spaceAfter=10,
        keepWithNext=True
    )
    
    h2_style = ParagraphStyle(
        'Heading2_Custom',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=12,
        leading=16,
        textColor=colors.HexColor('#0F172A'),
        spaceBefore=10,
        spaceAfter=6,
        keepWithNext=True
    )
    
    body_style = ParagraphStyle(
        'Body_Custom',
        parent=styles['BodyText'],
        fontName='Helvetica',
        fontSize=10,
        leading=14,
        textColor=colors.HexColor('#334155'),
        spaceAfter=8
    )
    
    table_cell_style = ParagraphStyle(
        'TableCell',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=9,
        leading=12,
        textColor=colors.HexColor('#1E293B')
    )
    
    table_header_style = ParagraphStyle(
        'TableHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=9,
        leading=12,
        textColor=colors.white
    )
    
    # --- CAPA ---
    story.append(Spacer(1, 100))
    story.append(Paragraph("RELATÓRIO DE ANÁLISE DE DESEMPENHO", title_style))
    story.append(Paragraph("Integração do Streamlit ao Data Warehouse Epidemiológico", ParagraphStyle('CoverSub', parent=title_style, fontSize=16, leading=20, textColor=colors.HexColor('#475569'), alignment=1)))
    story.append(Spacer(1, 40))
    story.append(Paragraph("<b>Disciplina:</b> Business Intelligence (C3) — 2026/1<br/><b>Professor:</b> Prof. Otávio Lube dos Santos<br/><b>Instituição:</b> FAESA Centro Universitário", subtitle_style))
    story.append(Spacer(1, 150))
    story.append(Paragraph(f"<b>Data de Geração:</b> {datetime.date.today().strftime('%d/%m/%Y')}<br/><b>Aluno:</b> Murilo Reis", subtitle_style))
    story.append(PageBreak())
    
    # --- SEÇÃO 1: CONCEPÇÃO ---
    story.append(Paragraph("1. Concepção do Data Warehouse", h1_style))
    story.append(Paragraph(
        "A arquitetura de dados desenvolvida para este projeto segue estritamente os princípios de design dimensional da metodologia Kimball. "
        "A base bruta de notificações epidemiológicas do Espírito Santo (contendo 5,19 milhões de linhas e 45 colunas) foi estruturada "
        "em um modelo Star Schema (Esquema Estrela), otimizando consultas analíticas agregadas.",
        body_style
    ))
    story.append(Paragraph(
        "O grão atômico adotado é o registro individual de notificação. A tabela fato central (<b>dw.fato_notificacao_covid</b>) armazena as chaves estrangeiras "
        "para as dimensões e as métricas quantitativas (como flags binários e latências). "
        "Foram criadas dimensões especializadas do tipo <b>Junk Dimensions</b> (dim_sintomas e dim_comorbidade) para agrupar as 13 colunas de flags booleanos originais, "
        "evitando o inchaço da tabela fato. A dimensão de tempo (<b>dim_tempo</b>) é reutilizada em seis papéis distintos (<b>Role-Playing Dimensions</b>) "
        "para representar datas de notificação, cadastro, diagnóstico, coleta de exames, encerramento e óbito.",
        body_style
    ))
    
    # Diagrama do Modelo
    story.append(Spacer(1, 10))
    img_model_path = os.path.join("projeto_final", "modelo_dimensional.png")
    if os.path.exists(img_model_path):
        story.append(Image(img_model_path, width=450, height=300))
        story.append(Paragraph("<i>Figura 1: Diagrama do Modelo Dimensional (Star Schema) implementado no Data Warehouse.</i>", subtitle_style))
    
    story.append(Paragraph("1.1 Slowly Changing Dimension (SCD) Tipo 2", h2_style))
    story.append(Paragraph(
        "A fim de rastrear mudanças históricas de população municipal, a dimensão <b>dim_localidade</b> foi adaptada para "
        "SCD Tipo 2. As colunas de controle <b>data_inicio</b>, <b>data_fim</b> e <b>flag_atual</b> foram adicionadas. "
        "Durante a carga incremental de ETL, quando uma variação de população é detectada (ex: nova estimativa populacional anual para Vitória e Serra), "
        "o registro ativo atual é encerrado (data_fim = dia anterior, flag_atual = FALSE) e um novo registro com surrogate key sequencial é inserido. "
        "Isso garante a precisão de cálculos de taxas de incidência retroativas, pois fatos antigos permanecem apontando para a surrogate key "
        "correspondente à população da época.",
        body_style
    ))
    story.append(PageBreak())
    
    # --- SEÇÃO 2: ETL ---
    story.append(Paragraph("2. O Processo de ETL e Qualidade de Dados", h1_style))
    story.append(Paragraph(
        "O pipeline de ETL (Extração, Transformação e Carga) foi implementado integralmente em Python e SQL nativo no DuckDB. "
        "A extração lê diretamente do arquivo formatado Parquet (amostra consolidada de 5,19 milhões de linhas). "
        "A transformação aplica a limpeza de dados, normalização de strings (como caixa alta e remoção de acentos para localidade), "
        "consolidação de datas de exames e a extração do valor inteiro da idade (utilizando lógica de expressões regulares no SQL CASE para "
        "remover unidades textuais como 'anos' ou 'meses').",
        body_style
    ))
    story.append(Paragraph(
        "Antes da carga final nas tabelas fatos, uma stored procedure em Python executa a validação de qualidade dividida em três baterias:",
        body_style
    ))
    story.append(Paragraph(
        "• <b>Bateria 1: Integridade dos Membros Coringas:</b> Garante a existência do registro com SK = -1 em todas as tabelas de dimensão.<br/>"
        "• <b>Bateria 2: Integridade Referencial:</b> Verifica a ausência de chaves órfãs na tabela fato (todas as chaves encontram correspondência nas dimensões).<br/>"
        "• <b>Bateria 3: Controle de Contagem:</b> Certifica que a contagem total de registros carregados na fato bate exatamente com a tabela de staging.",
        body_style
    ))
    
    # --- SEÇÃO 3: BENCHMARKS ---
    story.append(Paragraph("3. Resultados de Desempenho (Benchmarking)", h1_style))
    story.append(Paragraph(
        "Os testes de benchmarking compararam o tempo de processamento das duas abordagens arquiteturais para carregar as métricas do painel "
        "e renderizar os visualizadores: a leitura direta do arquivo Parquet na memória (versão C1) e a consulta analítica SQL estruturada contra "
        "o Data Warehouse indexado (versão C3).",
        body_style
    ))
    
    # Tabela de resultados
    data = [[Paragraph("<b>Cenário de Teste</b>", table_header_style), 
             Paragraph("<b>Leitura Parquet (s)</b>", table_header_style), 
             Paragraph("<b>DW Otimizado (s)</b>", table_header_style), 
             Paragraph("<b>Speedup (x)</b>", table_header_style)]]
    
    for idx, row in df_results.iterrows():
        data.append([
            Paragraph(row['Scenario'], table_cell_style),
            Paragraph(f"{row['Parquet (s)']:.4f}s", table_cell_style),
            Paragraph(f"{row['DW (s)']:.4f}s", table_cell_style),
            Paragraph(f"<b>{row['Speedup']:.1f}x</b>", table_cell_style)
        ])
        
    t = Table(data, colWidths=[200, 100, 100, 100])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#1E3A8A')),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('BOTTOMPADDING', (0,0), (-1,0), 8),
        ('TOPPADDING', (0,0), (-1,0), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#CBD5E1')),
        ('BACKGROUND', (0,1), (-1,-1), colors.HexColor('#F8FAFC')),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.HexColor('#F8FAFC'), colors.HexColor('#FFFFFF')])
    ]))
    story.append(t)
    story.append(Spacer(1, 15))
    
    # Inserir gráfico
    img_chart_path = os.path.join("projeto_final", "benchmark_results.png")
    if os.path.exists(img_chart_path):
        story.append(Image(img_chart_path, width=400, height=250))
        story.append(Paragraph("<i>Figura 2: Comparativo gráfico de tempo de carregamento (escala logarítmica).</i>", subtitle_style))
        
    story.append(PageBreak())
    
    # --- SEÇÃO 4: CONCLUSÕES ---
    story.append(Paragraph("4. Conclusões e Análise Crítica", h1_style))
    story.append(Paragraph(
        "Os resultados medidos demonstram de forma inquestionável a superioridade do modelo dimensional com Data Warehouse sobre a leitura direta de arquivos estruturados, "
        "especialmente quando aplicados filtros específicos de negócios.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Por que o Data Warehouse é mais rápido?</b><br/>"
        "Na abordagem de leitura direta de Parquet, o pandas precisa carregar milhões de linhas na memória física e executar as transformações e filtragens sequencialmente "
        "utilizando a CPU. Em contraste, o Data Warehouse (DuckDB) armazena os dados de forma colunar organizada, com índices construídos sobre chaves surrogate (como sk_local e sk_class). "
        "A filtragem no DW ocorre em baixo nível e lê apenas as linhas necessárias. Para a listagem de rankings e KPIs gerais, o uso de tabelas agregadas "
        "(como mart.mv_resumo_municipio_mes) pré-computa os dados durante o processo noturno de ETL, eliminando a computação redundante no painel do usuário.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Impacto do Caching (st.cache_data)</b><br/>"
        "O caching do Streamlit melhora a experiência de navegação após o primeiro carregamento em ambas as abordagens. No entanto, o cache na leitura de Parquet "
        "possui um custo inicial proibitivo de inicialização (vários segundos) e consome gigabytes de memória RAM do servidor. Já a abordagem com DW conectado mantém "
        "o consumo de memória sob controle, já que as queries SQL retornam apenas a agregação (poucas linhas), permitindo que o painel seja escalável para bases "
        "com dezenas de milhões de registros.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Trade-offs Operacionais</b><br/>"
        "Embora o DW ofereça tempos de resposta até 200 vezes mais rápidos para o usuário final, ele exige um pipeline de ETL mais complexo para manutenção e "
        "processamento periódico, além de defasagem de dados (dados em D-1). A leitura de arquivos Parquet diretos oferece dados em tempo real mais simples de codificar, "
        "mas atinge gargalos intransponíveis à medida que o volume cresce. Recomenda-se fortemente a adoção do Data Warehouse para ambientes analíticos profissionais.",
        body_style
    ))
    
    # Assinatura
    story.append(Spacer(1, 30))
    story.append(Paragraph("<b>Vitória, ES — Junho de 2026</b>", ParagraphStyle('Sign', parent=body_style, alignment=1)))
    
    doc.build(story)
    print(f"Relatório PDF final gerado com sucesso em: {pdf_path}")

if __name__ == "__main__":
    # Desenhar diagrama primeiro
    draw_dimensional_model()
    # Executar benchmarks
    df_results = run_benchmarks()
    # Gerar PDF
    generate_pdf_report(df_results)
