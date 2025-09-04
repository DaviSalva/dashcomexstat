import streamlit as st
import pandas as pd
import requests
import warnings
import plotly.express as px
import plotly.graph_objects as go
import re
from datetime import datetime
import os
import zipfile 

## Config Inicial
warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
BASE_URL = "https://api-comexstat.mdic.gov.br"
ANO_ATUAL = datetime.now().year

try:
    DIRETORIO_ATUAL = os.path.dirname(os.path.realpath(__file__))
except NameError: # Ocorre quando executado interativamente
    DIRETORIO_ATUAL = os.getcwd()
DIRETORIO_DADOS = os.path.join(DIRETORIO_ATUAL, "dados")
CAMINHO_NCM_TARIFADOS = os.path.join(DIRETORIO_DADOS, "lista_ncm_tarifados.csv")

st.set_page_config(
    page_title="Dashboard de Análise de Impacto",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 1. FUNÇÕES DE CARREGAMENTO E PROCESSAMENTO ---
@st.cache_data
def carregar_tabelas_auxiliares(diretorio_dados):
    try:
        df_paises = pd.read_csv(os.path.join(diretorio_dados, "PAIS.csv"), sep=';', encoding='latin-1', dtype={'CO_PAIS': str})
        df_ufs = pd.read_csv(os.path.join(diretorio_dados, "UF.csv"), sep=';', encoding='latin-1', dtype={'CO_UF': str})
        df_ncm = pd.read_csv(os.path.join(diretorio_dados, "NCM.csv"), sep=';', encoding='latin-1', dtype={'CO_NCM': str})

        # Filtros para limpar os dados
        df_paises = df_paises[~df_paises['NO_PAIS'].isin(['Bancos Centrais', 'A Designar'])]
        ufs_validas = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
            'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        df_ufs = df_ufs[df_ufs['SG_UF'].isin(ufs_validas)]
        return df_paises, df_ufs, df_ncm
    except FileNotFoundError as e:
        st.error(f"Erro: Arquivo auxiliar não encontrado no diretório 'dados'. Detalhe: {e}")
        st.stop()

@st.cache_data
def carregar_ncm_tarifados(caminho_arquivo):
    try:
        df = pd.read_csv(caminho_arquivo, dtype={'CO_NCM': str})
        return set(df['CO_NCM'])
    except FileNotFoundError:
        st.warning(f"Arquivo do Monitor de Tarifados não encontrado: {caminho_arquivo}")
        return set()

@st.cache_data
def carregar_dados_locais(tipo_fluxo: str, arquivo_sufixo: str):
    nome_arquivo = f"{tipo_fluxo.lower()}_{arquivo_sufixo}.parquet"
    caminho_completo = os.path.join(DIRETORIO_DADOS, nome_arquivo)
    try:
        return pd.read_parquet(caminho_completo)
    except FileNotFoundError:
        st.error(f"Arquivo de dados '{nome_arquivo}' não encontrado. Verifique se os scripts de pré-processamento foram executados.")
        st.stop()

# Função auxiliar para processar dados de Saldo Comercial
def processar_df_saldo(df_raw, lista_ncm_filtro):
    """Filtra, renomeia e formata o DataFrame para a análise de saldo."""
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()
    
    df_filtrado = df_raw.copy()
    if lista_ncm_filtro:
        df_filtrado = df_filtrado[df_filtrado['coNcm'].isin(lista_ncm_filtro)]
    
    if not df_filtrado.empty:
        df_filtrado['ncm'] = df_filtrado['ncm'].fillna(df_filtrado['coNcm'])
        df_final = df_filtrado.rename(columns={'ncm': 'Produto', 'metricFOB': 'Valor FOB (US$)', 'year': 'Ano', 'monthNumber': 'Mês', 'state': 'UF'})
        df_final['Valor FOB (US$)'] = pd.to_numeric(df_final['Valor FOB (US$)'], errors='coerce')
        df_final['Data'] = pd.to_datetime(df_final['Ano'].astype(str) + '-' + df_final['Mês'].astype(str))
        return df_final
    return pd.DataFrame()

@st.cache_data
def convert_df_to_csv(df):
    df_to_save = df.copy()
    if 'Data' in df_to_save.columns:
        df_to_save['Data'] = df_to_save['Data'].dt.strftime('%Y-%m-%d')
    return df_to_save.to_csv(index=False, sep=';', decimal=',', encoding='utf-8-sig')
def baixar_e_descompactar_dados(url_dados):
    caminho_zip = "dados.zip"
    if not os.path.exists(DIRETORIO_DADOS):
        st.info(f"Preparando o ambiente pela primeira vez. Isso pode levar alguns minutos...")
        with st.spinner(f"Baixando arquivos de dados..."):
            # Baixar o arquivo
            with requests.get(url_dados, stream=True) as r:
                r.raise_for_status()
                with open(caminho_zip, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        
        with st.spinner("Descompactando arquivos..."):
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                zip_ref.extractall(".") 
            os.remove(caminho_zip)
        st.success("Ambiente pronto! Carregando dashboard...")

# 2. CARREGAMENTO INICIAL E MAPAS ---
URL_DADOS_ZIP = "https://github.com/DaviSalva/dashcomexstat/releases/download/01.07-25/dados.zip" 
baixar_e_descompactar_dados(URL_DADOS_ZIP)
df_paises, df_ufs, df_ncm = carregar_tabelas_auxiliares(DIRETORIO_DADOS)
set_ncm_tarifados = carregar_ncm_tarifados(CAMINHO_NCM_TARIFADOS)

mapa_pais_para_cod = df_paises.set_index('NO_PAIS')['CO_PAIS'].to_dict()
mapa_uf_para_cod = df_ufs.set_index('SG_UF')['CO_UF'].to_dict()
mapa_cod_para_pais = df_paises.set_index('CO_PAIS')['NO_PAIS'].to_dict()
mapa_cod_para_ncm = df_ncm.set_index('CO_NCM')['NO_NCM_POR'].to_dict()

# 3. SIDEBAR DE FILTROS ---
with st.sidebar:
    st.image("https://instructure-uploads.s3.amazonaws.com/account_155730000000000001/attachments/503/logo3a.png", width=200)
    st.title("Filtros da Análise")
    tipo_fluxo_label = st.radio("1. Tipo de Análise:", ('Exportação', 'Importação', 'Saldo Comercial'))
    
    monitor_tarifados = False
    if tipo_fluxo_label == 'Exportação':
        st.divider()
        monitor_tarifados = st.toggle("Ativar Monitor de Tarifados", value=False)
    
    st.divider()
    st.markdown("##### Período da Análise")
    col1, col2 = st.columns(2)
    ano_inicio = col1.number_input("Ano Início", 1997, ANO_ATUAL, ANO_ATUAL - 1)
    mes_inicio = col2.selectbox("Mês Início", range(1, 13), format_func=lambda x: f"{x:02d}", index=0)
    ano_fim = col1.number_input("Ano Fim", 1997, ANO_ATUAL, ANO_ATUAL)
    mes_fim = col2.selectbox("Mês Fim", range(1, 13), format_func=lambda x: f"{x:02d}", index=datetime.now().month - 1)
    
    st.divider()
    lista_paises = ['Mundo'] + sorted(df_paises['NO_PAIS'].tolist())
    try:
        index_eua = lista_paises.index('Estados Unidos')
    except ValueError:
        index_eua = 0
    pais_selecionado = st.selectbox("País Parceiro:", lista_paises, index=index_eua)
    lista_ufs = ['Todos'] + sorted(df_ufs['SG_UF'].tolist())
    uf_selecionada = st.selectbox("UF de Origem/Destino (Brasil):", lista_ufs)
    
    st.divider()
    st.markdown("##### Filtro de Produtos (Opcional)")
    lista_ncm_texto = st.text_area("Cole uma lista de NCMs:", height=150, help="Separe os códigos por vírgula ou quebra de linha.")

    if st.button("Analisar Período", use_container_width=True, type="primary"):
        st.session_state.clear()
        if datetime(ano_inicio, mes_inicio, 1) > datetime(ano_fim, mes_fim, 1):
            st.error("A data de início não pode ser posterior à data de fim.")
            st.stop()
        
        with st.spinner("Buscando e processando dados..."):
            st.session_state['resultado'] = {
                'tipo': tipo_fluxo_label, 'uf_selecionada': uf_selecionada,
                'lista_ncm_texto': lista_ncm_texto, 'pais_selecionado': pais_selecionado,
                'monitor_tarifados': monitor_tarifados
            }
            
            periodo_selecionado_inicio = datetime(ano_inicio, mes_inicio, 1)
            periodo_selecionado_fim = datetime(ano_fim, mes_fim, 1)
            usa_historico = periodo_selecionado_inicio <= datetime(ANO_ATUAL - 1, 12, 31)
            usa_dados_ano_corrente = periodo_selecionado_fim >= datetime(ANO_ATUAL, 1, 1)

            cod_pais = mapa_pais_para_cod.get(pais_selecionado)
            sg_uf = uf_selecionada if uf_selecionada != 'Todos' else None
            
            fluxos_para_buscar = []
            if tipo_fluxo_label in ['Exportação', 'Saldo Comercial']: fluxos_para_buscar.append('export')
            if tipo_fluxo_label in ['Importação', 'Saldo Comercial']: fluxos_para_buscar.append('import')

            for fluxo in fluxos_para_buscar:
                df_historico_final, df_ano_corrente_final = pd.DataFrame(), pd.DataFrame()

                # Processamento de dados históricos (até ANO_ATUAL - 1)
                if usa_historico:
                    df_hist_bruto = carregar_dados_locais(fluxo, "historico")
                    query = f"CO_ANO >= {ano_inicio} and CO_ANO <= {ano_fim}"
                    df_filtrado = df_hist_bruto.query(query)
                    if pais_selecionado != 'Mundo': df_filtrado = df_filtrado[df_filtrado['CO_PAIS'] == cod_pais]
                    if uf_selecionada != 'Todos': df_filtrado = df_filtrado[df_filtrado['SG_UF_NCM'] == sg_uf]
                    
                    df_historico_final = df_filtrado.rename(columns={'CO_ANO': 'year', 'CO_MES': 'monthNumber', 'CO_NCM': 'coNcm', 'CO_PAIS': 'coPais', 'SG_UF_NCM': 'state', 'VL_FOB': 'metricFOB', 'KG_LIQUIDO': 'metricKG'})
                    df_historico_final['country'] = df_historico_final['coPais'].map(mapa_cod_para_pais)
                    df_historico_final['ncm'] = df_historico_final['coNcm'].map(mapa_cod_para_ncm)

                # Processamento de dados do ano corrente (ANO_ATUAL)
                if usa_dados_ano_corrente:
                    df_atual_bruto = carregar_dados_locais(fluxo, f"historico_{ANO_ATUAL}")
                    df_filtrado_atual = df_atual_bruto
                    if pais_selecionado != 'Mundo': df_filtrado_atual = df_filtrado_atual[df_filtrado_atual['CO_PAIS'] == cod_pais]
                    if uf_selecionada != 'Todos': df_filtrado_atual = df_filtrado_atual[df_filtrado_atual['SG_UF_NCM'] == sg_uf]
                    
                    df_ano_corrente_final = df_filtrado_atual.rename(columns={'CO_ANO': 'year', 'CO_MES': 'monthNumber', 'CO_NCM': 'coNcm', 'CO_PAIS': 'coPais', 'SG_UF_NCM': 'state', 'VL_FOB': 'metricFOB', 'KG_LIQUIDO': 'metricKG'})
                    df_ano_corrente_final['country'] = df_ano_corrente_final['coPais'].map(mapa_cod_para_pais)
                    df_ano_corrente_final['ncm'] = df_ano_corrente_final['coNcm'].map(mapa_cod_para_ncm)
                
                df_final = pd.concat([df_historico_final, df_ano_corrente_final], ignore_index=True)
                query_final = f"not ((year == {ano_inicio} and monthNumber < {mes_inicio}) or (year == {ano_fim} and monthNumber > {mes_fim}))"
                df_final = df_final.query(query_final)

                st.session_state['resultado'][f'df_{fluxo}'] = df_final
            
            # Carregamento dos totais mundiais para cálculo de coeficiente
            if tipo_fluxo_label in ['Exportação', 'Importação']:
                fluxo_world = 'export' if tipo_fluxo_label == 'Exportação' else 'import'
                df_mundo_hist_final = carregar_dados_locais(fluxo_world, "world_totals")
                query_world = f"CO_ANO >= {ano_inicio} and CO_ANO <= {ano_fim}"
                df_mundo_hist_final = df_mundo_hist_final.query(query_world).rename(columns={'CO_ANO': 'year', 'CO_MES': 'monthNumber', 'CO_NCM': 'coNcm', 'VL_FOB_MUNDO': 'metricFOB'})
                
                df_mundo_atual_final = pd.DataFrame()
                if usa_dados_ano_corrente:
                     df_mundo_atual_final = carregar_dados_locais(fluxo_world, f"world_totals_{ANO_ATUAL}")
                
                df_mundo_combinado = pd.concat([df_mundo_hist_final, df_mundo_atual_final], ignore_index=True)
                df_mundo_combinado = df_mundo_combinado.query(query_final)
                st.session_state['resultado']['df_mundo'] = df_mundo_combinado


# 4. PÁGINA PRINCIPAL ---
st.title("Dashboard de Análise COMEX | FACAMP")

if 'resultado' not in st.session_state:
    st.info("Bem-vindo! Utilize os filtros na barra lateral e clique em 'Analisar Período' para iniciar.")
else:
    resultado = st.session_state['resultado']
    tipo_resultado = resultado.get('tipo', 'Exportação')
    pais_selecionado = resultado.get('pais_selecionado')
    lista_ncm_texto = resultado.get('lista_ncm_texto', '')
    lista_ncm_filtro = [ncm.strip() for ncm in re.split(r'[,\n\s]+', lista_ncm_texto) if ncm.strip()]

    tab_analise, tab_tabelas = st.tabs(["📊 Análise Gráfica", "📋 Tabelas Consolidadas"])

    with tab_analise:
        # --- Análise de Exportação / Importação ---
        if tipo_resultado in ['Exportação', 'Importação']:
            sub_tab1, sub_tab2 = st.tabs(["Visão Geral e Evolução", "Análise Detalhada por Produto"])
            
            df_pais_raw = resultado.get('df_export' if tipo_resultado == 'Exportação' else 'df_import', pd.DataFrame())
            df_mundo_raw = resultado.get('df_mundo', pd.DataFrame())
            
            # Aplicação de filtro do Monitor de Tarifados, se ativo
            if tipo_resultado == 'Exportação' and resultado.get('monitor_tarifados', False):
                if set_ncm_tarifados:
                    registros_antes = len(df_pais_raw)
                    df_pais_raw = df_pais_raw[df_pais_raw['coNcm'].isin(set_ncm_tarifados)]
                    if not df_mundo_raw.empty:
                        df_mundo_raw = df_mundo_raw[df_mundo_raw['coNcm'].isin(set_ncm_tarifados)]
                    st.success(f"**Monitor de Tarifados ATIVO.** Exibindo dados para **{len(df_pais_raw)}** de **{registros_antes}** registros.")
                else:
                    st.warning("Monitor de Tarifados está ativo, mas a lista de NCMs não pôde ser carregada.")
            
            # Aplicação do filtro de NCMs do text_area
            if lista_ncm_filtro:
                df_pais_raw = df_pais_raw[df_pais_raw['coNcm'].isin(lista_ncm_filtro)]
            
            if df_pais_raw.empty:
                st.warning("Nenhum dado encontrado para os filtros selecionados.")
            else:
                # Cálculo shares e coeficientes
                df_pais_raw['metricFOB'] = pd.to_numeric(df_pais_raw['metricFOB'], errors='coerce')
                total_pais_por_produto = df_pais_raw.groupby('coNcm')['metricFOB'].sum().reset_index().rename(columns={'metricFOB': 'Valor FOB País'})
                df_final_shares = pd.merge(total_pais_por_produto, df_ncm[['CO_NCM', 'NO_NCM_POR']], left_on='coNcm', right_on='CO_NCM', how='left').rename(columns={'NO_NCM_POR': 'Produto'})
                df_final_shares['Produto'] = df_final_shares['Produto'].fillna(df_final_shares['coNcm'])
                valor_total_pais_filtrado = df_final_shares['Valor FOB País'].sum()
                df_final_shares['Share na Pauta (%)'] = (df_final_shares['Valor FOB País'] / valor_total_pais_filtrado) * 100 if valor_total_pais_filtrado > 0 else 0
                
                if not df_mundo_raw.empty:
                    df_mundo_raw['metricFOB'] = pd.to_numeric(df_mundo_raw['metricFOB'], errors='coerce')
                    total_mundo_por_produto = df_mundo_raw.groupby('coNcm')['metricFOB'].sum().reset_index().rename(columns={'metricFOB': 'Valor FOB Mundo'})
                    df_final_shares = pd.merge(df_final_shares, total_mundo_por_produto, on='coNcm', how='left')
                    df_final_shares['Valor FOB Mundo'] = df_final_shares['Valor FOB Mundo'].fillna(0)
                    df_final_shares['Coeficiente de Concentração de Produtos (%)'] = df_final_shares.apply(
                        lambda row: (row['Valor FOB País'] / row['Valor FOB Mundo']) * 100 if row['Valor FOB Mundo'] > 0 else 0,
                        axis=1)

                with sub_tab1:
                    st.header(f"Visão Geral: {tipo_resultado} para {pais_selecionado}")
                    st.metric(f"Valor Total - {tipo_resultado}", f"US$ {df_pais_raw['metricFOB'].sum():,.2f}")
                    st.subheader("Evolução Mensal (Valor FOB US$)")
                    df_pais_raw['Data'] = pd.to_datetime(df_pais_raw['year'].astype(str) + '-' + df_pais_raw['monthNumber'].astype(str))
                    evolucao_mensal = df_pais_raw.groupby('Data')['metricFOB'].sum().reset_index()
                    fig_linha = px.line(evolucao_mensal, x='Data', y='metricFOB', labels={'metricFOB': 'Valor FOB (US$)'}, markers=True)
                    st.plotly_chart(fig_linha, use_container_width=True)
                
                with sub_tab2:
                    st.header("Análise Detalhada por Produto")
                    if not df_final_shares.empty:
                        df_top_10 = df_final_shares.nlargest(10, 'Valor FOB País')
                        with st.expander("O que estes indicadores significam?", expanded=False):
                            st.markdown("""
                    **Share na Pauta:** Mede a importância de cada produto no **total comercializado com o país parceiro selecionado**. 
                    - *Exemplo:* Um share de 25% significa que este produto representa um quarto de tudo que o Brasil vendeu (ou comprou) para este país.

                    **Coeficiente de Concentração de Produtos:** Mede o quanto das vendas (ou compras) mundiais de cada produto brasileiro são **destinadas ao país parceiro selecionado**.
                    - *Exemplo:* Um coeficiente de concentração de 90% significa que 90% de todo o volume daquele produto que o Brasil exporta para o mundo, vai para este país.
                    """)
                        
                        st.subheader(f"Share na Pauta Brasil-{pais_selecionado} (Top 10 Produtos)")
                        fig_pauta = px.bar(df_top_10.sort_values('Share na Pauta (%)'), x='Share na Pauta (%)', y='Produto', orientation='h', text_auto='.2f')
                        st.plotly_chart(fig_pauta, use_container_width=True)
                        
                        st.divider()
                        if 'Coeficiente de Concentração de Produtos (%)' in df_final_shares.columns:
                            st.subheader(f"Coeficiente de Concentração Brasil-{pais_selecionado} (Top 10)")
                            fig_mercado = px.bar(df_top_10.sort_values('Coeficiente de Concentração de Produtos (%)'), x='Coeficiente de Concentração de Produtos (%)', y='Produto', orientation='h', text_auto='.2f')
                            st.plotly_chart(fig_mercado, use_container_width=True)
                    else:
                        st.info("Nenhum dado para exibir nesta aba.")

        # Análise de Saldo Comercial ---
        elif tipo_resultado == 'Saldo Comercial':
            sub_tab1, sub_tab2 = st.tabs(["Visão Geral e Evolução", "Análise de Saldo Detalhada"])
            
            df_final_exp = processar_df_saldo(resultado.get('df_export'), lista_ncm_filtro)
            df_final_imp = processar_df_saldo(resultado.get('df_import'), lista_ncm_filtro)

            if df_final_exp.empty and df_final_imp.empty:
                st.warning("Nenhum dado encontrado para os filtros selecionados.")
            else:
                with sub_tab1:
                    st.header(f"Visão Geral: {tipo_resultado} com {pais_selecionado}")
                    valor_export = df_final_exp['Valor FOB (US$)'].sum()
                    valor_import = df_final_imp['Valor FOB (US$)'].sum()
                    saldo_total = valor_export - valor_import
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Exportação Total (US$)", f"{valor_export:,.2f}")
                    col2.metric("Importação Total (US$)", f"{valor_import:,.2f}")
                    col3.metric("Saldo Comercial (US$)", f"{saldo_total:,.2f}", 
                                delta="Superávit" if saldo_total >= 0 else "Déficit", 
                                delta_color="normal" if saldo_total >= 0 else "inverse")
                    
                    df_evolucao_exp = df_final_exp.groupby('Data')['Valor FOB (US$)'].sum().rename('Exportação')
                    df_evolucao_imp = df_final_imp.groupby('Data')['Valor FOB (US$)'].sum().rename('Importação')
                    df_evolucao = pd.concat([df_evolucao_exp, df_evolucao_imp], axis=1).fillna(0)
                    df_evolucao['Saldo'] = df_evolucao['Exportação'] - df_evolucao['Importação']
                    df_evolucao.reset_index(inplace=True)

                    st.subheader("Evolução Temporal - Exportação vs Importação")
                    fig_linhas = go.Figure()
                    fig_linhas.add_trace(go.Scatter(x=df_evolucao['Data'], y=df_evolucao['Exportação'], mode='lines+markers', name='Exportação', line=dict(color='green', width=2)))
                    fig_linhas.add_trace(go.Scatter(x=df_evolucao['Data'], y=df_evolucao['Importação'], mode='lines+markers', name='Importação', line=dict(color='red', width=2)))
                    fig_linhas.add_trace(go.Scatter(x=df_evolucao['Data'], y=df_evolucao['Saldo'], mode='lines+markers', name='Saldo', line=dict(color='blue', width=3, dash='dot')))
                    fig_linhas.update_layout(xaxis_title="Período", yaxis_title="Valor FOB (US$)", hovermode='x unified')
                    st.plotly_chart(fig_linhas, use_container_width=True)
                    
                with sub_tab2:
                    st.header("Análise de Saldo por Produto")
                    df_exp_produto = df_final_exp.groupby(['coNcm','Produto'])['Valor FOB (US$)'].sum().rename('Exportação')
                    df_imp_produto = df_final_imp.groupby(['coNcm','Produto'])['Valor FOB (US$)'].sum().rename('Importação')
                    df_saldo_produto = pd.concat([df_exp_produto, df_imp_produto], axis=1).fillna(0)
                    df_saldo_produto['Saldo'] = df_saldo_produto['Exportação'] - df_saldo_produto['Importação']
                    df_saldo_produto.reset_index(inplace=True)
                    
                    df_top_15 = df_saldo_produto.reindex(df_saldo_produto['Saldo'].abs().nlargest(15).index).sort_values('Saldo')
                    st.subheader("Saldo Comercial por Produto (Top 15 com maior impacto)")
                    fig_produto = go.Figure(go.Bar(
                        x=df_top_15['Saldo'], y=df_top_15['Produto'], orientation='h',
                        marker_color=['green' if s >= 0 else 'red' for s in df_top_15['Saldo']],
                        text=[f"US$ {val:,.0f}" for val in df_top_15['Saldo']], textposition='outside'
                    ))
                    fig_produto.update_layout(title="Saldo por Produto (Verde = Superávit, Vermelho = Déficit)", xaxis_title="Valor FOB (US$)", yaxis_title="Produtos", height=600, showlegend=False)
                    fig_produto.add_vline(x=0, line_dash="dash", line_color="black")
                    st.plotly_chart(fig_produto, use_container_width=True)

    with tab_tabelas:
        st.header(f"Tabela de Dados para: {tipo_resultado}")
        if tipo_resultado in ['Exportação', 'Importação']:
            df_tabela = resultado.get('df_export' if tipo_resultado == 'Exportação' else 'df_import', pd.DataFrame())
            if not df_tabela.empty:
                df_display = df_tabela.copy()
                df_display['Data'] = pd.to_datetime(df_display['year'].astype(str) + '-' + df_display['monthNumber'].astype(str))
                colunas_map = {
                    'coNcm': 'CO_NCM', 'year': 'Ano', 'monthNumber': 'Mês', 'ncm': 'Produto',
                    'country': 'País', 'state': 'UF', 'metricFOB': 'Valor FOB (US$)', 'metricKG': 'Peso (KG)'
                }
                df_display = df_display.rename(columns=colunas_map)
                ordem_exibicao = ['Data', 'CO_NCM', 'Produto', 'País', 'UF', 'Valor FOB (US$)', 'Peso (KG)']
                df_display = df_display.reindex(columns=ordem_exibicao, fill_value='N/A')
                st.dataframe(df_display.sort_values('Valor FOB (US$)', ascending=False))
                st.download_button("📥 Baixar dados como CSV", convert_df_to_csv(df_display), f"dados_{tipo_resultado.lower()}.csv", "text/csv")
            else:
                st.info("Nenhum dado para exibir.")
        
        elif tipo_resultado == 'Saldo Comercial':
            df_exp_produto = processar_df_saldo(resultado.get('df_export'), lista_ncm_filtro)
            df_imp_produto = processar_df_saldo(resultado.get('df_import'), lista_ncm_filtro)
            if not df_exp_produto.empty or not df_imp_produto.empty:
                df_exp_agrupado = df_exp_produto.groupby(['coNcm','Produto'])['Valor FOB (US$)'].sum().rename('Exportação (US$)')
                df_imp_agrupado = df_imp_produto.groupby(['coNcm','Produto'])['Valor FOB (US$)'].sum().rename('Importação (US$)')
                df_saldo_produto = pd.concat([df_exp_agrupado, df_imp_agrupado], axis=1).fillna(0)
                df_saldo_produto['Saldo (US$)'] = df_saldo_produto['Exportação (US$)'] - df_saldo_produto['Importação (US$)']
                df_saldo_produto.reset_index(inplace=True)
                
                st.dataframe(df_saldo_produto.sort_values('Saldo (US$)', ascending=False), use_container_width=True)
                st.download_button("📥 Baixar tabela consolidada", convert_df_to_csv(df_saldo_produto), "saldo_comercial_consolidado.csv", 'text/csv')
            else:
                st.info("Nenhum dado para exibir.")
    
    st.markdown("---")
    with st.expander("❓ Sobre este Dashboard"):
        st.markdown("""
        Este dashboard permite analisar o comércio exterior brasileiro, com foco em:
        - **Fluxos:** Exportações, importações e saldo comercial por país, UF e produto (NCM).
        - **Produtos Tarifados:** Monitoramento específico de produtos sob novas tarifas (apenas para exportação).
        - **Indicadores:** Share de produtos na pauta com parceiros e coeficiente de concentração no mercado mundial.
        - **Saldo Comercial Detalhado:** Gráficos com cores indicativas (🟢 Superávit, 🔴 Déficit) para fácil interpretação.
        
        **Instruções:**
        Utilize os filtros na barra lateral para ajustar o período, país, UF ou produtos específicos. Para analisar produtos específicos, cole os códigos NCM na área de texto.
        """)