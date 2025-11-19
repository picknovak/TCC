import pandas as pd
import os

def ordenar_hierarquicamente(arquivo_entrada, arquivo_saida):
    """
    Ordena os dados hierarquicamente por:
    1. Ano (CO_ANO)
    2. Frequência do SH4 (mais repetido primeiro)
    3. Frequência do CO_PAIS (mais repetido primeiro)
    4. Valor FOB (VL_FOB - maior primeiro)
    """

    print("📖 Lendo arquivo de entrada...")

    try:
        # Lê o arquivo TSV (separado por tab)
        df = pd.read_csv(arquivo_entrada, sep='\t', encoding='utf-8')
        print(f"✅ Arquivo lido com sucesso! {len(df)} registros encontrados")

    except Exception as e:
        print(f"❌ Erro ao ler arquivo: {e}")
        return

    # Verifica se as colunas necessárias existem
    colunas_necessarias = ['CO_ANO', 'SH4', 'CO_PAIS', 'VL_FOB']
    for coluna in colunas_necessarias:
        if coluna not in df.columns:
            print(f"❌ Coluna '{coluna}' não encontrada no arquivo")
            print(f"Colunas disponíveis: {list(df.columns)}")
            return

    print("\n📊 Calculando frequências...")

    # Calcula frequência de cada SH4 (total geral)
    freq_sh4 = df['SH4'].value_counts().reset_index()
    freq_sh4.columns = ['SH4', 'FREQ_SH4']

    # Calcula frequência de cada CO_PAIS (total geral)
    freq_pais = df['CO_PAIS'].value_counts().reset_index()
    freq_pais.columns = ['CO_PAIS', 'FREQ_PAIS']

    # Adiciona as frequências ao DataFrame principal
    df = df.merge(freq_sh4, on='SH4', how='left')
    df = df.merge(freq_pais, on='CO_PAIS', how='left')

    print("🎯 Ordenando dados hierarquicamente...")

    # Ordenação hierárquica:
    # 1. Primeiro por ano (crescente)
    # 2. Depois por frequência do SH4 (decrescente - mais frequente primeiro)
    # 3. Depois por frequência do CO_PAIS (decrescente - mais frequente primeiro)
    # 4. Finalmente por VL_FOB (decrescente - maior valor primeiro)
    df_ordenado = df.sort_values([
        'CO_ANO',           # Ano em ordem crescente
        'FREQ_SH4',         # SH4 mais frequente primeiro
        'FREQ_PAIS',        # País mais frequente primeiro
        'VL_FOB'            # Maior valor FOB primeiro
    ], ascending=[True, False, False, False])

    # Remove as colunas de frequência temporárias se desejar
    # df_ordenado = df_ordenado.drop(['FREQ_SH4', 'FREQ_PAIS'], axis=1)

    print("💾 Salvando arquivo ordenado...")

    # Salva o novo arquivo CSV
    df_ordenado.to_csv(arquivo_saida, index=False, encoding='utf-8')

    # Gera estatísticas
    print(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
    print(f"📁 Arquivo salvo: {arquivo_saida}")
    print(f"📊 Total de registros: {len(df_ordenado)}")

    # Estatísticas por ano
    anos = df_ordenado['CO_ANO'].unique()
    print(f"📅 Anos presentes: {sorted(anos)}")

    print("\n🏆 TOP 5 SH4 mais frequentes (geral):")
    top_sh4 = freq_sh4.head(5)
    for i, (_, row) in enumerate(top_sh4.iterrows(), 1):
        print(f"   {i}. SH4 {row['SH4']}: {row['FREQ_SH4']} ocorrências")

    print("\n🌍 TOP 5 países mais frequentes (geral):")
    top_paises = freq_pais.head(5)
    for i, (_, row) in enumerate(top_paises.iterrows(), 1):
        print(f"   {i}. País {row['CO_PAIS']}: {row['FREQ_PAIS']} ocorrências")

    # Mostra amostra do resultado
    print(f"\n📄 AMOSTRA DO RESULTADO (primeiras 10 linhas):")
    colunas_mostrar = ['CO_ANO', 'SH4', 'CO_PAIS', 'VL_FOB', 'FREQ_SH4', 'FREQ_PAIS']
    colunas_existentes = [col for col in colunas_mostrar if col in df_ordenado.columns]
    print(df_ordenado[colunas_existentes].head(10).to_string(index=False))

def ordenar_por_ano_e_frequencia(arquivo_entrada, arquivo_saida):
    """
    Versão alternativa: ordena considerando frequências dentro de cada ano
    """

    print("📖 Lendo arquivo de entrada...")

    try:
        df = pd.read_csv(arquivo_entrada, sep='\t', encoding='utf-8')
        print(f"✅ Arquivo lido com sucesso! {len(df)} registros encontrados")

    except Exception as e:
        print(f"❌ Erro ao ler arquivo: {e}")
        return

    # Calcula frequências por ano
    print("\n📊 Calculando frequências por ano...")

    dados_por_ano = []

    for ano in df['CO_ANO'].unique():
        df_ano = df[df['CO_ANO'] == ano].copy()

        # Frequência SH4 dentro do ano
        freq_sh4_ano = df_ano['SH4'].value_counts().reset_index()
        freq_sh4_ano.columns = ['SH4', 'FREQ_SH4_ANO']

        # Frequência CO_PAIS dentro do ano
        freq_pais_ano = df_ano['CO_PAIS'].value_counts().reset_index()
        freq_pais_ano.columns = ['CO_PAIS', 'FREQ_PAIS_ANO']

        # Adiciona frequências do ano
        df_ano = df_ano.merge(freq_sh4_ano, on='SH4', how='left')
        df_ano = df_ano.merge(freq_pais_ano, on='CO_PAIS', how='left')

        # Ordena dentro do ano
        df_ano_ordenado = df_ano.sort_values([
            'FREQ_SH4_ANO',    # SH4 mais frequente no ano primeiro
            'FREQ_PAIS_ANO',   # País mais frequente no ano primeiro
            'VL_FOB'           # Maior valor FOB primeiro
        ], ascending=[False, False, False])

        dados_por_ano.append(df_ano_ordenado)

    # Combina todos os anos
    df_final = pd.concat(dados_por_ano, ignore_index=True)

    # Ordena por ano (crescente)
    df_final = df_final.sort_values('CO_ANO')

    print("💾 Salvando arquivo ordenado...")
    df_final.to_csv(arquivo_saida, index=False, encoding='utf-8')

    print(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
    print(f"📁 Arquivo salvo: {arquivo_saida}")
    print(f"📊 Total de registros: {len(df_final)}")

    print(f"\n📄 AMOSTRA DO RESULTADO (primeiras 10 linhas):")
    colunas_mostrar = ['CO_ANO', 'SH4', 'CO_PAIS', 'VL_FOB', 'FREQ_SH4_ANO', 'FREQ_PAIS_ANO']
    colunas_existentes = [col for col in colunas_mostrar if col in df_final.columns]
    print(df_final[colunas_existentes].head(10).to_string(index=False))

# Exemplo de uso
if __name__ == "__main__":
    ARQUIVO_ENTRADA = "dados_import_filtrados_4118501.csv"
    ARQUIVO_SAIDA = "dados_ordenados_hierarquico.csv"
    ARQUIVO_SAIDA_ALTERNATIVO = "dados_ordenados_por_ano.csv"

    print("=" * 60)
    print("ORDENAÇÃO HIERÁRQUICA - FREQUÊNCIA GERAL")
    print("=" * 60)
    ordenar_hierarquicamente(ARQUIVO_ENTRADA, ARQUIVO_SAIDA)

    print("\n" + "=" * 60)
    print("ORDENAÇÃO POR ANO - FREQUÊNCIA POR ANO")
    print("=" * 60)
    ordenar_por_ano_e_frequencia(ARQUIVO_ENTRADA, ARQUIVO_SAIDA_ALTERNATIVO)
