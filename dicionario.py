import pandas as pd
import os

def traduzir_dados_com_csv(arquivo_entrada, arquivo_saida, arquivo_sh4, arquivo_pais):
    """
    Lê os dados ordenados e traduz SH4 e CO_PAIS usando dicionários CSV
    """

    print("🚀 INICIANDO TRADUÇÃO DE DADOS COM DICIONÁRIOS CSV")
    print("=" * 60)

    # Carrega os dicionários CSV
    print("📚 Carregando dicionários CSV...")

    try:
        # Carrega dicionário SH4
        print(f"📖 Lendo dicionário SH4: {arquivo_sh4}")
        df_sh4 = pd.read_csv(arquivo_sh4, encoding='utf-8')
        print(f"✅ Dicionário SH4 carregado: {len(df_sh4)} registros")
        print(f"   Colunas disponíveis: {list(df_sh4.columns)}")

        # Identifica colunas do dicionário SH4
        coluna_sh4 = [col for col in df_sh4.columns if 'SH4' in col.upper()][0]
        coluna_nome_sh4 = [col for col in df_sh4.columns if any(palavra in col.upper() for palavra in ['NOME', 'DESCRIÇÃO', 'DESCRICAO', 'NO_SH4_POR'])][0]

        dicionario_sh4 = df_sh4[[coluna_sh4, coluna_nome_sh4]].copy()
        dicionario_sh4.columns = ['SH4', 'NO_SH4_POR']
        dicionario_sh4 = dicionario_sh4.drop_duplicates()
        print(f"   🔧 Mapeamento: {coluna_sh4} → SH4, {coluna_nome_sh4} → NO_SH4_POR")

        # Carrega dicionário de países
        print(f"📖 Lendo dicionário países: {arquivo_pais}")
        df_pais = pd.read_csv(arquivo_pais, encoding='utf-8')
        print(f"✅ Dicionário países carregado: {len(df_pais)} registros")
        print(f"   Colunas disponíveis: {list(df_pais.columns)}")

        # Identifica colunas do dicionário de países
        coluna_pais = [col for col in df_pais.columns if any(palavra in col.upper() for palavra in ['CO_PAIS', 'COD_PAIS', 'PAIS'])][0]
        coluna_nome_pais = [col for col in df_pais.columns if any(palavra in col.upper() for palavra in ['NOME', 'NO_PAIS', 'NOME_PAIS'])][0]

        dicionario_pais = df_pais[[coluna_pais, coluna_nome_pais]].copy()
        dicionario_pais.columns = ['CO_PAIS', 'NO_PAIS']
        dicionario_pais = dicionario_pais.drop_duplicates()
        print(f"   🔧 Mapeamento: {coluna_pais} → CO_PAIS, {coluna_nome_pais} → NO_PAIS")

    except Exception as e:
        print(f"❌ Erro ao carregar dicionários: {e}")
        print("💡 Dica: Verifique se os arquivos CSV estão na mesma pasta do script")
        return

    print("\n📖 Lendo arquivo de dados ordenados...")

    try:
        # Tenta ler como CSV (com diferentes separadores)
        try:
            df = pd.read_csv(arquivo_entrada, encoding='utf-8')
        except:
            try:
                df = pd.read_csv(arquivo_entrada, sep='\t', encoding='utf-8')
            except:
                df = pd.read_csv(arquivo_entrada, sep=';', encoding='utf-8')

        print(f"✅ Dados carregados: {len(df)} registros")
        print(f"📊 Colunas disponíveis: {list(df.columns)}")

    except Exception as e:
        print(f"❌ Erro ao ler arquivo de dados: {e}")
        return

    # Verifica se as colunas necessárias existem
    colunas_necessarias = ['SH4', 'CO_PAIS']
    for coluna in colunas_necessarias:
        if coluna not in df.columns:
            print(f"❌ Coluna '{coluna}' não encontrada nos dados")
            return

    print("\n🔍 Realizando traduções...")

    # Faz uma cópia do DataFrame original
    df_traduzido = df.copy()

    # Converte colunas para o mesmo tipo dos dicionários
    print("🔄 Convertendo tipos de dados para compatibilidade...")

    df_traduzido['SH4'] = pd.to_numeric(df_traduzido['SH4'], errors='coerce')
    df_traduzido['CO_PAIS'] = pd.to_numeric(df_traduzido['CO_PAIS'], errors='coerce')

    dicionario_sh4['SH4'] = pd.to_numeric(dicionario_sh4['SH4'], errors='coerce')
    dicionario_pais['CO_PAIS'] = pd.to_numeric(dicionario_pais['CO_PAIS'], errors='coerce')

    # 1. Traduz SH4 para NO_SH4_POR
    print("📦 Traduzindo códigos SH4...")
    registros_antes = len(df_traduzido)
    df_traduzido = df_traduzido.merge(dicionario_sh4, on='SH4', how='left')

    # Verifica quantos SH4 não foram traduzidos
    sh4_nao_traduzidos = df_traduzido['NO_SH4_POR'].isna().sum()
    sh4_traduzidos = len(df_traduzido) - sh4_nao_traduzidos
    print(f"   ✅ SH4 traduzidos: {sh4_traduzidos}/{len(df_traduzido)} ({sh4_traduzidos/len(df_traduzido)*100:.1f}%)")

    if sh4_nao_traduzidos > 0:
        sh4_faltantes = df_traduzido[df_traduzido['NO_SH4_POR'].isna()]['SH4'].unique()
        print(f"   ⚠️  SH4 não encontrados no dicionário: {sh4_nao_traduzidos}")
        print(f"   🔍 Códigos faltantes (amostra): {sh4_faltantes[:10]}")

    # 2. Traduz CO_PAIS para NO_PAIS
    print("🌍 Traduzindo códigos de países...")
    df_traduzido = df_traduzido.merge(dicionario_pais, on='CO_PAIS', how='left')

    # Verifica quantos países não foram traduzidos
    pais_nao_traduzidos = df_traduzido['NO_PAIS'].isna().sum()
    pais_traduzidos = len(df_traduzido) - pais_nao_traduzidos
    print(f"   ✅ Países traduzidos: {pais_traduzidos}/{len(df_traduzido)} ({pais_traduzidos/len(df_traduzido)*100:.1f}%)")

    if pais_nao_traduzidos > 0:
        pais_faltantes = df_traduzido[df_traduzido['NO_PAIS'].isna()]['CO_PAIS'].unique()
        print(f"   ⚠️  Países não encontrados no dicionário: {pais_nao_traduzidos}")
        print(f"   🔍 Códigos faltantes (amostra): {pais_faltantes[:10]}")

    # Reorganiza as colunas - coloca as novas colunas ao lado das originais
    colunas_ordenadas = []

    for coluna in df.columns:
        colunas_ordenadas.append(coluna)
        if coluna == 'SH4':
            colunas_ordenadas.append('NO_SH4_POR')
        elif coluna == 'CO_PAIS':
            colunas_ordenadas.append('NO_PAIS')

    # Adiciona quaisquer colunas que possam ter ficado de fora
    for coluna in df_traduzido.columns:
        if coluna not in colunas_ordenadas:
            colunas_ordenadas.append(coluna)

    df_traduzido = df_traduzido[colunas_ordenadas]

    print("\n💾 Salvando arquivo traduzido...")

    # Salva o novo arquivo
    df_traduzido.to_csv(arquivo_saida, index=False, encoding='utf-8')

    print(f"\n🎉 TRADUÇÃO CONCLUÍDA!")
    print(f"📁 Arquivo salvo: {arquivo_saida}")
    print(f"📊 Total de registros: {len(df_traduzido)}")

    # Estatísticas finais
    print(f"\n📈 ESTATÍSTICAS DA TRADUÇÃO:")
    print(f"   📦 SH4 traduzidos: {sh4_traduzidos}/{len(df_traduzido)} ({sh4_traduzidos/len(df_traduzido)*100:.1f}%)")
    print(f"   🌍 Países traduzidos: {pais_traduzidos}/{len(df_traduzido)} ({pais_traduzidos/len(df_traduzido)*100:.1f}%)")

    # Mostra amostra do resultado
    print(f"\n📄 AMOSTRA DO RESULTADO (primeiras 8 linhas):")
    colunas_mostrar = ['CO_ANO', 'SH4', 'NO_SH4_POR', 'CO_PAIS', 'NO_PAIS', 'VL_FOB']
    colunas_existentes = [col for col in colunas_mostrar if col in df_traduzido.columns]

    if colunas_existentes:
        print(df_traduzido[colunas_existentes].head(8).to_string(index=False))
    else:
        print(df_traduzido.head(8).to_string(index=False))

# Exemplo de uso
if __name__ == "__main__":
    # Arquivos de entrada e saída
    ARQUIVO_ENTRADA = "dados_ordenados_hierarquico.csv"  # Seu arquivo ordenado
    ARQUIVO_SAIDA = "dados_ordenados_hierarquicos_traduzidos.csv"

    # Arquivos dos dicionários (CSV na mesma pasta)
    ARQUIVO_SH4 = "dicionario_sh4.csv"      # Arquivo CSV com códigos SH4
    ARQUIVO_PAIS = "dicionario_pais.csv"    # Arquivo CSV com códigos de países

    # Verifica se os arquivos existem
    print("🔍 Verificando arquivos...")
    for arquivo in [ARQUIVO_ENTRADA, ARQUIVO_SH4, ARQUIVO_PAIS]:
        if os.path.exists(arquivo):
            print(f"   ✅ {arquivo} - encontrado")
        else:
            print(f"   ❌ {arquivo} - não encontrado")

    print("\n")
    traduzir_dados_com_csv(ARQUIVO_ENTRADA, ARQUIVO_SAIDA, ARQUIVO_SH4, ARQUIVO_PAIS)
