import pandas as pd
import os
import glob

def filtrar_municipio_por_ano(pasta_entrada, arquivo_saida, codigo_municipio=4118501):
    """
    Filtra dados de múltiplos arquivos CSV por código municipal e organiza por ano
    """

    # Lista todos os arquivos CSV na pasta
    padrao_arquivos = os.path.join(pasta_entrada, "*.csv")
    arquivos_csv = glob.glob(padrao_arquivos)

    if not arquivos_csv:
        print(f"Nenhum arquivo CSV encontrado em {pasta_entrada}")
        return

    print(f"Encontrados {len(arquivos_csv)} arquivos CSV")

    # Lista para armazenar todos os dados filtrados
    dados_filtrados = []

    for arquivo in arquivos_csv:
        try:
            print(f"\n--- Processando: {os.path.basename(arquivo)} ---")

            # Lê o arquivo CSV com separador correto
            # Primeiro vamos ler a primeira linha para entender a estrutura
            with open(arquivo, 'r', encoding='utf-8') as f:
                primeira_linha = f.readline().strip()

            print(f"Estrutura da primeira linha: {primeira_linha}")

            # Se a primeira linha contém todas as colunas juntas, precisamos reparar o CSV
            if ';' in primeira_linha and primeira_linha.count(';') > 3:
                # Lê o arquivo ignorando a primeira linha (cabeçalho) e usando ; como separador
                df = pd.read_csv(arquivo, sep=';', skiprows=1, header=None, encoding='utf-8')

                # Define os nomes das colunas manualmente baseado na estrutura
                colunas = ['CO_ANO', 'CO_MES', 'SH4', 'CO_PAIS', 'SG_UF_MUN', 'CO_MUN', 'KG_LIQUIDO', 'VL_FOB']
                df.columns = colunas

                print(f"✅ Arquivo reparado! Colunas: {list(df.columns)}")

            else:
                # Tenta ler normalmente
                df = pd.read_csv(arquivo, encoding='utf-8')
                print(f"Colunas disponíveis: {list(df.columns)}")

            # Remove aspas dos valores se existirem
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace('"', '')

            # Converte colunas numéricas
            colunas_numericas = ['CO_ANO', 'CO_MES', 'SH4', 'CO_PAIS', 'CO_MUN', 'KG_LIQUIDO', 'VL_FOB']
            for col in colunas_numericas:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            print(f"Amostra dos dados processados:")
            print(df.head(2).to_string())

            # Filtra pelo código do município
            df_filtrado = df[df['CO_MUN'] == codigo_municipio].copy()

            if not df_filtrado.empty:
                dados_filtrados.append(df_filtrado)
                print(f"✅ {len(df_filtrado)} registros encontrados para município {codigo_municipio}")
            else:
                print(f"❌ Nenhum registro para município {codigo_municipio}")

        except Exception as e:
            print(f"❌ Erro ao processar {os.path.basename(arquivo)}: {e}")
            # Tentativa alternativa de leitura
            try:
                print("Tentando leitura alternativa...")
                df = pd.read_csv(arquivo, encoding='latin-1')
                print(f"Colunas (alternativa): {list(df.columns)}")
            except Exception as e2:
                print(f"❌ Falha na leitura alternativa: {e2}")

    if not dados_filtrados:
        print(f"\n🚫 Nenhum dado encontrado para o município {codigo_municipio}")
        return

    # Combina todos os dados filtrados
    df_final = pd.concat(dados_filtrados, ignore_index=True)

    # Ordena por ano e mês
    if 'CO_ANO' in df_final.columns and 'CO_MES' in df_final.columns:
        df_final = df_final.sort_values(['CO_ANO', 'CO_MES'])
    elif 'CO_ANO' in df_final.columns:
        df_final = df_final.sort_values('CO_ANO')

    # Seleciona e ordena as colunas na sequência desejada
    colunas_desejadas = ['CO_ANO', 'CO_MES', 'SH4', 'CO_PAIS', 'SG_UF_MUN', 'CO_MUN', 'KG_LIQUIDO', 'VL_FOB']
    colunas_existentes = [col for col in colunas_desejadas if col in df_final.columns]
    df_final = df_final[colunas_existentes]

    # Salva o arquivo de saída
    df_final.to_csv(arquivo_saida, index=False, sep='\t')

    # Estatísticas
    print(f"\n🎉 PROCESSAMENTO CONCLUÍDO!")
    print(f"📁 Arquivo salvo: {arquivo_saida}")
    print(f"📊 Total de registros: {len(df_final)}")

    if 'CO_ANO' in df_final.columns:
        anos_unicos = df_final['CO_ANO'].nunique()
        print(f"📅 Anos com dados: {anos_unicos}")

        print("\n📋 Distribuição por ano:")
        distribuicao_ano = df_final['CO_ANO'].value_counts().sort_index()
        for ano, quantidade in distribuicao_ano.items():
            print(f"   {ano}: {quantidade} registros")

    # Mostra o resultado final
    print(f"\n📄 RESULTADO FINAL:")
    print(df_final.to_string(index=False))

# Versão específica para o formato problemático dos seus arquivos
def processar_arquivos_mal_formatados(pasta_entrada, arquivo_saida, codigo_municipio=4118501):
    """
    Versão específica para arquivos onde todas as colunas estão em uma string
    """

    padrao_arquivos = os.path.join(pasta_entrada, "*.csv")
    arquivos_csv = glob.glob(padrao_arquivos)

    if not arquivos_csv:
        print(f"Nenhum arquivo CSV encontrado em {pasta_entrada}")
        return

    print(f"Encontrados {len(arquivos_csv)} arquivos CSV")

    dados_filtrados = []

    for arquivo in arquivos_csv:
        print(f"\n--- Processando: {os.path.basename(arquivo)} ---")

        try:
            # Lê todo o conteúdo do arquivo
            with open(arquivo, 'r', encoding='utf-8') as f:
                linhas = f.readlines()

            # Processa cada linha
            dados = []
            for i, linha in enumerate(linhas):
                linha = linha.strip()
                if not linha:
                    continue

                if i == 0:  # Cabeçalho
                    continue

                # Divide a linha por ponto e vírgula
                partes = linha.split(';')

                # Remove aspas de cada parte
                partes = [p.replace('"', '') for p in partes]

                # Verifica se temos 8 colunas (como esperado)
                if len(partes) == 8:
                    dados.append(partes)

            # Cria DataFrame
            colunas = ['CO_ANO', 'CO_MES', 'SH4', 'CO_PAIS', 'SG_UF_MUN', 'CO_MUN', 'KG_LIQUIDO', 'VL_FOB']
            df = pd.DataFrame(dados, columns=colunas)

            # Converte para tipos numéricos
            for col in ['CO_ANO', 'CO_MES', 'SH4', 'CO_PAIS', 'CO_MUN', 'KG_LIQUIDO', 'VL_FOB']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            print(f"✅ {len(df)} registros lidos")
            print(f"Amostra:")
            print(df.head(2).to_string())

            # Filtra pelo município
            df_filtrado = df[df['CO_MUN'] == codigo_municipio]

            if len(df_filtrado) > 0:
                dados_filtrados.append(df_filtrado)
                print(f"🎯 {len(df_filtrado)} registros para município {codigo_municipio}")
            else:
                print(f"❌ Nenhum registro para município {codigo_municipio}")

        except Exception as e:
            print(f"❌ Erro: {e}")

    if not dados_filtrados:
        print(f"\n🚫 Nenhum dado encontrado para o município {codigo_municipio}")
        return

    # Combina resultados
    df_final = pd.concat(dados_filtrados, ignore_index=True)
    df_final = df_final.sort_values(['CO_ANO', 'CO_MES'])

    # Salva
    df_final.to_csv(arquivo_saida, index=False, sep='\t')

    print(f"\n🎉 CONCLUÍDO! Arquivo salvo: {arquivo_saida}")
    print(f"📊 Total de registros: {len(df_final)}")
    print(f"\n📄 RESULTADO:")
    print(df_final.to_string(index=False))

# Exemplo de uso
if __name__ == "__main__":
    PASTA_ENTRADA = "Bruto"
    ARQUIVO_SAIDA = "dados_import_filtrados_municipio_4118501.csv"
    CODIGO_MUNICIPIO = 4118501

    # Usa a versão específica para arquivos mal formatados
    processar_arquivos_mal_formatados(PASTA_ENTRADA, ARQUIVO_SAIDA, CODIGO_MUNICIPIO)
