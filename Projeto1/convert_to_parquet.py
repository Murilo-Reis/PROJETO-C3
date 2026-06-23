import pandas as pd

def convert_to_parquet():
    print("Iniciando conversão de CSV para Parquet...")
    print("Lendo o arquivo CSV (isso pode demorar mais de um minuto)...")
    # Lendo o arquivo como foi feito no notebook do usuário
    df = pd.read_csv('MICRODADOS.csv', sep=';', encoding='latin-1', low_memory=False)
    
    print(f"Arquivo lido com sucesso! Total de registros: {len(df)}")
    
    print("Salvando em formato Parquet...")
    # Convertendo tipos para otimizar tamanho se quisermos, mas como manteremos o padrão, vamos apenas salvar
    df.to_parquet('MICRODADOS.parquet', index=False)
    
    print("Conversão concluída com sucesso! 'MICRODADOS.parquet' foi salvo.")

if __name__ == "__main__":
    convert_to_parquet()
