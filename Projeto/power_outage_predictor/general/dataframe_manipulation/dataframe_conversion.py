def dataframe_to_tuple_list(df):
    lista_de_tuplas = []
    
    for coluna in df.columns:
        valores_da_coluna = df[coluna].tolist()
        lista_de_tuplas.append((coluna, valores_da_coluna))
    
    return lista_de_tuplas