import pandas as pd

# Converter o CSV do vendedor
df_vendedor = pd.read_csv("produtos_vendedor.csv")
df_vendedor.to_parquet("produtos_vendedor.parquet", engine="pyarrow", index=False)

# Converter o CSV dos concorrentes
df_concorrentes = pd.read_csv("produtos_concorrentes.csv")
df_concorrentes.to_parquet("produtos_concorrentes.parquet", engine="pyarrow", index=False)

print("Arquivos convertidos com sucesso!")
