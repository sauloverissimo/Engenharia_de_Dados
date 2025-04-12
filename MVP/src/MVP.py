# Databricks notebook source
# MAGIC %md
# MAGIC #### 🟩 Inicialização do "Compute"

# COMMAND ----------

# Instala bibliotecas necessárias do python para realizar a execução dos notebooks
# Nessa versão community que estamos utilizando, é neccessário instalar toda vez que cria um compute. 🥲 
# tentei automatizar isso, mas não consegui. 

%pip install openpyxl kaggle pandas numpy
# Em qualquer notebook Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Coleta: 
# MAGIC 🟩 Importação de dados (_RAW DATA, Bronze Stage_)

# COMMAND ----------

# Importa os metadados do projeto e gera um dataframe pra eventuais manipulações
import os
import pandas as pd

# Caminho original no DBFS
RAW_DIR = "dbfs:/FileStore/tables/"
METADADOS_FILE = RAW_DIR + "metadata.xlsx"
# Caminho temporário para o arquivo ser acessado pelo Pandas
TEMP_FILE = "/tmp/metadata.xlsx"
# Copiar arquivo do DBFS para o diretório temporário
dbutils.fs.cp(METADADOS_FILE, "file:" + TEMP_FILE, True)
# Carregar a planilha ignorando as duas primeiras linhas
METADADOS = pd.read_excel(TEMP_FILE, skiprows=2, engine="openpyxl")
# Exibir as primeiras linhas do DataFrame
METADADOS.head()

# COMMAND ----------

# Realiza o estágio BRONZE, copiando o arquivo e salvando em CSV

# Caminho do arquivo original no DBFS
STUDENTS_FILE_DBFS = "dbfs:/FileStore/tables/Students_Grading_Dataset.csv"
STUDENTS_FILE_LOCAL = "/tmp/Students_Grading_Dataset.csv"

# Copiar do DBFS para um caminho local
dbutils.fs.cp(STUDENTS_FILE_DBFS, "file:" + STUDENTS_FILE_LOCAL, True)

# Verificar se o arquivo foi copiado corretamente
if not os.path.exists(STUDENTS_FILE_LOCAL):
    raise FileNotFoundError(f"Arquivo {STUDENTS_FILE_LOCAL} não encontrado.")

# Carregar com Pandas
BRONZE = pd.read_csv(STUDENTS_FILE_LOCAL)

# Exibir as primeiras linhas
print("Estrutura do DataFrame BRONZE:")
print(BRONZE.head())

# Criar diretório no DBFS para salvar o CSV
BRONZE_DIR_DBFS = "dbfs:/FileStore/tables/1_BRONZE"
dbutils.fs.mkdirs(BRONZE_DIR_DBFS)

# Salvar o arquivo temporariamente em /tmp
BRONZE_TEMP_PATH = "/tmp/BRONZE.csv"
BRONZE.to_csv(BRONZE_TEMP_PATH, decimal='.', sep=",", index=False, encoding='utf-8')

# Copiar o CSV local (/tmp) para o DBFS
BRONZE_FILE_DBFS = "dbfs:/FileStore/tables/1_BRONZE/BRONZE.csv"
dbutils.fs.cp("file:" + BRONZE_TEMP_PATH, BRONZE_FILE_DBFS)

# Confirmação
print(f"Arquivo BRONZE salvo corretamente em: {BRONZE_FILE_DBFS}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Modelagem: 
# MAGIC ⭐ Esquema em Estrela (Star Schema)

# COMMAND ----------

# Importe PySpark sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

# Iniciei a sessão Spark para poder trabalhar com os dados no formato distribuído
spark = SparkSession.builder.appName("Modelagem Dimensional").getOrCreate()

# Carreguei o arquivo BRONZE que eu havia salvo no DBFS com separador padrão (vírgula)

BRONZE = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("dbfs:/FileStore/tables/1_BRONZE/BRONZE.csv")

# Corrigi o nome da coluna que dava problema ("Stress_Level (1-10)") para evitar erros futuros
BRONZE = BRONZE.withColumnRenamed("Stress_Level (1-10)", "Stress_Level")

#  Criei a dimensão ALUNOS com os dados pessoais e socioeconômicos dos estudantes
ALUNOS = BRONZE.select(
    col("Student_ID").cast("string").alias("ID_Aluno"),
    "First_Name", "Last_Name", "Email", "Gender", "Age",
    "Parent_Education_Level", "Family_Income_Level"
)

# Criei a dimensão DISCIPLINAS com os departamentos únicos e um ID incremental
DISCIPLINAS = BRONZE.select("Department").distinct() \
    .withColumnRenamed("Department", "Nome_Disciplina") \
    .withColumn("ID_Disciplina", monotonically_increasing_id())

# Criei a dimensão HÁBITOS com as variáveis comportamentais e tecnológicas
HABITOS = BRONZE.select(
    col("Student_ID").cast("string").alias("ID_Aluno"),
    "Study_Hours_per_Week",
    "Extracurricular_Activities",
    "Internet_Access_at_Home",
    "Stress_Level",
    "Sleep_Hours_per_Night"
)

# Criei a tabela fato DESEMPENHO com as notas, frequência e conceito final
FATO_DESEMPENHO = BRONZE.select(
    col("Student_ID").cast("string").alias("ID_Aluno"),
    col("Department").alias("ID_Disciplina"),
    col("Attendance (%)").alias("Presenca"),
    col("Midterm_Score").alias("Nota_Midterm"),
    col("Final_Score").alias("Nota_Final"),
    col("Projects_Score").alias("Nota_Projetos"),
    col("Total_Score").alias("Nota_Total"),
    col("Grade").alias("Conceito")
)

# Defini o caminho onde vou salvar os arquivos do estágio PRATA
PRATA_DIR_DBFS = "dbfs:/FileStore/tables/2_PRATA"
dbutils.fs.mkdirs(PRATA_DIR_DBFS)

# Salvei cada uma das tabelas em formato CSV no diretório PRATA
ALUNOS.write.mode("overwrite").option("header", "true").csv(f"{PRATA_DIR_DBFS}/ALUNOS.csv")
DISCIPLINAS.write.mode("overwrite").option("header", "true").csv(f"{PRATA_DIR_DBFS}/DISCIPLINAS.csv")
HABITOS.write.mode("overwrite").option("header", "true").csv(f"{PRATA_DIR_DBFS}/HABITOS.csv")
FATO_DESEMPENHO.write.mode("overwrite").option("header", "true").csv(f"{PRATA_DIR_DBFS}/FATO_DESEMPENHO.csv")

print("✅ Arquivos PRATA salvos com sucesso em:", PRATA_DIR_DBFS)


# COMMAND ----------

# Gera as views temporárias do csv para visualizar no Databricks.

# ALUNOS
spark.read.option("header", "true").csv(f"{PRATA_DIR_DBFS}/ALUNOS.csv").createOrReplaceTempView("csv_alunos")

# DISCIPLINAS
spark.read.option("header", "true").csv(f"{PRATA_DIR_DBFS}/DISCIPLINAS.csv").createOrReplaceTempView("csv_disciplinas")

# HÁBITOS
spark.read.option("header", "true").csv(f"{PRATA_DIR_DBFS}/HABITOS.csv").createOrReplaceTempView("csv_habitos")

# FATO_DESEMPENHO
spark.read.option("header", "true").csv(f"{PRATA_DIR_DBFS}/FATO_DESEMPENHO.csv").createOrReplaceTempView("csv_fato_desempenho")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Carga:
# MAGIC ⬆️ Vizualização e carregamento de dados (_Loading_)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### _🗃️_ Tabela Alunos

# COMMAND ----------

# Carreguei o CSV com os dados de alunos
alunos_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("dbfs:/FileStore/tables/2_PRATA/ALUNOS.csv")

# Garanti que o ID_Aluno seja do tipo string para evitar conflitos
alunos_df = alunos_df.withColumn("ID_Aluno", col("ID_Aluno").cast("string"))

# Salvei como tabela Delta no banco default
alunos_df.write.format("delta").mode("overwrite").saveAsTable("default.alunos")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.alunos
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE default.alunos;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 🗃️ Tabela Habitos

# COMMAND ----------

# Carreguei o CSV com os dados de hábitos
habitos_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("dbfs:/FileStore/tables/2_PRATA/HABITOS.csv")

# Renomeei a coluna com caracteres inválidos (caso ainda exista no CSV)
habitos_df = habitos_df.withColumnRenamed("Stress_Level (1-10)", "Stress_Level")

# Garanti que ID_Aluno seja string
habitos_df = habitos_df.withColumn("ID_Aluno", col("ID_Aluno").cast("string"))

# Salvei como tabela Delta no banco default
habitos_df.write.format("delta").mode("overwrite").saveAsTable("default.habitos")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.habitos
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE default.habitos;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 🗃️ Tabela Disciplinas

# COMMAND ----------

# Carreguei o CSV com os dados de disciplinas
disciplinas_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("dbfs:/FileStore/tables/2_PRATA/DISCIPLINAS.csv")

# (Opcional) Garanti que o ID seja string (caso você use joins depois com tipo string)
disciplinas_df = disciplinas_df.withColumn("ID_Disciplina", col("ID_Disciplina").cast("string"))

# Salvei como tabela Delta no banco default
disciplinas_df.write.format("delta").mode("overwrite").saveAsTable("default.disciplinas")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.disciplinas
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE default.disciplinas;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 🗃️ Fato Desempenho

# COMMAND ----------

# Carreguei o CSV com os dados de desempenho
fato_df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("dbfs:/FileStore/tables/2_PRATA/FATO_DESEMPENHO.csv")

# Garanti que ID_Aluno e ID_Disciplina sejam strings
fato_df = fato_df.withColumn("ID_Aluno", col("ID_Aluno").cast("string"))
fato_df = fato_df.withColumn("ID_Disciplina", col("ID_Disciplina").cast("string"))

# Salvei como tabela Delta no banco default
fato_df.write.format("delta").mode("overwrite").saveAsTable("default.fato_desempenho")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.fato_desempenho
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE default.fato_desempenho;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🔎 Análise Exploratória dos dados

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 🔎 Apresentar as tabelas e visualizações do modelo

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN default;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 🧮 Quantidade de Registros

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC   SELECT 'alunos' AS tabela, COUNT(*) AS total FROM default.alunos
# MAGIC   UNION ALL
# MAGIC   SELECT 'disciplinas', COUNT(*) FROM default.disciplinas
# MAGIC   UNION ALL
# MAGIC   SELECT 'fato_desempenho', COUNT(*) FROM default.fato_desempenho
# MAGIC   UNION ALL
# MAGIC   SELECT 'habitos', COUNT(*) FROM default.habitos
# MAGIC ) AS t
# MAGIC ORDER BY total DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     'alunos' AS tabela, 
# MAGIC     COUNT(*) AS total, 
# MAGIC     SUM(CASE WHEN ID_Aluno IS NULL THEN 1 ELSE 0 END) AS ID_Aluno_nulos,
# MAGIC     SUM(CASE WHEN First_Name IS NULL THEN 1 ELSE 0 END) AS First_Name_nulos
# MAGIC FROM default.alunos;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 📊 Estatísticas descritivas (Exemplos)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     MIN(Age) AS idade_minima,
# MAGIC     MAX(Age) AS idade_maxima,
# MAGIC     AVG(Age) AS idade_media
# MAGIC FROM default.alunos;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Age, COUNT(*) AS quantidade FROM default.alunos GROUP BY Age ORDER BY Age;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     Gender,
# MAGIC     COUNT(*) AS total,
# MAGIC     ROUND(AVG(Age), 2) AS idade_media,
# MAGIC     MIN(Age) AS idade_min,
# MAGIC     MAX(Age) AS idade_max
# MAGIC FROM default.alunos
# MAGIC GROUP BY Gender;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     MIN(Study_Hours_per_Week) AS minimo,
# MAGIC     MAX(Study_Hours_per_Week) AS maximo,
# MAGIC     ROUND(AVG(Study_Hours_per_Week), 2) AS media
# MAGIC FROM default.habitos;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     Conceito,
# MAGIC     COUNT(*) AS total,
# MAGIC     ROUND(AVG(Nota_Total), 2) AS media_total,
# MAGIC     ROUND(AVG(Presenca), 2) AS media_presenca
# MAGIC FROM default.fato_desempenho
# MAGIC GROUP BY Conceito
# MAGIC ORDER BY Conceito;
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt

# Verifica se a coluna Age existe
df_check = spark.sql("SHOW COLUMNS IN default.alunos").toPandas()
if 'Age' not in df_check['col_name'].values:
    raise ValueError("A coluna 'Age' não existe na tabela 'alunos'. Verifique os nomes das colunas.")

# Consulta SQL para unir idade e nota final
query = """
SELECT A.Age, F.Nota_Final 
FROM default.alunos A
JOIN default.fato_desempenho F 
ON A.ID_Aluno = F.ID_Aluno
WHERE A.Age IS NOT NULL AND F.Nota_Final IS NOT NULL
"""

# Converte para Pandas
df = spark.sql(query).toPandas()

# Criar histogramas com Matplotlib
plt.figure(figsize=(14, 5))

# Subplot 1: Idade
plt.subplot(1, 2, 1)
plt.hist(df["Age"], bins=20, color="skyblue", edgecolor="black")
plt.title("Distribuição de Idades - Alunos")
plt.xlabel("Idade")
plt.ylabel("Quantidade")

# Subplot 2: Nota Final
plt.subplot(1, 2, 2)
plt.hist(df["Nota_Final"], bins=20, color="salmon", edgecolor="black")
plt.title("Distribuição das Notas Finais")
plt.xlabel("Nota Final")
plt.ylabel("Quantidade")

plt.tight_layout()
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt

# Listar tabelas para análise
tables = ["alunos", "disciplinas", "fato_desempenho", "habitos"]

# Criar gráficos para cada tabela
for table in tables:
    # Ler a tabela
    df = spark.sql(f"SELECT * FROM default.{table}").toPandas()
    
    # Calcular valores nulos e válidos
    total = df.size
    nulos = df.isnull().sum().sum()
    validos = total - nulos
    
    # Plotar gráfico
    plt.figure(figsize=(4, 4))
    plt.bar(["Válidos", "Nulos"], [validos, nulos], color=["seagreen", "orangered"])
    plt.title(f"Quantidade de Dados - {table}")
    plt.ylabel("Células")
    for i, valor in enumerate([validos, nulos]):
        plt.text(i, valor + total * 0.01, str(valor), ha='center')
    plt.ylim(0, total * 1.1)
    plt.tight_layout()
    plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Entrega 📫

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❗ Problema
# MAGIC Parte dos alunos apresenta baixo desempenho acadêmico, e as causas nem sempre são claras, dificultando ações preventivas e direcionadas.

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Consultar dados do banco e converter para DataFrame Pandas
df_conceitos = spark.sql("""
SELECT 
  Conceito,
  COUNT(*) AS Quantidade_de_Registros
FROM 
  default.fato_desempenho
GROUP BY 
  Conceito
""").toPandas()

# Calcular percentual de cada conceito
df_conceitos['Percentual'] = (df_conceitos['Quantidade_de_Registros'] / df_conceitos['Quantidade_de_Registros'].sum()) * 100

# Ordenar pela ordem lógica dos conceitos
ordem_conceitos = ['A', 'B', 'C', 'D', 'F']
df_conceitos['Conceito'] = pd.Categorical(df_conceitos['Conceito'], categories=ordem_conceitos, ordered=True)
df_conceitos = df_conceitos.sort_values('Conceito', ascending=True)

# Criar gráfico horizontal com Matplotlib
plt.figure(figsize=(8, 5))
plt.barh(df_conceitos['Conceito'], df_conceitos['Percentual'], color='teal', edgecolor='black')

# Adicionar rótulos
for i, (conceito, pct) in enumerate(zip(df_conceitos['Conceito'], df_conceitos['Percentual'])):
    plt.text(pct + 0.5, i, f"{pct:.1f}%", va='center')

plt.title("Percentual de Alunos por Conceito de Desempenho")
plt.xlabel("Percentual (%)")
plt.ylabel("Conceito")
plt.grid(axis='x', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 🎯 Objetivo
# MAGIC
# MAGIC O objetivo é entender **quais fatores contribuem para o baixo desempenho** dos alunos — especialmente os que obtiveram conceito **F**.
# MAGIC
# MAGIC Para isso, unificamos dados acadêmicos, comportamentais e socioeconômicos em uma única tabela: `alunos_unificados`.  
# MAGIC Essa tabela permite que façamos comparações entre os grupos de desempenho (**Alto**, **Médio**, **Baixo**) e identifiquemos padrões relevantes.
# MAGIC
# MAGIC > A análise se concentrará em variáveis como: **estresse, sono, horas de estudo, escolaridade dos pais e renda familiar**.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.alunos_unificados AS
# MAGIC SELECT 
# MAGIC     f.ID_Aluno,
# MAGIC     f.ID_Disciplina,
# MAGIC     CAST(f.Presenca AS DOUBLE) AS Presenca,
# MAGIC     CAST(f.Nota_Midterm AS DOUBLE) AS Nota_Midterm,
# MAGIC     CAST(f.Nota_Final AS DOUBLE) AS Nota_Final,
# MAGIC     CAST(f.Nota_Projetos AS DOUBLE) AS Nota_Projetos,
# MAGIC     CAST(f.Nota_Total AS DOUBLE) AS Nota_Total,
# MAGIC     f.Conceito,
# MAGIC     a.Gender,
# MAGIC     CAST(a.Age AS INT) AS Age,
# MAGIC     a.Parent_Education_Level,
# MAGIC     a.Family_Income_Level,
# MAGIC     CAST(h.Study_Hours_per_Week AS DOUBLE) AS Study_Hours_per_Week,
# MAGIC     h.Extracurricular_Activities,
# MAGIC     h.Internet_Access_at_Home,
# MAGIC     CAST(h.Stress_Level AS INT) AS Stress_Level,
# MAGIC     CAST(h.Sleep_Hours_per_Night AS DOUBLE) AS Sleep_Hours_per_Night,
# MAGIC     CASE
# MAGIC       WHEN f.Conceito IN ('A', 'B') THEN 'Alto'
# MAGIC       WHEN f.Conceito IN ('F') THEN 'Baixo'
# MAGIC       ELSE 'Medio'
# MAGIC     END AS Desempenho
# MAGIC FROM 
# MAGIC     default.fato_desempenho f
# MAGIC LEFT JOIN 
# MAGIC     default.alunos a ON f.ID_Aluno = a.ID_Aluno
# MAGIC LEFT JOIN 
# MAGIC     default.habitos h ON f.ID_Aluno = h.ID_Aluno;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.alunos_unificados
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE default.alunos_unificados;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ✍️ Alunos com Baixo desempenho
# MAGIC Criação de View com Alunos  Baixo Desempenho

# COMMAND ----------

# MAGIC %md
# MAGIC #### 💡 Solução
# MAGIC Integrar e analisar os dados de desempenho, hábitos e perfil socioeconômico dos alunos para encontrar padrões que indiquem risco de baixo desempenho, auxiliando na definição de estratégias de suporte mais eficazes.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 👁️ Construção da View comparativa
# MAGIC O objetivo de comparar dados por desempenhp

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🎲 Interpretação dos dados
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ⏰ Horas de Estudo Média

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   Desempenho,
# MAGIC   ROUND(AVG(Study_Hours_per_Week), 2) AS Media_Horas_Estudo,
# MAGIC   COUNT(*) AS Total_Alunos
# MAGIC FROM default.alunos_com_desempenho
# MAGIC GROUP BY Desempenho
# MAGIC ORDER BY 
# MAGIC   CASE Desempenho
# MAGIC     WHEN 'Alto' THEN 1
# MAGIC     WHEN 'Medio' THEN 2
# MAGIC     WHEN 'Baixo' THEN 3
# MAGIC     ELSE 4
# MAGIC   END;
# MAGIC

# COMMAND ----------

# Consulta a média de horas de estudo por grupo de desempenho
df_estudo = spark.sql("""
SELECT 
  Desempenho,
  ROUND(AVG(Study_Hours_per_Week), 2) AS Media_Horas_Estudo,
  COUNT(*) AS Total_Alunos
FROM default.alunos_com_desempenho
GROUP BY Desempenho
""").toPandas()

import matplotlib.pyplot as plt

# Ordena os grupos de forma lógica
ordem = ['Alto', 'Medio', 'Baixo']
df_estudo['Desempenho'] = pd.Categorical(df_estudo['Desempenho'], categories=ordem, ordered=True)
df_estudo = df_estudo.sort_values('Desempenho')

# Cria o gráfico de barras
plt.figure(figsize=(6, 4))
plt.bar(df_estudo['Desempenho'], df_estudo['Media_Horas_Estudo'], color='cornflowerblue', edgecolor='black')

# Adiciona rótulos nas barras
for i, v in enumerate(df_estudo['Media_Horas_Estudo']):
    plt.text(i, v + 0.1, f'{v:.2f}', ha='center', fontsize=10)

plt.title('Média de Horas de Estudo por Grupo de Desempenho')
plt.ylabel('Horas por Semana')
plt.xlabel('Desempenho')
plt.ylim(0, df_estudo['Media_Horas_Estudo'].max() + 1)
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 💤 Horas de sono Média

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   Desempenho,
# MAGIC   ROUND(AVG(Sleep_Hours_per_Night),2) AS Media_Horas_Sono
# MAGIC FROM default.alunos_com_desempenho
# MAGIC GROUP BY Desempenho
# MAGIC ORDER BY 
# MAGIC   CASE Desempenho WHEN 'Alto' THEN 1 WHEN 'Medio' THEN 2 ELSE 3 END;
# MAGIC

# COMMAND ----------

# Consulta a média de horas de sono por grupo de desempenho
df_sono = spark.sql("""
SELECT 
  Desempenho,
  Sleep_Hours_per_Night
FROM default.alunos_com_desempenho
WHERE Sleep_Hours_per_Night IS NOT NULL
""").toPandas()

import matplotlib.pyplot as plt

# Ordena os grupos
ordem = ['Alto', 'Medio', 'Baixo']
df_sono['Desempenho'] = pd.Categorical(df_sono['Desempenho'], categories=ordem, ordered=True)
df_sono = df_sono.sort_values('Desempenho')

# Cria boxplot
plt.figure(figsize=(6, 4))
df_sono.boxplot(column='Sleep_Hours_per_Night', by='Desempenho', grid=False, patch_artist=True,
                boxprops=dict(facecolor='lightblue', color='black'),
                medianprops=dict(color='black'),
                whiskerprops=dict(color='black'),
                capprops=dict(color='black'),
                flierprops=dict(markerfacecolor='red', marker='o', markersize=4, linestyle='none'))

plt.title('Distribuição de Horas de Sono por Grupo de Desempenho')
plt.suptitle('')
plt.xlabel('Desempenho')
plt.ylabel('Horas de Sono por Noite')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 😡 Nivel de estresse médio

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   Desempenho,
# MAGIC   COUNT(CASE WHEN Stress_Level >= 7 THEN 1 END) AS Alunos_Estresse_Alto,
# MAGIC   COUNT(*) AS Total_Alunos,
# MAGIC   ROUND((COUNT(CASE WHEN Stress_Level >= 7 THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Estresse_Alto
# MAGIC FROM default.alunos_com_desempenho
# MAGIC GROUP BY Desempenho
# MAGIC ORDER BY 
# MAGIC   CASE Desempenho WHEN 'Alto' THEN 1 WHEN 'Medio' THEN 2 ELSE 3 END;
# MAGIC

# COMMAND ----------

# Consulta o percentual de estresse alto por grupo de desempenho
df_estresse = spark.sql("""
SELECT 
  Desempenho,
  COUNT(CASE WHEN Stress_Level >= 7 THEN 1 END) AS Alunos_Estresse_Alto,
  COUNT(*) AS Total_Alunos,
  ROUND((COUNT(CASE WHEN Stress_Level >= 7 THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Estresse_Alto
FROM default.alunos_com_desempenho
GROUP BY Desempenho
""").toPandas()

import matplotlib.pyplot as plt

# Ordena os grupos
ordem = ['Alto', 'Medio', 'Baixo']
df_estresse['Desempenho'] = pd.Categorical(df_estresse['Desempenho'], categories=ordem, ordered=True)
df_estresse = df_estresse.sort_values('Desempenho')

# Cria gráfico de barras com percentual
plt.figure(figsize=(6, 4))
plt.bar(df_estresse['Desempenho'], df_estresse['Percentual_Estresse_Alto'], color='tomato', edgecolor='black')

# Adiciona rótulos
for i, v in enumerate(df_estresse['Percentual_Estresse_Alto']):
    plt.text(i, v + 1, f'{v:.1f}%', ha='center', fontsize=10)

plt.title('Percentual de Alunos com Estresse Alto (≥ 7)')
plt.ylabel('Percentual (%)')
plt.xlabel('Desempenho')
plt.ylim(0, df_estresse['Percentual_Estresse_Alto'].max() + 10)
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##### 🎓 4. Escolaridade dos Pais (Proporção de Pais com Ensino Médio ou Inferior)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   Desempenho,
# MAGIC   COUNT(CASE WHEN Parent_Education_Level IN ('High School', 'None') THEN 1 END) AS Pais_Baixa_Escolaridade,
# MAGIC   COUNT(*) AS Total_Alunos,
# MAGIC   ROUND((COUNT(CASE WHEN Parent_Education_Level IN ('High School', 'None') THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Pais_Baixa_Escolaridade
# MAGIC FROM default.alunos_com_desempenho
# MAGIC GROUP BY Desempenho
# MAGIC ORDER BY 
# MAGIC   CASE Desempenho WHEN 'Alto' THEN 1 WHEN 'Medio' THEN 2 ELSE 3 END;
# MAGIC

# COMMAND ----------

# Consulta o percentual de pais com baixa escolaridade por grupo de desempenho
df_escolaridade = spark.sql("""
SELECT 
  Desempenho,
  COUNT(CASE WHEN Parent_Education_Level IN ('High School', 'None') THEN 1 END) AS Pais_Baixa_Escolaridade,
  COUNT(*) AS Total_Alunos,
  ROUND((COUNT(CASE WHEN Parent_Education_Level IN ('High School', 'None') THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Pais_Baixa_Escolaridade
FROM default.alunos_com_desempenho
GROUP BY Desempenho
""").toPandas()

import matplotlib.pyplot as plt

# Ordena os grupos
ordem = ['Alto', 'Medio', 'Baixo']
df_escolaridade['Desempenho'] = pd.Categorical(df_escolaridade['Desempenho'], categories=ordem, ordered=True)
df_escolaridade = df_escolaridade.sort_values('Desempenho')

# Cria gráfico de barras com percentual
plt.figure(figsize=(6, 4))
plt.bar(df_escolaridade['Desempenho'], df_escolaridade['Percentual_Pais_Baixa_Escolaridade'], color='darkorange', edgecolor='black')

# Adiciona rótulos
for i, v in enumerate(df_escolaridade['Percentual_Pais_Baixa_Escolaridade']):
    plt.text(i, v + 1, f'{v:.1f}%', ha='center', fontsize=10)

plt.title('Pais com Escolaridade Baixa por Grupo de Desempenho')
plt.ylabel('Percentual (%)')
plt.xlabel('Desempenho')
plt.ylim(0, df_escolaridade['Percentual_Pais_Baixa_Escolaridade'].max() + 10)
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##### 💰 5. Renda Familiar (Proporção de Alunos com Renda Familiar Baixa)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   Desempenho,
# MAGIC   COUNT(CASE WHEN Family_Income_Level = 'Low' THEN 1 END) AS Alunos_Baixa_Renda,
# MAGIC   COUNT(*) AS Total_Alunos,
# MAGIC   ROUND((COUNT(CASE WHEN Family_Income_Level = 'Low' THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Baixa_Renda
# MAGIC FROM default.alunos_com_desempenho
# MAGIC GROUP BY Desempenho
# MAGIC ORDER BY 
# MAGIC   CASE Desempenho WHEN 'Alto' THEN 1 WHEN 'Medio' THEN 2 ELSE 3 END;
# MAGIC

# COMMAND ----------

# Consulta o percentual de alunos com baixa renda por grupo
df_renda = spark.sql("""
SELECT 
  Desempenho,
  COUNT(CASE WHEN Family_Income_Level = 'Low' THEN 1 END) AS Alunos_Baixa_Renda,
  COUNT(*) AS Total_Alunos,
  ROUND((COUNT(CASE WHEN Family_Income_Level = 'Low' THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Baixa_Renda
FROM default.alunos_com_desempenho
GROUP BY Desempenho
""").toPandas()

import matplotlib.pyplot as plt

# Ordena os grupos
ordem = ['Alto', 'Medio', 'Baixo']
df_renda['Desempenho'] = pd.Categorical(df_renda['Desempenho'], categories=ordem, ordered=True)
df_renda = df_renda.sort_values('Desempenho')

# Cria gráfico de barras
plt.figure(figsize=(6, 4))
plt.bar(df_renda['Desempenho'], df_renda['Percentual_Baixa_Renda'], color='firebrick', edgecolor='black')

# Adiciona rótulos
for i, v in enumerate(df_renda['Percentual_Baixa_Renda']):
    plt.text(i, v + 1, f'{v:.1f}%', ha='center', fontsize=10)

plt.title('Alunos com Baixa Renda Familiar por Desempenho')
plt.ylabel('Percentual (%)')
plt.xlabel('Desempenho')
plt.ylim(0, df_renda['Percentual_Baixa_Renda'].max() + 10)
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 🏃 6. Atividades Extracurriculares (Proporção que participam)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   Desempenho,
# MAGIC   COUNT(CASE WHEN Extracurricular_Activities = 'Yes' THEN 1 END) AS Com_Atividades,
# MAGIC   COUNT(*) AS Total_Alunos,
# MAGIC   ROUND((COUNT(CASE WHEN Extracurricular_Activities = 'Yes' THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Com_Atividades
# MAGIC FROM default.alunos_com_desempenho
# MAGIC GROUP BY Desempenho
# MAGIC ORDER BY 
# MAGIC   CASE Desempenho WHEN 'Alto' THEN 1 WHEN 'Medio' THEN 2 ELSE 3 END;
# MAGIC

# COMMAND ----------

# Consulta o percentual de alunos com atividades extracurriculares por grupo
df_atividades = spark.sql("""
SELECT 
  Desempenho,
  COUNT(CASE WHEN Extracurricular_Activities = 'Yes' THEN 1 END) AS Com_Atividades,
  COUNT(*) AS Total_Alunos,
  ROUND((COUNT(CASE WHEN Extracurricular_Activities = 'Yes' THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Com_Atividades
FROM default.alunos_com_desempenho
GROUP BY Desempenho
""").toPandas()

import matplotlib.pyplot as plt

# Ordena os grupos
ordem = ['Alto', 'Medio', 'Baixo']
df_atividades['Desempenho'] = pd.Categorical(df_atividades['Desempenho'], categories=ordem, ordered=True)
df_atividades = df_atividades.sort_values('Desempenho')

# Cria gráfico de barras
plt.figure(figsize=(6, 4))
plt.bar(df_atividades['Desempenho'], df_atividades['Percentual_Com_Atividades'], color='mediumseagreen', edgecolor='black')

# Adiciona rótulos
for i, v in enumerate(df_atividades['Percentual_Com_Atividades']):
    plt.text(i, v + 1, f'{v:.1f}%', ha='center', fontsize=10)

plt.title('Participação em Atividades Extracurriculares por Desempenho')
plt.ylabel('Percentual (%)')
plt.xlabel('Desempenho')
plt.ylim(0, df_atividades['Percentual_Com_Atividades'].max() + 10)
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()




# COMMAND ----------

# MAGIC %md
# MAGIC ##### 🌐 7. Acesso à Internet em Casa (Proporção sem acesso)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   Desempenho,
# MAGIC   COUNT(CASE WHEN Internet_Access_at_Home = 'No' THEN 1 END) AS Sem_Internet,
# MAGIC   COUNT(*) AS Total_Alunos,
# MAGIC   ROUND((COUNT(CASE WHEN Internet_Access_at_Home = 'No' THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Sem_Internet
# MAGIC FROM default.alunos_com_desempenho
# MAGIC GROUP BY Desempenho
# MAGIC ORDER BY 
# MAGIC   CASE Desempenho WHEN 'Alto' THEN 1 WHEN 'Medio' THEN 2 ELSE 3 END;
# MAGIC

# COMMAND ----------

# Consulta o percentual de alunos sem acesso à internet por grupo
df_internet = spark.sql("""
SELECT 
  Desempenho,
  COUNT(CASE WHEN Internet_Access_at_Home = 'No' THEN 1 END) AS Sem_Internet,
  COUNT(*) AS Total_Alunos,
  ROUND((COUNT(CASE WHEN Internet_Access_at_Home = 'No' THEN 1 END) / COUNT(*)) * 100, 2) AS Percentual_Sem_Internet
FROM default.alunos_com_desempenho
GROUP BY Desempenho
""").toPandas()

import matplotlib.pyplot as plt

# Ordena os grupos
ordem = ['Alto', 'Medio', 'Baixo']
df_internet['Desempenho'] = pd.Categorical(df_internet['Desempenho'], categories=ordem, ordered=True)
df_internet = df_internet.sort_values('Desempenho')

# Cria gráfico de barras
plt.figure(figsize=(6, 4))
plt.bar(df_internet['Desempenho'], df_internet['Percentual_Sem_Internet'], color='slategray', edgecolor='black')

# Adiciona rótulos
for i, v in enumerate(df_internet['Percentual_Sem_Internet']):
    plt.text(i, v + 1, f'{v:.1f}%', ha='center', fontsize=10)

plt.title('Alunos Sem Acesso à Internet em Casa por Desempenho')
plt.ylabel('Percentual (%)')
plt.xlabel('Desempenho')
plt.ylim(0, df_internet['Percentual_Sem_Internet'].max() + 10)
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 📌 Conclusão do Projeto
# MAGIC
# MAGIC O objetivo deste projeto foi identificar os fatores comportamentais e socioeconômicos que influenciam negativamente o desempenho acadêmico dos alunos, com base em uma base integrada de dados escolares, perfis familiares e hábitos individuais.
# MAGIC
# MAGIC Os alunos foram classificados em três grupos de desempenho: **Alto (conceito A/B)**, **Médio (C/D)** e **Baixo (F)**. As análises mostraram que apenas 16,8% dos estudantes apresentaram conceito **F**, o que já representa um grupo considerável em situação crítica. Ao comparar esse grupo com os demais, foram identificadas diferenças marcantes em diversos aspectos.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔎 Principais descobertas com base nos dados
# MAGIC
# MAGIC - **Horas de estudo**: Os três grupos estudam em média a mesma quantidade de horas por semana. O grupo de alto desempenho estudou **17,71h/semana**, e o de baixo desempenho, **17,63h/semana**. Isso indica que **o tempo de estudo não é um fator discriminante relevante** entre os grupos.
# MAGIC
# MAGIC - **Sono**: Alunos com baixo desempenho dormem, em média, **1 hora a menos por noite** que os de alto desempenho. A média no grupo com conceito **F** foi de **6,1 horas**, enquanto o grupo **A/B** apresentou média de **7,1 horas**.
# MAGIC
# MAGIC - **Estresse**: Cerca de **43,4%** dos alunos com desempenho **F** registraram nível de estresse **igual ou superior a 7**. No grupo de alto desempenho, essa proporção cai para apenas **16,9%**. Esse é um dos fatores com maior variação entre os grupos, indicando **forte relação entre estresse elevado e desempenho acadêmico ruim**.
# MAGIC
# MAGIC - **Escolaridade dos pais**: No grupo de baixo desempenho, **55,2%** dos alunos têm pais com escolaridade até o ensino médio ou sem formação. No grupo de alto desempenho, essa proporção é de **26,8%**. Isso sugere que **contexto familiar e capital cultural influenciam diretamente nos resultados dos alunos**.
# MAGIC
# MAGIC - **Atividades extracurriculares**: Apenas **29,1%** dos alunos com conceito F participam de atividades extracurriculares, enquanto entre os alunos de alto desempenho esse número sobe para **49,2%**. Isso pode indicar que engajamento em experiências para além da sala de aula impacta positivamente no desempenho.
# MAGIC
# MAGIC - **Acesso à internet**: **16,3%** dos alunos com conceito F não têm acesso à internet em casa. No grupo de alto desempenho, apenas **4,1%** estão na mesma condição. Esse dado evidencia **um problema estrutural de desigualdade de acesso à tecnologia e informação**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧭 Caminhos para intervenção
# MAGIC
# MAGIC Com base nos dados levantados, recomenda-se que escolas e gestores educacionais considerem as seguintes estratégias:
# MAGIC
# MAGIC - **Implantar programas de regulação emocional e redução de estresse**, como acompanhamento psicológico e oficinas de autocuidado;
# MAGIC - **Oferecer suporte a famílias com baixa escolaridade**, com ações de conscientização sobre o papel do ambiente familiar no desempenho escolar;
# MAGIC - **Ampliar o acesso à internet e dispositivos digitais** para alunos em situação de vulnerabilidade;
# MAGIC - **Estimular a participação em atividades extracurriculares**, que promovem habilidades socioemocionais e senso de pertencimento.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Considerações finais
# MAGIC
# MAGIC Os dados mostram com clareza que **o baixo desempenho acadêmico está associado a uma série de fatores emocionais, sociais e estruturais**, e não apenas à dedicação aos estudos.
# MAGIC
# MAGIC Estudantes com menos horas de sono, maior estresse, baixa participação social e contextos familiares mais vulneráveis apresentam desempenho significativamente inferior, mesmo estudando tanto quanto os demais.
# MAGIC
# MAGIC Esses achados reforçam a importância de uma abordagem educacional integrada, que reconheça o papel do contexto e da saúde mental na jornada de aprendizagem. Com base nessas evidências, é possível direcionar políticas públicas e intervenções escolares mais eficazes, garantindo uma educação mais justa, acolhedora e transformadora.
# MAGIC