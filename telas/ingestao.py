import streamlit as st
import pandas as pd
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

def ingestao():
    col1, col2, col3 = st.columns([1,6,1])
    with col1:
        st.write("")
    with col2:
        st.image("img/xlsx_csv_parquet.jpg")
    with col3:
        st.write("")
    
    mes = st.selectbox('Qual é o mês referente a extração de dados?',
                       ('Janeiro', 'Fevereiro', 'Março','Abril','Maio','Junho','Julho','Agosto',
                        'Setembro','Outubro','Novembro','Dezembro'))
    servico = st.radio("Qual é o tipo de serviço da base de dados?",
                       ('AAA', 'Broadband', 'Browsing'))
    if servico == 'AAA':
        servico_res = 'AAA'
    elif servico == 'Broadband':
        servico_res = 'BB'
    elif servico == 'Brwosing':
        servico_res = 'BWS'

        
    carregamento_arquivo = st.file_uploader("Anexe o arquivo para ingestão de dados")
    if carregamento_arquivo is not None:
        #bytes_arquivo = carregamento_arquivo.getvalue()
        dataframe = pd.read_csv(carregamento_arquivo, encoding = "latin-1", on_bad_lines='skip', sep=';')
        
    if st.button('Ingerir Dados'):
        
        spark = SparkSession.builder.appName('one').getOrCreate()  
        dataframe_spark=spark.createDataFrame(dataframe) 

        udf = F.UserDefinedFunction(lambda x: x.replace(",","."), T.StringType())
        out = dataframe_spark.withColumn("valor", udf(F.col("valor")).cast(T.FloatType()))
        etl_dataframe_spark = out.withColumn("valor",F.round(out["valor"]))

        dataframe_spark.write.csv("hdfs://localhost:9000//root/NOKIA_Analytics/raw/"+"raw"+"_"+mes+"_"+servico)

        etl_dataframe_spark.write.csv("hdfs://localhost:9000//root/NOKIA_Analytics/report/"+"report"+"_"+mes+"_"+servico)
        #novo = out1.withColumn("valor",F.round(out1["valor"]))
        #novo = out.withColumn("valor",F.round(out.valor.cast(T.DoubleType())))

        st.write("Base de Dados enviada com sucesso!")
           
        
            