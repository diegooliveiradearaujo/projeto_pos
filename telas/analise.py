import streamlit as st
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import SparkSession
import plotly.express as px
import altair as alt
#from streamlit_folium import st_folium
#import folium 
#from geopy.geocoders import Nominatim

spark = SparkSession.builder.appName('one').getOrCreate() 

def analise():
    st.image("img/analise.jpg",width=740,use_column_width=True)

    servico = st.selectbox('Qual é o serviço referente para a análise de dados?',
                              ('AAA', 'Broadband', 'Browsing'))
    mes = st.selectbox('Qual é o mês referente para a análise de dados?',
                              ('Janeiro', 'Fevereiro', 'Março','Abril','Maio','Junho','Julho','Agosto',
                               'Setembro','Outubro','Novembro','Dezembro'))
    if st.button('Analisar'):
        try:
            schema_def = StructType()
            schema_def.add("user","string",True)      
            schema_def.add("razao_social","string",True)     
            schema_def.add("regiao","string",True)  
            schema_def.add("produto","string",True)      
            schema_def.add("categoria","string",True)     
            schema_def.add("valor","float",True)
            schema_def.add("satisfacao","integer",True)      
            schema_def.add("leg_satisfacao","string",True)     
            schema_def.add("latitude","float",True)
            schema_def.add("longitude","float",True)      
                
            dataframe_spark = spark.read.csv("hdfs://localhost:9000//root/NOKIA_Analytics/report/"+"report"+"_"+mes+"_"+servico,sep=',',schema=schema_def)
            dataframe = dataframe_spark.toPandas()
            
            st.write(dataframe_spark)
            
            dataframe['satisfacao'] = dataframe['satisfacao'].astype(int)
            agrup_valor_por_regiao_sum = dataframe.groupby('regiao')['valor'].sum()
            #valor_per_regiao_sum = dataframe.groupBy('regiao').sum('valor')

            agrup_regiao_por_usuario_count = dataframe.groupby('regiao')['user'].count()
            #regiao_per_user_count = dataframe_spark.groupBy('regiao').count()

            agrup_satisfacao = dataframe.groupby('leg_satisfacao')['satisfacao'].count()

            #RESET INDEX
            agrup_valor_por_regiao_sum = agrup_valor_por_regiao_sum.reset_index()
            agrup_regiao_por_usuario_count = agrup_regiao_por_usuario_count.reset_index()
            agrup_satisfacao = agrup_satisfacao.reset_index()

            st.markdown("<h7 style='text-align: center; color: white;'>Total de vendas (R$) por região</h7>", unsafe_allow_html=True)
            line_fig = px.bar(agrup_valor_por_regiao_sum, x="regiao", y='valor', color='regiao',width=800, height=500)
            st.plotly_chart(line_fig, use_container_width=False, sharing='streamlit')

            st.markdown("<h7 style='text-align: center; color: white;'>Quantidade de clientes por região</h7>", unsafe_allow_html=True)
            #st.bar_chart(agrup_regiao_por_usuario_count,x='regiao',y='user')
            c = alt.Chart(agrup_regiao_por_usuario_count).mark_bar().encode(
            x=alt.X('user:Q'),y=alt.Y('regiao:N')).properties(height=500)

            st.altair_chart(c, use_container_width=True)

            st.markdown("<h7 style='text-align: center; color: white;'>Distribuição da satisfação dos clientes</h7>", unsafe_allow_html=True)
            pie_fig = px.pie(agrup_satisfacao, values='satisfacao', names='leg_satisfacao')
            st.plotly_chart(pie_fig)         
                
        except:
            st.write("Não há dados!")