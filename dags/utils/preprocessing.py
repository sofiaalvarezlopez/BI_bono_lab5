import pandas as pd
import requests
import os

def leer_datos():
    # Primero se crea una carpeta para almacenar los archivos:
    try: 
        os.mkdir('datos')
    except:
        pass
    # Ahora, se descargan los archivos
    ## Dimension city
    req = requests.get("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/datalakeBI/dimension_city.csv?op=OPEN&user.name=cursobi06")
    url_content = req.content
    csv_file = open('datos/dimension_city.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()
    ## Dimension Customer
    req = requests.get("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/datalakeBI/dimension_customer.csv?op=OPEN&user.name=cursobi06")
    url_content = req.content
    csv_file = open('datos/dimension_customer.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()
    ## Dimension Date
    req = requests.get("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/datalakeBI/dimension_date.csv?op=OPEN&user.name=cursobi06")
    url_content = req.content
    csv_file = open('datos/dimension_date.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()
    ## Dimension Employee
    req = requests.get("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/datalakeBI/dimension_employee.csv?op=OPEN&user.name=cursobi06")
    url_content = req.content
    csv_file = open('datos/dimension_employee.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()
    ## Dimension Stock item
    req = requests.get("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/datalakeBI/dimension_stock_item.csv?op=OPEN&user.name=cursobi06")
    url_content = req.content
    csv_file = open('datos/dimension_stock_item.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()
    ## Fact Table
    req = requests.get("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/datalakeBI/fact_order.csv?op=OPEN&user.name=cursobi06")
    url_content = req.content
    csv_file = open('datos/fact_order.csv', 'wb')
    csv_file.write(url_content)
    csv_file.close()

def preprocessing():
    # Llamo a la funcion auxiliar de leer datos
    leer_datos()
    # Creo la carpeta data (si no existe)
    try:
         os.os.mkdir('data')
    except:
        pass
    ## Dimension city:
    datos_dim_city = pd.read_csv('datos/dimension_city.csv', delimiter=',')
    datos_dim_city = datos_dim_city.drop(0)
    datos_dim_city = datos_dim_city.drop(['row ID'], axis=1)
    datos_dim_city.to_csv("data/dimension_city.csv")
    ## Dimension Customer:
    datos_dim_cust = pd.read_csv('datos/dimension_customer.csv', delimiter=',', encoding='utf-8', encoding_errors='ignore')
    datos_dim_cust = datos_dim_cust.drop(0)
    datos_dim_cust['Postal_Code'] = datos_dim_cust['Postal_Code'].astype(int)
    datos_dim_cust['Bill_To_Customer'] = datos_dim_cust['Bill_To_Customer'].str.replace("'", "")
    datos_dim_cust['Category'] = datos_dim_cust['Category'].str.replace("'", "")
    datos_dim_cust['Buying_Group'] = datos_dim_cust['Buying_Group'].str.replace("'", "")
    datos_dim_cust['Customer'] = datos_dim_cust['Customer'].str.replace("'", "")
    datos_dim_cust.to_csv("data/dimension_customer.csv")
    ## Dimension Date - No hay preprocesamiento
    datos_dim_date = pd.read_csv('datos/dimension_date.csv', delimiter=',', encoding='utf-8', encoding_errors='ignore')
    datos_dim_date.to_csv("data/dimension_date.csv")
    ## Dimension Employee
    datos_dim_emp = pd.read_csv('datos/dimension_employee.csv', delimiter=',', encoding='utf-8')
    datos_dim_emp = datos_dim_emp.drop(0)
    datos_dim_emp['Is_Salesperson'] = datos_dim_emp['Is_Salesperson'].astype(int) 
    datos_dim_emp['Is_Salesperson'] = datos_dim_emp['Is_Salesperson'].astype(str) 
    datos_dim_emp.to_csv("data/dimension_employee.csv")
    ## Dimension Stock item
    datos_dim_stock = pd.read_csv('datos/dimension_stock_item.csv', delimiter=',', encoding='utf-8', encoding_errors='ignore')
    datos_dim_stock = datos_dim_stock.drop(0)
    # Cambiando los valores booleanos
    datos_dim_stock['Is_Chiller_Stock'] = datos_dim_stock['Is_Chiller_Stock'].astype(int) 
    datos_dim_stock['Is_Chiller_Stock'] = datos_dim_stock['Is_Chiller_Stock'].astype(str)
    # Llenando los valores nulos
    datos_dim_stock.fillna('NA', inplace=True)
    # Cambiando los datos de string a numerico
    datos_dim_stock[['Tax_Rate', 'Unit_Price', 'Recommended_Retail_Price', 'Typical_Weight_Per_Unit']] = datos_dim_stock[['Tax_Rate', 'Unit_Price', 'Recommended_Retail_Price', 'Typical_Weight_Per_Unit']].replace(',', '.', regex=True)
    datos_dim_stock[['Tax_Rate', 'Unit_Price', 'Recommended_Retail_Price', 'Typical_Weight_Per_Unit']] = datos_dim_stock[['Tax_Rate', 'Unit_Price', 'Recommended_Retail_Price', 'Typical_Weight_Per_Unit']].astype(float)
    datos_dim_stock['Stock_Item'] = datos_dim_stock['Stock_Item'].str.replace("'", "")
    datos_dim_stock.to_csv("data/dimension_stock_item.csv")
    # Fact table - No hay preprocesamiento
    fact_table = pd.read_csv('datos/fact_order.csv', delimiter=',', encoding='utf-8', encoding_errors='ignore')
    fact_table.to_csv("data/fact_order.csv")

#leer_datos()
