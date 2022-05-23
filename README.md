# Bono: Laboratorio 5 - BI
Este repositorio contiene el código con el preprocesamiento incluido en el DAG. Lo primero que se hizo fue descargar los archivos usando peticiones HTTP, 
como puede verse en la función <code>leer_datos()</code> del archivo <code>preprocessing.py</code>. Un ejemplo de esta lectura, usando la librería <code>
requests</code> de Python, para la tabla dimensión City, se muestra a continuación:
```python
req = requests.get("http://bigdata-cluster4-01.virtual.uniandes.edu.co:50070/webhdfs/v1/datalakeBI/dimension_city.csv?op=OPEN&user.name=cursobi06")
url_content = req.content
csv_file = open('datos/dimension_city.csv', 'wb')
csv_file.write(url_content)
csv_file.close()
```
Dicha función es llamada en el preprocesamiento, en la función <code>preprocessing()</code> el cual hace las transformaciones que, en la entrega normal 
del laboratorio, se hacían en el notebook de <code>jupyter</code>.

Luego, en el archivo <code>ELT.py</code>, se agregaba el task usando el operador de Python de Airflow, como se ve a continuación:
```python
# task: 0 - preprocesamiento
preprocesamiento = PythonOperator(
task_id='preprocesamiento',
python_callable=preprocessing
)
```
Por último, se modificaba el flujo de ejecución de las tareas para agregar el paso de preprocesamiento creado previamente:
```python
# flujo de ejecución de las tareas  
preprocesamiento >> crear_tablas_db >> poblar_tablas_dimensiones >> poblar_fact_order
```
Finalmente, se volvía a ejecutar el ELT, con lo cual se obtuvo el siguiente grafo dirigido acíclico, como era de esperarse:
<img width="738" alt="grafo_preprocesamiento" src="https://user-images.githubusercontent.com/41558363/169764003-08bc4fc7-a4a5-47a1-af68-aec93308705e.png">

Note que este grafo incluye ahora el paso de preprocesamiento previamente creado.

Finalmente, el árbol de ejecución, en su última corrida, muestra como todos los pasos fueron satisfactoriamente realizados:
<img width="543" alt="arbol_preprocesamiento" src="https://user-images.githubusercontent.com/41558363/169764171-5c064efb-f95e-4cdd-91b7-7dafdf4afc88.png">

**Nota:** Para ver las imágenes donde se adjunta también el usuario PostgreSQL del grupo 6, remítase a <a href="https://github.com/sofiaalvarezlopez/BI_bono_lab5/blob/main/images/Anexo_preprocesamiento_grafo.png">
  esta imagen para el grafo</a> y a <a href="https://github.com/sofiaalvarezlopez/BI_bono_lab5/blob/main/images/Anexo_preprocesamiento_arbol.png">esta imagen para el árbol</a>.
