<h1>TEST: BEES Data Engineering – Breweries Case</h1>

<h1>Solution:</h1> Airflow > Databricks </br> </br>
As an orchestrator, I used Airflow to manage the pipeline, which was built as a medallion model in Unity Catalog on Databricks. </br></br>

Bronze: In the bronze code, I made the request to the API (since the API doesn't require credentials, I didn't need to create a Secrets). In this layer, I created the table schema with varchar and used append to store the data. </br> </br>
Silver: The fields were typed, transformation and an incremental load process was created. </br> </br>
Gold: I created two versions: one with an incremental table for performance and another with a view, as requested in the test. </br> </br>

<h2>Otimization</h2>
If you need to optimize the table's performance, you can cluster the table and enable autoOptimize. </br> </br>

ALTER TABLE bronze.api.breweries_case CLUSTER BY (id ,insertion_at) </br>
ALTER TABLE silver.api.breweries_case CLUSTER BY (id ,insertion_at) </br>
ALTER TABLE gold.api.breweries_case CLUSTER BY (id ,insertion_at) </br>

ALTER TABLE bronze.api.breweries_case SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true'); </br>
ALTER TABLE silver.api.breweries_case SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true'); </br>
ALTER TABLE gold.api.breweries_case SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true'); </br>

<h2>Alert / Error </h2>
I added alerts to the Databricks job in case of errors, along with two additional retry attempts in case of failure, with a 5-minute interval, in case the API experiences any error.

<h3> Evidence databricks table: </h3>
![image](https://github.com/user-attachments/assets/266ab0b9-2001-402a-b2f8-2577c10886e9)


<h3> Evidence databricks job:  </h3>
![Captura de tela 2024-11-11 023241](https://github.com/user-attachments/assets/0dae19fb-b7b2-447a-81eb-4642756168a9)

<h3>  Evidence airflow: </h3>
![Captura de tela 2024-11-11 024246](https://github.com/user-attachments/assets/a22c1000-205c-41e6-a17e-dbad48d248a7)



