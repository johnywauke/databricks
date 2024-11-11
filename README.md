<h1> Databricks Airflow Pipeline - Modelo Medallion </h1></br>
Este projeto documenta um pipeline de dados orquestrado com Airflow, utilizando o Databricks como plataforma para transformação de dados no modelo medallion (Bronze, Silver, Gold).</br>

<h2> Visão Geral do Projeto </h2>
O pipeline é responsável por: Johny Wauke</br>

Extração de dados de uma API pública.</br>
Processamento e armazenamento de dados em camadas, seguindo o modelo medallion.</br>
Otimização de desempenho com clustering e propriedades Delta.</br>
Configuração de alertas e tentativas em caso de falha.</br>
<h2> Estrutura das Camadas </h2>
</br>
<h4>Bronze: </h4>

Extrai dados da API e os armazena em sua forma bruta.</br>
O schema é definido com varchar e o modo de escrita append.</br>
Como a API não requer autenticação, não foi necessário configurar secrets.</br>
</br></br>
<h4>Silver: </h4>

Realiza tipagem dos campos e transformações necessárias.</br>
Configura a carga incremental para otimização de performance.</br>
</br></br>

<h4>Gold: </h4>
Criei duas versões: </br>
Uma tabela incremental para melhor desempenho.</br>
Uma view conforme solicitado, para visualização dos dados.</br></br>

<h2> Configuração de Otimização </h2> </br>
Para otimizar as tabelas, utilizamos clustering e habilitamos a propriedade de auto-otimização nas tabelas Delta.</br>

<h3> -- Clustering </h3></br>
ALTER TABLE bronze.api.breweries_case CLUSTER BY (id, insertion_at);</br>
ALTER TABLE silver.api.breweries_case CLUSTER BY (id, insertion_at);</br>
ALTER TABLE gold.api.breweries_case CLUSTER BY (id, insertion_at);</br>

<h3> -- Auto-otimização </h3></br>
ALTER TABLE bronze.api.breweries_case SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');</br>
ALTER TABLE silver.api.breweries_case SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');</br>
ALTER TABLE gold.api.breweries_case SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');</br>

<h2> Alertas e Tratamento de Erros </h2></br>
O job no Databricks possui alertas configurados para informar em caso de erros.</br>
Foram configuradas duas tentativas adicionais em caso de falha, com um intervalo de 5 minutos entre as execuções, para evitar falhas devido a problemas temporários da API.</br>

<h2> Execução do Pipeline </h2></br>
Airflow: Utilize o Airflow para iniciar e monitorar o pipeline.</br>
Databricks: Acompanhe a execução dos jobs e o status das tabelas no Unity Catalog.</br>

<h2> Evidências </h2></br>
Inclua capturas de tela ou exemplos de registros das tabelas Bronze, Silver e Gold no Databricks para referência e validação.</br>

<h3> Evidence databricks table: </h3>

![ev01](https://github.com/user-attachments/assets/5b8f8411-f90e-4460-9858-e7221bf4326d)

<h3> Evidence databricks job:  </h3>

![ev02](https://github.com/user-attachments/assets/30b5e18b-b43e-4434-881d-d50495e689b6)

<h3>  Evidence airflow: </h3>

![ev03](https://github.com/user-attachments/assets/692bb3d8-cb55-433e-acb8-8f798c638d22)




