**Overview**

> The Medallion Architecture is implemented using Databricks to manage data across different layers through distinct catalogs by implementing the simple ETL pipeline. The data is initially ingested from Google Cloud Storage as CSV files and written into Delta Lake tables as bronze (raw) data. This data is then processed through a series of transformations to enrich and aggregate it into silver and gold layers for advanced analytics and reporting.

> **Catalogs Overview**
> 
> - **Bronze Catalog:** Contains raw, unprocessed data.
> 
> - **Silver Catalog:** Holds cleaned and enriched data with preliminary transformations.
> 
> - **Gold Catalog:** Used for highly refined, analytical data optimized for business intelligence and reporting.

**Use Case**

> **Objective:** Analysts aim to understand how sales performance varies by customer demographics and geographic regions over time. This analysis enables businesses to identify trends, high-performing regions, and customer preferences, facilitating targeted marketing and sales strategies.
> 
> **Data Sources:**
> 
> - **Tables Used:** customers and invoices
> 
> - **Data Location:** Google Cloud Storage (GCS)
> 
> The ETL workflow involves processing data from GCS in Databricks and storing it in Delta Lake. Visualizations can be performed within Databricks or connected to other BI tools via Databricks Partner Connect.
> 
**Solution Architecture**

![Architecture](https://github.com/user-attachments/assets/afb00f63-0582-44fc-b3b6-12b6918b6307)

**Workflow**

> ![image](https://github.com/user-attachments/assets/7c55e4bb-8df5-483c-a29e-4788a57ff94d)
> 
