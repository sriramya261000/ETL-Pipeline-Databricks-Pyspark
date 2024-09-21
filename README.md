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

![Architecture](https://github.com/user-attachments/assets/b8874b87-4861-40ae-b011-ea7ee132c7a9)

**Workflow**

![Workflow](https://github.com/user-attachments/assets/84415a5e-3e8f-48b7-9f4c-80046b6577b9)


