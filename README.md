# mega-gld
#### Automation pipeline to onboard the Global Labor Database to the Corporate Data Lake and publish its datasets to the Microdata Library
#### 
The pipeline is organized in five steps, and is supported by several support folders/files/tables:
* The **`_ingestion_metadata table`** is a table that keeps track of which tables have been ingested and published in the Microdata Library, as well as of some dataset-specific metadata. It also stores the path to the dta files and do files that have been associated with that specific ingestion
* The /Volumes/prd_csc_mega/sgld48/vgld48/Workspace/**survey_metadata.csv** file keeps track of all country survey specific metadata. 
> When a new survey is onboarded to the GLD catalog, the survey should be added in this csv file. 
>>(Location of the csv file might be changed to Document folder)
* The /Volumes/prd_csc_mega/sgld48/vgld48/Workspace/**countries.csv** file is a lookup file to match country codes to names. 
> This file should already contain all world countries, but if any is missing, they can simply be appended.
* The folder /Volumes/prd_csc_mega/sgld48/vgld48/Workspace/**jsons_temp**/ contains all json files awaiting publications. 
> These files are created in Script 3 and deleted once publication is successfully completed in Script 4. 


## Steps
**Script 0 (0-gld-identify-delta.R):** identifies new tables that need to be ingested. To do so, it crawls the entire Sharepoint library to identify "Data/Harmonized" folders and then filters for the highest Master Data Version (M) and Table Version (A).The new identified tables are added to the __ingestion_metadata_ table. If an older version is identified, the table for that older version is removed (this behavior might change).
This script features the creation of the following __ingestion_metadata_ variables: `filename`, `dta_path`, `country`, `year`, `quarter`, `survey`, `M_version`, `A_version`. These are  metadata fields that can be inferred from the filename.

**Script 1 (1-gld-ingest-full.py):** ingests all new tables to databricks, either in their entirety or in 5000-line chunks, if the file exceeds 900MB. At the end of the ingestion process, the following variables are updated in __ingestion_metadata_ - `harmonization` (GLD or GLD Light) and `household_level` (TRUE if the dataset contains the hhid variable), and `ingested` (TRUE if the ingestion process is successful).
> Please note that Databricks tables cannot include the character "-" in their name. Therefore, "-" is replaced with "_" and the value correspondence is stored in the `table_name` column of __ingestion_metadata_

**Script 2 (2-gls-metadata-parse.py):** this script identifies the .do file that corresponds to the .dta file ingested, 
Both `do_path` and `version_label` are stored in the __ingestion_metadata_ table

