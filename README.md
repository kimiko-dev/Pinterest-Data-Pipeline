# Pinterest Data Pipeline

## _Table of Contents_

1. [Introduction](#1-introduction)

2. [Technologies Utilised](#2-technologies-utilised)

    2.1 [Python Libraries](#21-python-libraries)

    2.2 [AWS Services](#22-aws-services)

    2.3 [Databricks](#23-databricks)

    2.4 [Diagram Creators](#24-diagram-creators)

3. [Pipline Architecture](#3-pipline-architecture)

    3.1 [Batch Processing Architechture](#31-batch-processing-architechture)

    3.2 [Streaming Architechture](#32-streaming-architechture)

4. [The Data Used](#4-the-data-used)

5. [File Structure](#5-file-structure)

## 1. Introduction

## 2. Technologies Utilised

### 2.1 Python Libraries

- __`airflow`__: Used to create the __DAG__ (_Directed Acyclic Graph_) for __Amazon MWAA__.

- __`datetime`__: Used when setting options for the __DAG__.

- __`json`__: Used to serialise Python dictionaries into a __JSON-formatted string__, to be sent to the __RESTful API__.

- __`pyspark`__: Used in __Databricks notebooks__, leveraging the native support for Spark.

- __`random`__: Used to introduce a random delay (between 0 and 2 seconds in my case) before fetching data from the database (for simulation purposes). I also used `random.seed()` to set the seed for the random number generator.

- __`requests`__: Used to send HTTP requests. In my case, I used it to send data to an __AWS API Gateway endpoint__.

- __`sqlalchemy`__: Used to create an engine which connected to the __Source Data__.

- __`time`__: Used to introduce delays for simulation purposes.

- __`urllib`__: Used to encode the `SECRET_KEY` when mounting __S3 buckets__ to __Databricks__.  

### 2.2 AWS Services

- __Amazon RDS__: Used to host the source data.

- __API Gateway__: Used to create a __REST API__ with __Kafka REST proxy integration__ and __Kinesis REST proxy integration__.

- __EC2 (Elastic Compute Cloud)__: Used for deploying Kafka, Kafka Connect, creating Kafka topics, and initiating the API service.

- __IAM (Identity and Access Management)__: Used to manage secure access and permissions for AWS resources.

- __Kinesis__: Used for real-time stream processing.

- __MSK (Managed Streaming for Apache Kafka)__: Used to integrate __Amazon S3__ with __Kafka Connect connectors__ for storage of __Kafka topics__.

- __MWAA (Managed Workflows for Apache Airflow)__: Used to orchestrate and manage __Apache Airflow workflows__.

- __S3 (Simple Storage Service)__: Used to store the __Kafka Topic__ data.

### 2.3 Databricks

- __Delta Tables__: Used to store the transformed data in __Databricks__.

- __Notebooks__: Used to connect to S3 buckets and perform data transformations.

### 2.4 Diagram Creators

- __dbdocs__: Used for creating the database diagram.

- __draw.io__: Used for creating the pipeline architecture diagrams.

## 3. Pipline Architecture

In this project, I made two different data pipelines. One for __Batch Processing__, which used an ELT (_Extract, Load and Transform_) pipeline, and another for __Streaming__ which used an ETL (_Extract, Transform and Load_) pipeline. In the below subsections I will talk about their specific architechture, with diagrams provided.

### 3.1 Batch Processing Architechture

----------

__Data Extraction__

The source data was extracted from an __Amazon RDS__ database. The extraction process involved three tables: `pin data`, `geo data`, and `user data`. The extraction script, [`user_posting_emulation.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Posting_emulation_scripts/user_posting_emulation.py), establishes a connection to the Amazon RDS database using specified credentials, host, and port. For security purposes, these credentials are stored in a separate file, which were loaded into the script during execution.

The script employs logic to extract one row at a time from each table, ensuring that the rows correspond to each other. This is key to emulate real-time Pinterest posting by users.

----------

__Data Loading__

After extracting rows from the __Amazon RDS__ database using [`user_posting_emulation.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Posting_emulation_scripts/user_posting_emulation.py), each extracted row is sent to a __REST API__ with __Kafka REST proxy integration__. It should be noted that the __client EC2 Machine__ was used to start the API. In the script, I made sure to handle errors accordingly, where the script would terminate in the event of any __status code__ which was not __`200`__.

- __REST API Integration with Kafka__

  - The REST API takes the posts sent by the extraction script.

  - It interfaces with a __Kafka producer__ using __MSK Connect__, using the established Kafka workers in a plugin.

- __Kafka Event Streaming__

  - The __Kafka producer__ writes events to a __Kafka topic__ for each post.

- __Storage in S3 Buckets__

  - The __Kafka topics__ are configured to store __events__ in three separate __S3 buckets__.

  - Each __S3 bucket__ is named in correspondence to the type of data.

- __Mounting the S3 Buckets in Databricks__

  - Before any __transformations__ were made, I had to mount the __S3 buckets__ in __databricks__. The code for this can be seen in [`mount_s3_buckets.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/mount_s3_buckets.ipynb).

----------

__Data Transformation__

After the __S3 buckets__ had been mounted, data transformations (cleaning and querying) were applied using the __notebook__ [batch_processing.ipynb](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/batch_processing.ipynb) in __databricks__. The seamless integration of __Spark__ with __Databricks__ played a key role in ensuring the efficiency of the transformation processes.

For more information on __data cleaning__, see [here](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/journal.md#61-data-cleaning).

For information on what __queries__ I ran, see [here](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/journal.md#62-sql-queries).

----------

__Orchestration__

A __DAG__, defined in [`<IAM_username>_dag.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/%3CIAM_username%3E_dag.py), had been successfully deployed on __Managed Workflows for Apache Airflow (MWAA)__ for automated batch processing. __MWAA__ utilises __Apache Airflow__ which makes it easy to schedule and monitor workflows. The __DAG__ is set to trigger daily, making it so that data transformation takes place at a consistent time each day to integrate the addition of new batch data.

----------

__Architectural Diagram__

The diagram below serves as a visual representation of the main architectural components within the batch processing framework.

![Batch_Processing_Architecture_diagram](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Diagrams/Batch_Processing_Architecture.png?raw=true)

### 3.2 Streaming Architechture

----------

__Data Extraction__



----------

__Data Transformation__



----------

__Data Loading__



----------

__Architectural Diagram__

![Diagrams/Streaming_Architecture](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Diagrams/Streaming_Architecture.png?raw=true)

## 4. The Data Used

![Pinterest_Data](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Diagrams/Pinterest_Data.png?raw=true)

## 5. File Structure

Below is the file structure for the GitHub repo. I have included as many files as possible. But as I mentioned before since most of the project is done on the cloud, please consult [`journal.md`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/journal.md) for further discussions to what I did in the project.

```
├── Databricks_notebooks
│   ├── batch_processing.ipynb
│   ├── mount_s3_buckets.ipynb
│   └── stream_processing.ipynb
├── Diagrams
│   ├── Batch_Processing_Architecture.png 
│   ├── Pinterest_Data.png
│   └── Streaming_Architecture.png
├── Posting_emulation_scripts
│   ├── user_posting_emulation.py # Emulates posting data for batch processing
│   └── user_posting_emulation_streaming.py # Emulates posting data for streaming
├── <IAM_username>_dag.py # The DAG used on MWAA
├── README.md
├── journal.md

```