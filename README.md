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

## 1. Introduction

## 2. Technologies Utilised

### 2.1 Python Libraries

- `airflow`: Used to create the [__DAG__](https://github.com/Zymeh/Pinterest-Data-Pipeline/blob/master/%3CIAM_username%3E_dag.py) (_Directed Acyclic Graph_) for __Amazon MWAA__.

- `datetime`: Used when setting options for the [__DAG__](https://github.com/Zymeh/Pinterest-Data-Pipeline/blob/master/%3CIAM_username%3E_dag.py).

- __`json`__: Used to serialise Python dictionaries into a __JSON-formatted string__, to be sent to the __RESTful API__.

- __`pyspark`__: Used in __Databricks notebooks__, leveraging the native support for Spark.

- __`random`__: Used to introduce a random delay (between 0 and 2 seconds in my case) before fetching data from the database (for simulation purposes). I also used `random.seed()` to set the seed for the random number generator.

- __`requests`__: Used to send HTTP requests. In my case, I used it to send data to an __AWS API Gateway endpoint__.

- __`sqlalchemy`__: Used to create an engine which connected to the __Source Data__.

- __`time`__: used to introduce delays for simulation purposes.

### 2.2 AWS Services

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

In this project, I made two different data pipelines. One for __Batch Processing__, which used an ELT (_Extract, Load and Transform_) pipeline, and another for __Streaming__ which used an ETL (_Extract, Transform and Load_) pipeline. In the below subsections I will talk about their architechture, with diagrams provided.

### 3.1 Batch Processing Architechture

![Batch_Processing_Architecture_diagram](https://github.com/Zymeh/Pinterest-Data-Pipeline/blob/master/Diagrams/Batch_Processing_Architecture.png?raw=true)

### 3.2 Streaming Architechture

![Diagrams/Streaming_Architecture](https://github.com/Zymeh/Pinterest-Data-Pipeline/blob/master/Diagrams/Streaming_Architecture.png?raw=true)

## 4. The Data Used

![Pinterest_Data](https://github.com/Zymeh/Pinterest-Data-Pipeline/blob/master/Diagrams/Pinterest_Data.png?raw=true)