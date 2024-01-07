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

4. [The Data](#4-the-data)

    4.1 [`pin_table`](#41-pin_table)

    4.2 [`geo_table`](#42-geo_table)

    4.3 [`user_table`](#43-user_table)

    4.4 [The Database Diagram](#44-the-database-diagram)

    4.5 [Querying the Data](#45-querying-the-data)

5. [File Structure](#5-file-structure)

## 1. Introduction

In this project designated by [AiCore](https://www.theaicore.com/), I've deployed two robust data pipelines, each serving a distinct purpose: a __Batch Processing Pipeline__ and a __Stream Processing Pipeline__. These pipelines are precisely tuned to seamlessly integrate the collection of data, paving the way for efficient and tailored data transformations.

Throughout this project, I gained valuable expertise in navigating the intricacies of __AWS__, delving into key components like __API proxy integrations__, __Amazon Kinesis__, and __Amazon MWAA__ (just to name a few!). The journey included a deep dive into the nuances of the data pipeline by building Pipeline Architecture diagrams, reinforcing my understanding of end-to-end pipeline processes. The experience has been instrumental in honing my skills with integral components of a data pipeline, specifically __Apache Kafka__, __Apache Spark__, and __Databricks__. These technologies now stand as pillars in my Data Engineering toolkit, contributing to a comprehensive understanding of building efficient and scalable data pipelines.

Before you begin, I implore you to take a look at my journal of making.  As a significant portion of the project operates in the cloud, this journal (available at [`journal.md`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/journal.md)) serves as a comprehensive account of everything that transpired during the development process, providing a detailed narrative that adds context and depth to the entire project journey.

## 2. Technologies Utilised

### 2.1 Python Libraries

- __`airflow`__: Used to create the __DAG__ (_Directed Acyclic Graph_) for __Amazon MWAA__.

- __`datetime`__: Used when setting options for the __DAG__.

- __`json`__: Used to serialise Python dictionaries into a __JSON-formatted string__, to be sent to the __RESTful API__.

- __`pyspark.sql.functions`__ : Used in __Databricks notebooks__, for manipulating and transforming data in __Spark DataFrames__.

- __`pyspark.sql.types`__ : Used in __Databricks notebooks__, for defining the structure of data in __Spark DataFrames__.

- __`random`__: Used to introduce a random delay (between 0 and 2 seconds in my case) before fetching data from the database (for simulation purposes). I also used `random.seed()` to set the seed for the random number generator.

- __`requests`__: Used to send HTTP requests. In my case, I used it to send data to an __AWS API Gateway endpoint__.

- __`sqlalchemy`__: Used to create an engine which connected to the __Source Data__.

- __`time`__: Used to introduce delays for simulation purposes.

- __`urllib`__: Used to encode the `SECRET_KEY` when mounting __S3 buckets__ to __Databricks__, and reading __Kinesis Data Streams__.  

### 2.2 AWS Services

- __Amazon RDS__: Used to host the source data.

- __API Gateway__: Used to create a __REST API__ with __Kafka REST proxy integration__ and __Kinesis REST proxy integration__.

- __EC2 (Elastic Compute Cloud)__: Used for deploying Kafka, Kafka Connect, creating Kafka topics, and initiating the API service.

- __IAM (Identity and Access Management)__: Used to manage secure access and permissions for AWS resources.

- __Kinesis__: Used for real-time stream processing.

- __MSK (Managed Streaming for Apache Kafka)__: Used to integrate __Amazon S3__ with __Kafka Connect connectors__ for storage of __Kafka topics__.

- __MWAA (Managed Workflows for Apache Airflow)__: Used to orchestrate and manage __Apache Airflow workflows__.

- __S3 (Simple Storage Service)__: Used to store the __Kafka Topic__ data, serving as a __Data Lake__. I also stored a __DAG__ here, which was used by __MWAA__

### 2.3 Databricks

- __Delta Tables__: Used to store processed tables, they provide a reliable and scalable storage solution with support for __ACID transactions__.

- __Notebooks__: Used to read data from __S3 buckets__ and __Kinesis streams__, as well as performing data transformations.

### 2.4 Diagram Creators

- __dbdocs__: Used for creating the database diagram.

- __draw.io__: Used for creating the pipeline architecture diagrams.

## 3. Pipline Architecture

In this project, I made two different data pipelines. One for __Batch Processing__, which used an ELT (_Extract, Load and Transform_) pipeline, and another for __Streaming__ which used an ETL (_Extract, Transform and Load_) pipeline. In the below subsections I will talk about their specific architechture, with diagrams provided.

### 3.1 Batch Processing Architechture

----------

__Data Extraction__

The source data was extracted from an __Amazon RDS__ database. The extraction process involved three tables: `pin data`, `geo data`, and `user data`. The extraction script, [`user_posting_emulation.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Posting_emulation_scripts/user_posting_emulation.py), establishes a connection to the __Amazon RDS__ database using specified credentials. For security purposes, these credentials are stored in a separate file, which were loaded into the script during execution.

The script employs logic to extract one row at a time from each table, ensuring that the rows correspond to each other. This is key to emulate real-time Pinterest posting by users.

For further information, please consult the docstrings in the script [`user_posting_emulation.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Posting_emulation_scripts/user_posting_emulation.py).

----------

__Data Loading__

After extracting rows from the __Amazon RDS__ database using [`user_posting_emulation.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Posting_emulation_scripts/user_posting_emulation.py), each extracted row is sent to a __REST API__ with __Kafka REST proxy integration__. It should be noted that the __client EC2 Machine__ was used to start the API. In the script, I made sure to handle errors accordingly, where the script would terminate in the event of any __status code__ which was not __`200`__. Below, I will break down each step when the data had been sent to the __API__

- __REST API Integration with Kafka__

  - The REST API receives the posts sent by the extraction script.

  - It communictes with a __Kafka producer__ via __MSK Connect__, using the established __Kafka workers__ in a plugin I set up.

- __Kafka Event Streaming__

  - The __Kafka producer__ writes events to a __Kafka topic__ for each post.

- __Storage in S3 Buckets__

  - The __Kafka topics__ are configured to store __events__ in three distinct __S3 buckets__.

  - Each __S3 bucket__ is named to correspond with the data type it contains.

- __Mounting the S3 Buckets in Databricks__

  - Before any __transformations__ were made, I had to mount the __S3 buckets__ in __databricks__, which will make the data in the S3 buckets accessible within the __Databricks workspace__. The code for this can be seen in [`mount_s3_buckets.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/mount_s3_buckets.ipynb).

----------

__Data Transformation__

After the __S3 buckets__ had been mounted, data transformations (cleaning and querying) were applied using the __notebook__ [`batch_processing.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/batch_processing.ipynb) in __Databricks__. The seamless integration of __Spark__ with __Databricks__ played a key role in ensuring the efficiency of the transformation processes.

For more information on __data cleaning__, see [here](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/journal.md#61-data-cleaning).

For information on what __queries__ I ran, see [here](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/journal.md#62-sql-queries).

----------

__Orchestration__

A __DAG__, defined in [`<IAM_username>_dag.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/%3CIAM_username%3E_dag.py), had been successfully deployed on __Managed Workflows for Apache Airflow (MWAA)__ for automated batch processing. __MWAA__ utilises __Apache Airflow__ which makes it easy to schedule and monitor workflows. The __DAG__ is set to trigger daily, making it so that data transformation takes place at a consistent time each day to integrate the addition of new batch data.

----------

__Architectural Diagram__

The diagram below is a visual representation of the main architectural components within the batch processing framework.

![Batch_Processing_Architecture_diagram](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Diagrams/Batch_Processing_Architectural_Diagram.png?raw=true)

### 3.2 Streaming Architechture

----------

__Data Extraction__

Similar to before, the source data was extracted from an __Amazon RDS__ database. The extraction process involved three tables: `pin data`, `geo data`, and `user data`. The extraction script, [`user_posting_emulation_streaming.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Posting_emulation_scripts/user_posting_emulation_streaming.py), establishes a connection to the __Amazon RDS__ database using specified credentials. Again, for security purposes, these credentials are stored in a separate file, which were loaded into the script during execution.

The script employs logic to extract one row at a time from each table, ensuring that the rows correspond to each other. This is key to emulate real-time Pinterest posting by users.

For further information, please consult the docstrings in the script [`user_posting_emulation_streaming.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Posting_emulation_scripts/user_posting_emulation_streaming.py).

----------

__Data Transformation__

Upon sending the extracted data to the __API__ , the data was picked up by __Amazon Kinesis__ thanks to the __API Gateway__ which had __Kinesis proxy integration__ configured. Then within __Kinesis__ service, the data was organised into streams, that I configured [here](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/journal.md#81-create-data-streams-using-kinesis-data-streams).

Next I read the data from the __Kinesis streams__ to __Databricks__, and then cleaned the data in __Databricks__ using the native __Spark__ functions. This process can be seen in the __notebook__ [`stream_processing.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/stream_processing.ipynb). 

----------

__Data Loading__

After cleaning the data in Databricks, the __Spark Dataframes__ were loaded to __Delta Tables__ (using a function I made in [`stream_processing.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/stream_processing.ipynb)). If needed, further analysis can be done. For example __ACID transactions__.

----------

__Architectural Diagram__

The diagram below is a visual representation of the main architectural components within the stream processing framework.

![Diagrams/Streaming_Architecture](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Diagrams/Stream_Processing_Architectural_Diagram.png?raw=true)

## 4. The Data

After the data has processed in each of the pipelines, we end up with three tables, namely `pin_table`, `geo_table` and `user_table`. Let's break them down:

### 4.1 `pin_table`

This table contains information about the Pinterest post data.

| Data | Description |
| ---- | ----------- |
| `ind` | The index of that row |
| `unique_id` | A UUID associated with the Pinterest post |
| `title` | The title of the Pinterest post |
| `description` | The discription of the Pinterest post |
| `follower_count` | The number of followers the Pinterest poster has |
| `poster_name` | The name of the Pinterest poster |
| `tag_list` | The list of user defined tags for the Pinterest post |
| `is_image_or_video` | The type of media file the Pinterest post is |
| `image_src` | The Pinterest Post thumbnail |
| `save_location` | The file path of the Pinterest post |
| `category` | The category of the Pinterest post |

### 4.2 `geo_table`

This table contains information about the Geolocation data connected to the Pinterest post.

| Data | Description |
| ---- | ----------- |
| `ind` | The index of that row |
| `country` | The country the post was made |
| `timestamp` | The date and time for when the post was made |
| `coordinates` | The geographical coordinates for the location the post was made |

### 4.3 `user_table`

This table contains information about the User data connected to the Pinterest post.

| Data | Description |
| ---- | ----------- |
| `ind` | The index of that row |
| `user_name` | The users first and last name |
| `age` | The age of the user |
| `date_joined` | The date and time for when the user made their account |

### 4.4 The Database Diagram

Below is the database diagram which shows how the data is related to each other. This will be useful for querying the data.

![Pinterest_Data](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Diagrams/Pinterest_Data.png?raw=true)

### 4.5 Querying the Data

Below are some questions one could run queries on:

- Find the most popular category in each country.
- Find how many posts each category had between 2018 and 2022.
- Find the user with the most followers in each country.
- Find which was the most popular for different age groups.
- Find the median follower count for different age groups.
- Find how many users have joined between 2015 and 2020.
- Find the median follower count of users have joined between 2015 and 2020.
- Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.

## 5. File Structure

Below is the file structure for the GitHub repo. I have included as many files as possible. But as I mentioned before, since most of the project is done on the cloud, please consult [`journal.md`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/journal.md) for further discussions to what I did in the project.

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
│   ├── user_posting_emulation.py # redundant
│   └── user_posting_emulation_streaming.py # redundant
│   └── user_posting_emulation_batch_and_streaming.py # combines the two emulation scripts into one, with improved functionality
├── <IAM_username>_dag.py # The DAG used on MWAA
├── README.md
├── journal.md

```