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

- __`json`__: Used to serialise Python dictionaries into a __JSON-formatted string__, to be sent to the __RESTful API__.

- __`random`__: Used to introduce a random delay (between 0 and 2 seconds in my case) before fetching data from the database (for simulation purposes). I also used `random.seed()` to set the seed for the random number generator.

- __`requests`__: Used to send HTTP requests. In my case, I used it to send data to an __AWS API Gateway endpoint__.

- __`sqlalchemy`__: Used to create an engine which connected to the __Source Data__.

- __`time`__: used to introduce delays for simulation purposes.

### 2.2 AWS Services

- __API Gateway__: Used to create a __REST API__ with __Kafka REST proxy integration__ and __Kinesis REST proxy integration__.

- __EC2 (Elastic Compute Cloud)__:

- __IAM (Identity and Access Management)__:

- __Kinesis__:

- __MSK (Managed Streaming for Apache Kafka)__:

- __MWAA (Managed Workflows for Apache Airflow)__:

- __S3 (Simple Storage Service)__:

### 2.3 Databricks

### 2.4 Diagram Creators

## 3. Pipline Architecture

### 3.1 Batch Processing Architechture

### 3.2 Streaming Architechture

## 4. The Data Used