# Pinterest Data Pipeline Journal
---------------------------------

## _Table of Contents_

1. [Introduction](#1-introduction)

2. [Configuring the EC2 Kafka Client](#2-configuring-the-ec2-kafka-client)

    2.1 [Create a `.pem` Key File Locally](#21-create-a-pem-key-file-locally)

    2.2 [Connect to the EC2 Instance](#22-connect-to-the-ec2-instance)

    2.3 [Set Up Kafka on the EC2 Client](#23-set-up-kafka-on-the-ec2-client)

    2.4 [Create Kafka Topics](#24-create-kafka-topics)

3. [Connect a MSK Cluster to a S3 Bucket](#3-connect-a-msk-cluster-to-a-s3-bucket)

    3.1 [Create a Custom Plugin with MSK Connect](#31-create-a-custom-plugin-with-msk-connect)

    3.2 [Create a Connector with MSK](#32-create-a-connector-with-msk)
    
4. [Configuring an API in API Gateway](#4-configuring-an-api-in-api-gateway)

    4.1 [Building a Kafka REST Proxy Integration Method for the API](#41-building-a-kafka-rest-proxy-integration-method-for-the-api)

    4.2 [Setting Up the Kafka REST Proxy on the EC2 Client](#42-setting-up-the-kafka-rest-proxy-on-the-ec2-client)

    4.3 [Send Data to the API](#43-send-data-to-the-api)

5. [Setting up Databricks](#5-setting-up-databricks)

    5.1 [Mounting a S3 Bucket to Databricks](#51-mounting-a-s3-bucket-to-databricks)
    
6. [Data Cleaning and SQL Queries Using Spark on Databricks](#6-data-cleaning-and-sql-queries-using-spark-on-databricks)

    6.1 [Data Cleaning](#61-data-cleaning)

    - 6.1.1 [Cleaning the Pinterest Post Data](#611-cleaning-the-pinterest-post-data)

    - 6.1.2 [Cleaning the Geolocation Data](#612-cleaning-the-geolocation-data)

    - 6.1.3 [Cleaning the User Data](#613-cleaning-the-user-data)

    6.2 [SQL Queries](#62-sql-queries)

7. [AWS MWAA](#7-aws-mwaa)

    7.1 [Create and Upload a DAG to a MWAA Environment](#71-create-and-upload-a-dag-to-a-mwaa-environment)

    7.2 [Trigger a DAG that Runs a Databricks Notebook](#72-trigger-a-dag-that-runs-a-databricks-notebook)

8. [AWS Kinesis](#8-aws-kinesis)

    8.1 [Create Data Streams Using Kinesis Data Streams](#81-create-data-streams-using-kinesis-data-streams)

    8.2 [Configure an API with Kinesis Proxy Integration](#82-configure-an-api-with-kinesis-proxy-integration)

    - 8.2.1 [List Streams](#821-list-streams)

    - 8.2.2 [Create Streams](#822-create-streams)

    - 8.2.3 [Describe Streams](#823-describe-streams)

    - 8.2.4 [Delete Streams](#824-delete-streams)

    - 8.2.5 [Add Records to Streams](#825-add-records-to-streams)

    8.3 [Send Data to the Kinesis Stream](#83-send-data-to-the-kinesis-stream)

    8.4 [Read Data from the Kinesis Streams in Databricks](#84-read-data-from-the-kinesis-streams-in-databricks)

    8.5 [Transform Kinesis streams in Databricks](#85-transform-kinesis-streams-in-databricks)

    8.6 [Write the Streaming Data to Delta Tables](#86-write-the-streaming-data-to-delta-tables)


## 1. Introduction
------------------

Welcome to my journal of making! 

__*Why This Journal?*__

I concieved the idea to make this since most of the work I have done is on the cloud, namely __AWS__ and __Databricks__, and I would like to showcase all the steps and skills I have gained upon deploying two functional data pipelines.

__*What to Expect?*__

In the below sections, you will find a brief overview of the tasks completed and why the step is necessary. In the corresponding subsections, you will find detailed descriptions of what I did at each step.

Now, without further ado, lets dive in!

<sub>(P.S. I hope you enjoy your stay, it will be a long one. I recommend equipping some snacks and beverages of your choice!)</sub>

## 2. Configuring the EC2 Kafka Client
--------------------------------------

<ins>__Overview__</ins> :

I connected to a __client EC2 machine__, configured __Kafka__ and created __Kafka topics__.

<ins>__Fundamental Importance__</ins> :

Simply put, the __client EC2 machine__ is what hosts __Kafka__. The data sent to the __API__ will be rerouted to this instance (through __Kafka proxy integration__, but more on this later) and picked up by __Kafka Producers__, who then writes to a __Kafka topic__ for each bit of data sent. Using __Kafka__ here is important since we can send the data to storage as soon as it gets picked up by the __API__. We will discuss how the data is sent to storage later on.

### 2.1 Create a `.pem` Key File Locally
----------------------------------------

Upon signing into my AWS account, I navigated to the __Parameter Store__. I searched the parameters using my `SSH Keypair ID`. Under __Parameter details__, I found the and decrypted the `Value`, revealing an _RSA private key_. I copied and pasted this key into a new `.pem` file, making sure this was created in the correct folder I wanted to connect to the __EC2 instance__ in. However, we need to name this file as `<Key pair assigned at launch>.pem`. To identify the `Key pair assigned at launch`, I went to the __Instances__ section located in the __EC2 Service__. I found the correct instance by referencing my __AWS IAM Username__, and then accessed the __instance summary__. In the __details__ section, I found `Key pair assigned at launch`, which I copy and pasted as the filename.

### 2.2 Connect to the EC2 Instance
-----------------------------------

I needed to connect to the __EC2 instance__ on an __SSH client__. I am using __WSL__, and I am using the `OpenSSH` client. To install the `OpenSSH` client, I simply wrote the command `sudo apt-get install openssh-client` in a __WSL__ terminal. After confirming I was in the same folder where the `.pem` file is stored, I ran the command `chmod 400 <Key pair assigned at launch>.pem` to make sure the file had been set to the correct permissions. Then I ran `ssh -i "<Key pair assigned at launch>.pem" root@<Public IPv4 DNS>`, which connected me to the __EC2 instance__. 

### 2.3 Set Up Kafka on the EC2 Client
--------------------------------------

I need to download and install __Kafka__ on my __client EC2 machine__. I have already been proivded with access to an __IAM authenticated MSK cluster__, so I did not have to do this in the project. On the __EC2 client__, I first had to install __Java__, with following command `sudo yum install java-1.8.0`. Since the cluster I was using was running on __Kafka 2.12-2.8.1__, I had to make sure that I dowloaded the correct version of __Kafka__ on my __client EC2 machine__. The command for this is: `wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz`. With the file downloaded, I then used `tar -xzf kafka_2.12-2.8.1.tgz` to extract all the files needed for __Kafka__. 

Following this, I needed to download the __IAM MSK authentication package__ on my __client EC2 machine__. First I navigated to the `libs` folder inside the __Kafka 2.12-2.8.1__ directory (the filepath is `/home/ec2-user/kafka_2.12-2.8.1/libs`). Then, I used the command `wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar`. Before I continued, I navigated to the __Roles__ section in the __IAM console__. It gave me a list of roles, where I had to find the role which was named `<your_UserId>-ec2-access-role`. In this role, I made note of the __ARN__ (_Amazon Resource Name_) in the __Summary__ section. Below the __Summary__ section, I selected the __Trust relationships__ tab, and then hit __Edit trust policy__. Here, I clicked on the __Add a principal__ button and selected __IAM roles__ as the __Principal type__. I had to then paste my __ARN__ in the box below. Upon doing this, I hit the __Add principal__ button, which successfully gave me the __IAM role__, as this contains the necessary permissions to authenticate to the __MSK cluster__.

Now, I needed to configure my Kafka client to use AWS IAM authentication on the cluster. To do this, I headed to `/home/ec2-user/kafka_2.12-2.8.1/bin` and edited the `client.properties` file. In here, I added the following:

```
# Sets up TLS for encryption and SASL for authN.
security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="<my_ARN>";

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```
where I replaced `<my_ARN>` with the __ARN__ I noted down before.

Finally, I had to set up the `CLASSPATH` in the `.bashrc` file. This file is located at `/home/ec2-user/.bashrc`. When editing, I added the line: `export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar`. Also, while I was here, I made sure to add in `export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.382.b05-1.amzn2.0.2.x86_64/jre` too. To make sure the environment variables were set correctly, I wrote the following commands `echo $CLASSPATH` and `echo $JAVA_HOME`, both returing the correct filepaths.

### 2.4 Create Kafka Topics
---------------------------

Before creating the __Kafka topics__, I needed to note down the `Bootstrap servers string` and the `Plaintext Apache Zookeeper connection string`. To retreive these, I headed to the __MSK Management Console__, clicked on the appropriate __cluster__, and clicked on __View client information__. The `Bootstrap servers string` was in the __Bootstrap servers__ box, under the __Private endpoint (single-VPC)__ column. Scrolling down, I found the `Plaintext Apache Zookeeper connection string` under __Apache ZooKeeper connection__, where I copied the _Plaintext_ string. 

Now we can create the three following topics:
- `<your_UserId>.pin` for the Pinterest posts data
- `<your_UserId>.geo` for the post geolocation data
- `<your_UserId>.user` for the post user data

To do this (in the __EC2 client__) I navigated to `/home/ec2-user/kafka_2.12-2.8.1/bin`, where I executed the following command `./kafka-console-producer.sh --bootstrap-server <BootstrapServerString> --producer.config client.properties --group students --topic <topic_name>`. I made sure to replace the `BootstrapServerString` with the one I obtained previously, and also replaced `<topic_name>` appropriately (we need to make 3 topics, so I used this command three times each with the desired topic names.)

## 3. Connect a MSK Cluster to a S3 Bucket
------------------------------------------

<ins>__Overview__</ins> :

Created a __custom MSK plugin__ using __Confluent.io Amazon S3 Connector__, then established it as a __connector__ in __MSK Connect__ using the correct __configuration settings__. 

<ins>__Fundamental Importance__</ins> :

The __plugin__ makes use of the __Amazon S3 Sink connector__, which exports the data from __Kafka topics__ to __S3 buckets__ in __JSON__ formats (in my case at least, as it supports other formats such as __Avro__ and __Bytes__).

### 3.1 Create a Custom Plugin with MSK Connect
-------------------------------------------------

Firstly, I need to go to the __S3 console__ and find the correct bucket. It is named `user-<my_UserId>-bucket`, and I made a note of the name.

Now, on my __EC2 client__, I downloaded the __Confluent.io Amazon S3 Connector__. To do this, I simply used to command `sudo wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip`. Next, I need to move it to the correct __S3 bucket__. This was easy to do, I simply entered the command `aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<bucket_name>/kafka-connect-s3/`, where I replaced `<bucket_name>` with the one I obtained earlier.

I then navigated to the __Amazon MSK__ console, and clicked on __Create customised plugin__ under the __MSK Connect__ section. Here, I entered the __S3 URI__ `s3://<bucket_name>/kafka-connect-s3/confluentinc-kafka-connect-s3-10.0.3.zip`, again changing `<bucket_name>` to the correct one. I then had to name it `<your_UserId>-plugin`, since my account only had permissions to create a plugin called this.

### 3.2 Create a Connector with MSK
-----------------------------------

Going back to the __Amazon MSK__ console, I clicked on __Connectors__, and then pressed __Create connector__ button. Firstly, I selected the `Use existing customised plugin` option, looked for the __Customised plugin__ I had just created, and then selected it. After hitting next, I was in the __Connector properties__ section. Here (following a similar convention as before) I entered the __Connector name__ as `<your_UserId>-connector`. For the __Apache Kafka cluster__, I chose the __MSK cluster__ option, and selected the appropriate cluster. Now, I had to set up the __Connector configuration__, under the __Configuration settings__ I simply put:
```
connector.class=io.confluent.connect.s3.S3SinkConnector
# same region as our bucket and cluster
s3.region=us-east-1
flush.size=1
schema.compatibility=NONE
tasks.max=3
# include nomeclature of topic name, given here as an example will read all data from topic names starting with msk.topic....
topics.regex=<UserID>.*
format.class=io.confluent.connect.s3.format.json.JsonFormat
partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
value.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
storage.class=io.confluent.connect.s3.storage.S3Storage
key.converter=org.apache.kafka.connect.storage.StringConverter
s3.bucket.name=<my_bucket_name>
```

I left the rest of the options as default, apart from the __Access Permissions__ at the bottom of the page. Here, I found my access role (again by searching for `<UserID>-ec2-access-role`) and chose it. 

I then proceeded to create the __Cluster connector__ after going through all the remaining pages and leaving them as default. I received a message saying my __Connector__ was successfully created.

## 4. Configuring an API in API Gateway
---------------------------------------

__NOTE__: For this project, I did not need to create my own __API__ since one had already been made for me. However, I still needed to configure it. 

<ins>__Overview__</ins> :

In this section, I built a __Kafka REST proxy integration__ method, set up the __Kafka REST proxy__ on the __client EC2 machine__, and sent data to the __API__.

<ins>__Fundamental Importance__</ins> :

__Kafka REST proxy integration__ enables __Kafka producers__ on the __client EC2 machine__ to capture the data sent to the __API__ and processes it into __Kafka topics__ for storage.

### 4.1 Building a Kafka REST Proxy Integration Method for the API
------------------------------------------------------------------

First of all, I navigated to the __EC2 instances__, found my __client EC2 machine__ and selected it. In the __Details__ section, I found the __Public IPv4 DNS__ and made note of it for later use.

On the __API Gateway__, I searched for the __API__ which had been setup for me. Upon finding it, I clicked on it which took me to the __Resources__ page. Here, I clicked on the __Create resource__ button, which took me to a set up screen. Now, I made sure to turn on __Proxy resource__, I set the __Resource path__ as `/{proxy+}` and set the __Resource name__ as `{proxy+}`. Lastly, I selected the __CORS (Cross Origin Resource Sharing)__ box and then hit __Create resource__.

When clicking on the new resource I had just created, I pressed the __Create method__ button as I wanted to create a __HTTP ANY method__. For the __Method type__ I selected __ANY__ in the dropdown box. For __Integration type__, I selected __HTTP Proxy__. Finally, for the __Endpoint URL__, I wrote the following `http://<Public IPv4 DNS>:8082/{proxy}`. I then clicked __Create Method__, which successfully created the method. 

Now, I needed to get __Invoke URL__. To do this, I simply pressed the __Deploy API__ button, created a new __Stage__ called `dev` and then deployed the __API__. It took me to the __Stages__ page on the __API Gateway__ where it gave me the __Invoke URL__, and I noted this down. 

### 4.2 Setting Up the Kafka REST Proxy on the EC2 Client
---------------------------------------------------------

On the __client EC2 machine__, I had to install __Confluent package__ for the __Kafka REST Proxy__. To do this, I ran the following commands: `sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz` (which downloads the __Confluent package__) and then `tar -xvzf confluent-7.2.0.tar.gz ` (which extracts and decompresses the contents of `confluent-7.2.0.tar.gz`)

Next, on the __client EC2 machine__ I navigated to `/home/ec2-user/confluent-7.2.0/etc/kafka-rest` where I modified the `kafka-rest.properties` file, where I changed the `bootstrap.servers` and the `zookeeper.connect` variables in this file, with the corresponding **Boostrap server string** and **Plaintext Apache Zookeeper connection string** (these are the ones from [2.4](#24-create-kafka-topics)). On top of this, I had to surpass the __IAM authentecation__ of the __MSK cluster__, so similar to [before](#23-set-up-kafka-on-the-ec2-client) (and making use of the __IAM MSK authentication package__), I added the following to the `kafka-rest.properties` file:

```
# Sets up TLS for encryption and SASL for authN.
client.security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
client.sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
client.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="<my_ARN>";

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
client.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```
Again replacing `<my_ARN>` with the one I obtained from [2.3](#23-set-up-kafka-on-the-ec2-client).

To make sure everything was in working order, I started the __REST proxy__ on the __client EC2 machine__. To do this, I first navigated to `/home/ec2-user/confluent-7.2.0/bin` and then ran the command `./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties`. Everything ran correctly as I was given the message _INFO Server started, listening for requests..._ on the __client EC2 machine__.

### 4.3 Send Data to the API
----------------------------

Here, I edited the [`user_posting_emulation.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Posting_emulation_scripts/user_posting_emulation.py) file which was supplied to us. I added a function which took in the arguments: data obtained from a source, and the topic name of the topic I needed to send that data to.

## 5. Setting up Databricks
---------------------------

__NOTE__: Before I was able to clean the data, I needed to read the data from my __S3 bucket__ into __Databricks__. This can be done by mounting the desired __S3 bucket__ to __Databricks__.

<ins>__Overview__</ins> :

In this step I simply __mounted__ the __S3 buckets__ containing __Kafka topic__ data to __Databricks__.

<ins>__Fundamental Importance__</ins> :

__Databricks__ is the preffered __cloud platform__ for data __transformation__ the data since it utilises __Spark__ natively. As part of this step, we access the __Kafka topic__ data for __transformation tasks__.

### 5.1 Mounting a S3 Bucket to Databricks
------------------------------------------

My `authentication_credentials.csv` file (which contains the __AWS access key__ and __secret access key__) had already been uploaded to __Databricks__, so I can start mounting the __S3 bucket__ right away. 

To write the code for this, I simply created a new __Notebook__. First I needed import the correct libraries and read the `authentication_credentials.csv` file, the code looked like:

```
from pyspark.sql.functions import *
import urllib


# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")
```

Then, I extracted the __AWS access key__ and __secret access key__ from the __Spark DataFrame__ I created above. Additionally, I made sure to __encode__ the __secret access key__ for security reasons. The code for this is:

```
# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")
```

Now, I was able to mount the __S3 bucket__ by passing in the __S3 URL__ and the __desired mount name__ into the code below:

```
# AWS S3 bucket name
AWS_S3_BUCKET = "<S3_URL>"
# Mount name for the bucket
MOUNT_NAME = "/mnt/<desired_mount_name>"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)
```
Since I have three __Kafka Topics__ which correspond to three different paths in the __S3 bucket__, I made sure to have three copies of the code above using appropriate values for `<S3_URL>` and `<desired_mount_name>`.

I ran the whole code and it returned `True`, meaning it was successfully mounted.

The __notebook__ I used to __mount__ the __S3 buckets__ is [`mount_s3_buckets.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/mount_s3_buckets.ipynb).

## 6. Data Cleaning and SQL Queries Using Spark on Databricks
-------------------------------------------------------------

<ins>__Overview__</ins> :

In this section, I simply __cleaned__ and __queried__ the data using __Spark__ on __Databricks__.

<ins>__Fundamental Importance__</ins> :

Cleaning the data is necessary when querying the data effectively. For example, in the __`pin table`__, we had abbriviated follower counts (like `100k`). To implement __data aggregation__ through __queries__, it was necessary to convert these values into integers.

For details on all of the __transformations__, please consult the __notebook__ [`batch_processing.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/batch_processing.ipynb).

### 6.1 Data Cleaning
---------------------

First, I had to read the data from the __mounted S3 bucket__. I had to read this data three times since I had three __S3 URL__\s where the seperate data was stored. So I decided to make a function. The code inside the function was:

```
file_location = "/mnt/<mount_name>/*.json" 
file_type = "json"
infer_schema = "true"
df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
```

The file for

In the below subsections, I will explain how I cleaned each dataframe.

#### 6.1.1 Cleaning the Pinterest Post Data
-------------------------------------------

Here, there were rows of post data which were labled as `multi-video(story page format)` and a lot of its columns had 'empty' entries, such as:

| Column | Entry |
| :----: | :----: |
| `description` | `No description available Story format` |
| `follower_count` | `User Info Error` |
| `image_src` | `Image src error.` |
| `poster_name` | `User Info Error` |
| `tag_list` | `N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e` |
| `title` | `No Title Data Available` |

I made sure to scrub these entries clean, by replacing them with `None`.

Next, I had to turn the `follower_count` column into __Integer__ data type. This wasn't so straight forward as `follower_count`\'s over `100,000` either had a `k` or `M` suffix attached to the end. If it had a suffix of `'k'`, I replaced it with `'000'` (since it was still a string). For example, `'400k'` would turn into `'400000'`. Similarly, if it had a suffix of `'M'`, it would be replaced with `'000000'`. For example, `'2M'` would be transformed to `'2000000`. I then cast the `follower_count` column into the correct __Integer__ data type. 

I then renamed the `index` column to `ind` so that it was consistent across all three dataframes.

Following this, entires in the `save_location` column all had `Local save in` infront of the save location path. This information is redundant so I simply removed the `Local save in` from all entries. For example, `Local save in /data/home-decor` would be transformed to just `/data/home-decor`.

Lastly, I reordered the columns into an order which required.

#### 6.1.2 Cleaning the Geolocation Data
----------------------------------------

The first thing I did was create a new column called `coordinates`. In this column, I made an array using values from the `latitude` and `longitude` columns. The array looked like this `['latitude', 'longitude']`. After this new column is created, I dropped the `latitude` and `longitude` columns since they have become redundant. 

Then, I converted the `timestamp` column into the correct `timestamp` datatype, I also made sure that the formatting was in the correct form of `yyyy-MM-dd HH:mm:ss`.

Lastly, I reordered the columns into an order which required.

#### 6.1.3 Cleaning the User Data
---------------------------------

Firstly, I created a new column called `user_name`. In this column, I made a new string using values from the `first_name` and `last_name` columns. The new string looked like this looked like this `'first_name' 'last_name'`. After this new column is created, I dropped the `first_name` and `last_name` columns since they have become redundant.

Then, I converted the `date_joined` column into the correct `timestamp` datatype, I also made sure that the formatting was in the correct form of `yyyy-MM-dd HH:mm:ss`.

Lastly, I reordered the columns into an order which required.

### 6.2 SQL Queries
-------------------

I utilised the native __pyspark__ integration on __Databricks__  to create the queries. I wrote queries based on the following questions:

- Find the most popular category in each country.
- Find how many posts each category had between 2018 and 2022.
- Find the user with the most followers in each country.
- Find which was the most popular for different age groups.
- Find the median follower count for different age groups.
- Find how many users have joined between 2015 and 2020.
- Find the median follower count of users have joined between 2015 and 2020.
- Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.

## 7. AWS MWAA
--------------

__NOTE__: In this section, I did not need to create a __S3 Bucket__ for __MWAA__, and I also did not need to create a __MWAA environment__ since these had already been made for me. 

<ins>__Overview__</ins> :

I created a __DAG__, uploaded it to a __MWAA environment__ and __triggered__ it so it runs on __Databricks Notebooks__

<ins>__Fundamental Importance__</ins> :

Deploying a __DAG__ (Directed Acyclic Graph) on __MWAA__ allows us to __orchestrate__ and __automate__ __workflows__. By __triggering__ the __DAG__, we initiate and execute processes on the __Databricks Notebook__ which ensures the reproducibility and reliability of data workflows.

### 7.1 Create and Upload a DAG to a MWAA Environment
-----------------------------------------------------

To create the __DAG__, I made a new python script, [`<IAM_Username>-dag.py`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/%3CIAM_username%3E_dag.py) (the file had to be called this since I only had permission to upload a file titled this). In this file, I simply wrote the following code:

```
#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '<path-to-databricks-notebook>',
}

default_args = {
    'owner': '<name>',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

#Creating the DAG.
with DAG('<IAM_Username>-dag',
    start_date=datetime.now(),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='<cluster_id>',
        notebook_task=notebook_task
    )
    opr_submit_run
```
I was instructed to make the __DAG__ trigger once a day, indicated by the line `schedule_interval='@daily'` when creating the __DAG__. 

To obtain the `<cluster_id>`, I made sure I was in a notebook which was in the same path as `<path-to-databricks-notebook>` and ran the line:

```
spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
```

After making sure the setup was correct, I uploaded the `<IAM_Username>-dag.py` file to the appropriate __S3 bucket__ file path, with that being `mwaa-dags-bucket/dags`. 

### 7.2 Trigger a DAG that Runs a Databricks Notebook
-----------------------------------------------------

First, I navigated to the __MWAA console__, and in the __Environments__ section, I found the correct environment `Databricks-Airflow-env`, and then clicked on __Open Airflow UI__, which took me to the __Airflow UI__. I was presented with a list of __DAGs__, where I found I found the __DAG__ I had just created. After I clicked on my __DAG__, I clicked unpause and it started to get to work. After 6 minutes, the __DAG__ was finished and I was greeted with a _dark green_ box in the __Tree__ section of my __DAG__ indicating that it had been successfully ran! 

## 8 AWS Kinesis
----------------

<ins>__Overview__</ins> :

In this process, I established __data streams__ in __Amazon Kinesis__, configured my __API__ with __Kinesis proxy integration__, sent data to the __Kinesis streams__, read the data from the __Kinesis streams__ in __Databricks__, applied __transformations__ to the data in the __Kinesis streams__ and finally wrote the __Streaming data__ to __Delta Tables__ on __Databricks__. 

<ins>__Fundamental Importance__</ins> :

__Kinesis data streams__ allow us to ingest and process the __streaming data__ in real-time. But for the __streams__ to pick up the real-time data, we have to first configure __Kinesis proxy integration__ on the __API__ as this has been set up to direct the incoming data into the prescribed streams. Once all of this was set up, we can move to __Databricks__ which reads the __Kinesis streams__ and extracts the data ready to be __transformed__. After applying __transformations__ to the data, it was written to __Delta Tables__ on __Databricks__ which provide long time storage, where the data can undergo further analysis such as __ACID transformations__.

### 8.1 Create Data Streams Using Kinesis Data Streams
------------------------------------------------------

 I had to create three __Data streams__ which correspond to each table. So I navigated to the __Kinesis console__, and in the __Data streams__ section I pressed on the __Create data streams__ button. This took me to a page for the __Data stream configuration__. For the __Data stream name__, I called the three seperate streams `streaming-<IAM_username>-pin`,`streaming-<IAM_username>-pin` and `streaming-<IAM_username>-pin` (these streams had to be called this as I only had permission to create __Data streams__ named this way). And for all three, I made sure that the __Capacity mode__ which was selected was infact __On-demand__.

### 8.2 Configure an API with Kinesis Proxy Integration
-------------------------------------------------------

First of all, I need to go to the __IAM dashboard__ and then go onto the __Roles__ section. I filtered the results using my __IAM Username__ and found the __Kinesis access role__, which was named as `<IAM_username>-kinesis-access-role`. I clicked on it, and noted down my __ARN__ which was in the summary section.

Next, I went to the __API gateway__ and found the __API__ I was using [here](#41-building-a-kafka-rest-proxy-integration-method-for-the-api). I needed to make the __API__ invoke the following actions: List, create, describe, delete and add records to streams in Kinesis. I will talk about how I did all of these below.

#### 8.2.1 List Streams
----------------------

In the __Actions__ tab of the __API__, I clicked on __Create Resource__. For the __Resource Name__ I typed `streams`. I made sure that the __Resource Path__ was correct, it needed to be `/streams` and it was. I left the rest of the options as defualt and pressed __Create Reasource__.

Remaining on the __Resource__ page of the __API__, I selected `/streams`, making sure it showed a section called __/stream Methods__. I then pressed the __Actions__ tab again and clicked on __Create Method__. Here, I selected __`GET`__ from the drop-down list and pressed the tick next to it. I was then greeted by a __Setup__ panel and I configured the following settings:

- __Integration type__ as __AWS Service__
- __AWS Region__ as __`us-east-1`__
- __AWS Service__ as __Kinesis__
- __HTTP method__ as __`POST`__
- __Action Type__ as __User action name__
- __Action__ as __`ListStreams`__
- __Execution role__ as the __ARN__ I mentioned [here](#82-configure-an-api-with-kinesis-proxy-integration)

I pressed __Save__ and it took me to a page which showed the __Method Execution__. I pressed on the __Integration Request__ and then I scrolled down to find a section called __HTTP Headers__. Here, I selected __Add header__, entered the __Name__ as
__`Content-Type`__ and for __Mapped from__ I put __`'application/x-amz-json-1.1'`__. I the pressed the tick button which created the __HTTP header__. Next, I expanded the __Mapping Templates__ panel below the __HTTP Headers__ and I did the following: Selected __Add mapping template__, for __Content-Type__ I wrote `application/json` and then pressed the tick button. I scrolled down a bit to find the __template editior__, where I simply typed `{}`. I made sure to click the __Save__ button to save the __Mapping Template__

#### 8.2.2 Create Streams
-------------------------

First of all, we will need to create a new __child resource__ under the __`streams` resource__. For this __resource__, the __Resource Name__ is `stream-name` and the full __Resource Path__ is `/streams/{stream-name}`. 

After creating this, I simply clicked on `/{stream-name}` so that it was highlighted. Then, I pressed the __Actions__ box, selected __Create Method__. In the dropdown box that appeared, I selected __`POST`__ and pressed the tick. This took me to the __Setup__ panel, where I chose the following options:

- __Integration type__ as __AWS Service__
- __AWS Region__ as __`us-east-1`__
- __AWS Service__ as __Kinesis__
- __HTTP method__ as __`POST`__
- __Action Type__ as __User action name__
- __Action__ as __`CreateStream`__
- __Execution role__ as the __ARN__ I mentioned [here](#82-configure-an-api-with-kinesis-proxy-integration)

I pressed __Save__ and it took me to a page which showed the __Method Execution__. I pressed on the __Integration Request__ and then I scrolled down to find a section called __HTTP Headers__. Here, I selected __Add header__, entered the __Name__ as
__`Content-Type`__ and for __Mapped from__ I put __`'application/x-amz-json-1.1'`__. I the pressed the tick button which created the __HTTP header__. Next, I expanded the __Mapping Templates__ panel below the __HTTP Headers__ and I did the following: Selected __Add mapping template__, for __Content-Type__ I wrote `application/json` and then pressed the tick button. I scrolled down a bit to find the __template editior__, where I simply typed:

```
{
    "ShardCount": #if($input.path('$.ShardCount') == '') 5 #else $input.path('$.ShardCount') #end,
    "StreamName": "$input.params('stream-name')"
}
```
I finally clicked the __Save__ button to save the __Mapping Template__.

#### 8.2.3 Describe Streams
---------------------------

First of all, I simply clicked on `/{stream-name}` (from [before](#822-create-streams)) so that it was highlighted. Then, I pressed the __Actions__ box, selected __Create Method__. In the dropdown box that appeared, I selected __`GET`__ and pressed the tick. This took me to the __Setup__ panel, where I chose the following options:

- __Integration type__ as __AWS Service__
- __AWS Region__ as __`us-east-1`__
- __AWS Service__ as __Kinesis__
- __HTTP method__ as __`POST`__
- __Action Type__ as __User action name__
- __Action__ as __`DescribeStream`__
- __Execution role__ as the __ARN__ I mentioned [here](#82-configure-an-api-with-kinesis-proxy-integration)

I pressed __Save__ and it took me to a page which showed the __Method Execution__. I pressed on the __Integration Request__ and then I scrolled down to find a section called __HTTP Headers__. Here, I selected __Add header__, entered the __Name__ as
__`Content-Type`__ and for __Mapped from__ I put __`'application/x-amz-json-1.1'`__. I the pressed the tick button which created the __HTTP header__. Next, I expanded the __Mapping Templates__ panel below the __HTTP Headers__ and I did the following: Selected __Add mapping template__, for __Content-Type__ I wrote `application/json` and then pressed the tick button. I scrolled down a bit to find the __template editior__, where I simply typed:

```
{
    "StreamName": "$input.params('stream-name')"
}
```
I finally clicked the __Save__ button to save the __Mapping Template__.

#### 8.2.4 Delete Streams
-------------------------

First of all, I simply clicked on `/{stream-name}` (from [before](#822-create-streams)) so that it was highlighted. Then, I pressed the __Actions__ box, selected __Create Method__. In the dropdown box that appeared, I selected __`DELETE`__ and pressed the tick. This took me to the __Setup__ panel, where I chose the following options:

- __Integration type__ as __AWS Service__
- __AWS Region__ as __`us-east-1`__
- __AWS Service__ as __Kinesis__
- __HTTP method__ as __`POST`__
- __Action Type__ as __User action name__
- __Action__ as __`DeleteStream`__
- __Execution role__ as the __ARN__ I mentioned [here](#82-configure-an-api-with-kinesis-proxy-integration)

I pressed __Save__ and it took me to a page which showed the __Method Execution__. I pressed on the __Integration Request__ and then I scrolled down to find a section called __HTTP Headers__. Here, I selected __Add header__, entered the __Name__ as
__`Content-Type`__ and for __Mapped from__ I put __`'application/x-amz-json-1.1'`__. I the pressed the tick button which created the __HTTP header__. Next, I expanded the __Mapping Templates__ panel below the __HTTP Headers__ and I did the following: Selected __Add mapping template__, for __Content-Type__ I wrote `application/json` and then pressed the tick button. I scrolled down a bit to find the __template editior__, where I simply typed:

```
{
    "StreamName": "$input.params('stream-name')"
}
```
I finally clicked the __Save__ button to save the __Mapping Template__.

#### 8.2.5 Add Records to Streams
---------------------------------

Going back to my __API Resources__ page, I clicked on `/{stream-name}`, and then pressed the __Action__ button and selected __Create Resource__. I set up a child resource, where the __Resource Name__ was __`record`__ and the full __Resource Path__ is `/streams/{stream-name}/record`, I then hit save. Which took me back to the __API Resources__ page.

After clicking on `/record`, I clicked the __Actions__ box and selected __Create Method__. In the drop down box, I made sure to choose __`PUT`__ and then clicked on the tick button. This took me to the __Setup__ panel, where I chose the following options:

- __Integration type__ as __AWS Service__
- __AWS Region__ as __`us-east-1`__
- __AWS Service__ as __Kinesis__
- __HTTP method__ as __`POST`__
- __Action Type__ as __User action name__
- __Action__ as __`PutRecord`__
- __Execution role__ as the __ARN__ I mentioned [here](#82-configure-an-api-with-kinesis-proxy-integration)

I pressed __Save__ and it took me to a page which showed the __Method Execution__. I pressed on the __Integration Request__ and then I scrolled down to find a section called __HTTP Headers__. Here, I selected __Add header__, entered the __Name__ as
__`Content-Type`__ and for __Mapped from__ I put __`'application/x-amz-json-1.1'`__. I the pressed the tick button which created the __HTTP header__. Next, I expanded the __Mapping Templates__ panel below the __HTTP Headers__ and I did the following: Selected __Add mapping template__, for __Content-Type__ I wrote `application/json` and then pressed the tick button. I scrolled down a bit to find the __template editior__, where I simply typed:

```
{
    "StreamName": "$input.params('stream-name')",
    "Data": "$util.base64Encode($input.json('$.Data'))",
    "PartitionKey": "$input.path('$.PartitionKey')"
}
```
I finally clicked the __Save__ button to save the __Mapping Template__.

After creating all the new __Methods__ and __Resources__, I had to redeploy the __API__.

### 8.3 Send Data to the Kinesis Stream
---------------------------------------

Here, I edited the `user_posting_emulation.py` file last seen [here](#43-send-data-to-the-api). I added a function which took in the arguments: data obtained from a source, and the stream name of the stream I needed to send that data to. When running this file, I received a status code of __`200`__ which signified the data had been sent to the __Kinesis Stream__ correctly. 

### 8.4 Read Data from the Kinesis Streams in Databricks
--------------------------------------------------------

After creating a new __Notebook__ in __Databricks__\, I read the file `authentication_credentials.csv`, and retrieved the __`Access Key`__ and the __`Secret Access Key`__. This is exactly the same as [before](#51-mounting-a-s3-bucket-to-databricks), so I will spare you the details here. I made a function which reads the __Kinesis Stream__ data and transforms it into a usable dataframe ready for further transformation. The function is:

```
def read_kinesis_data(stream_name, json_schema):

    # Creating the kinesis_stream dataframe
    kinesis_stream = spark \
        .readStream \
        .format('kinesis') \
        .option('streamName', stream_name) \
        .option('initialPosition', 'earliest') \
        .option('region', 'us-east-1') \
        .option('awsAccessKey', ACCESS_KEY) \
        .option('awsSecretKey', SECRET_KEY) \
        .load()

    # deserialising the data
    df = kinesis_stream \
    .selectExpr("CAST(data as STRING)") \ 
    .withColumn("data", from_json(col("data"), json_schema)) \ 
    .select(col("data.*")) 
    return df
```

For the docstrings, please see [`stream_processing.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/stream_processing.ipynb).

### 8.5 Transform Kinesis streams in Databricks
-----------------------------------------------

Here, I performed transformations to each data set in the same way as I did [here](#61-data-cleaning).

For the each specific set of data see:
- [Cleaning the Pinterest Post Data](#611-cleaning-the-pinterest-post-data)
- [Cleaning the Geolocation Data](#612-cleaning-the-geolocation-data)
- [Cleaning the User Data](#613-cleaning-the-user-data)

To see the implementation, please see [`stream_processing.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/stream_processing.ipynb).

### 8.6 Write the Streaming Data to Delta Tables
------------------------------------------------

Here, I simply sent the cleaned data to __Delta Tables__ using the following function:

```
def write_kinesis_data(table_name, df):

    df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
    .table(table_name)
```

This function also includes `.option("checkpointLocation", "/tmp/kinesis/_checkpoints/")`, which allows me to recover the previous state of a table just incase something goes wrong.

For the docstrings, please see [`stream_processing.ipynb`](https://github.com/kimiko-dev/Pinterest-Data-Pipeline/blob/master/Databricks_notebooks/stream_processing.ipynb).


------




<sub>(Thank you for reading this journal! What a journey it has been from writing the code to all the documentation. This journal took a very long time to write, so I appreciate you finding me here! As a reward, take this :star2:)</sub>