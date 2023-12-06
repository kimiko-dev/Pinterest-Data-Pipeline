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


## 1. Introduction
------------------

## 2. Configuring the EC2 Kafka Client
--------------------------------------

### 2.1 Create a `.pem` Key File Locally
----------------------------------------

Upon signing into my AWS account, I navigated to the __Parameter Store__. I searched the parameters ussing my `SSH Keypair ID`. Under __Parameter details__, I found the and decrypted the `Value`, revealing an _RSA private key_. I copied and pasted this key into a new `.pem` file, making sure this was created in the correct folder I wanted to connect to the __EC2 instance__ in. However, we need to name this file as `<Key pair assigned at launch>.pem`. To identify the `Key pair assigned at launch`, I went to the __Instances__ section located in the __EC2 Service__. I found the correct instance by referencing my __AWS IAM Username__, and then accessed the __instance summary__. In the __details__ section, I found `Key pair assigned at launch`, which I copy and pasted as the filename.

### 2.2 Connect to the EC2 Instance
-----------------------------------

I needed to connect to the __EC2 instance__ on an __SSH client__. I am using __WSL__, and I am using the `OpenSSH` client. To install the `OpenSSH` client, I simply wrote the command `sudo apt-get install openssh-client` in a __WSL__ terminal. After confirming I was in the same folder where the `.pem` file is stored, I ran the command `chmod 400 <Key pair assigned at launch>.pem` to make sure the file had been set to the correct permissions. Then I ran `ssh -i "<Key pair assigned at launch>.pem" root@<Public IPv4 DNS>`, which connected me to the __EC2 instance__. 

### 2.3 Set Up Kafka on the EC2 Client
--------------------------------------

I need to download and install __Kafka__ on my __client EC2 machine__. I have already been proivded with access to an __IAM authenticated MSK cluster__, so I did not have to do this in the project. On the __EC2 client__, I first had to install __Java__, with following command `sudo yum install java-1.8.0`. Since the cluster I was using was running on __Kafka 2.12-2.8.1__, I had to make sure that I dowloaded the correct version of __Kafka__ on my __client EC2 machine__. The command for this is: `wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz`. With the file downloaded, I then used `tar -xzf kafka_2.12-2.8.1.tgz` to extract all the files needed for __Kafka__. 

Following this, I needed to download the __IAM MSK authentication package__ on my __client EC2 machine__. First I navigated to the `libs` folder inside the __Kafka 2.12-2.8.1__ folder (the filepath is `/home/ec2-user/kafka_2.12-2.8.1/libs`). Then, I used the command `wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar`. Before I continued, I navigated to the __Roles__ section in the __IAM console__. It gave me a list of roles, then I had to find the role which was named `<your_UserId>-ec2-access-role`. In this role, I made note of the __ARN__ (_Amazon Resource Name_) in the __Summary__ section. Below the __Summary__ section, I selected the __Trust relationships__ tab, and then hit __Edit trust policy__. Here, I clicked on the __Add a principal__ button and selected __IAM roles__ as the __Principal type__. I had to then paste my __ARN__ in the box below. Upon doing this, I hit the __Add principal__ button, which successfully gave me the __IAM role__, which contains the necessary permissions to authenticate to the __MSK cluster__.

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

Finally, I had to set up the `CLASSPATH` in the `.bashrc` file. This file is located at `/home/ec2-user/.bashrc`. When editing, I added the line: `export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar`. Also, while I was here, I made sure to add in `export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.382.b05-1.amzn2.0.2.x86_64/jre` too. To make sure the environment variables were set correctly, I wrote the following commands `echo $CLASSPATH` and `echo $JAVA_HOME`

### 2.4 Create Kafka Topics
---------------------------

Before creating the __Kafka topics__, I needed to note down the `Bootstrap servers string` and the `Plaintext Apache Zookeeper connection string`. To retreive these, I headed to the __MSK Management Console__, clicked on the appropriate __cluster__ and clicked on __View client information__. The `Bootstrap servers string` was in the __Bootstrap servers__ box, in the __Private endpoint (single-VPC)__ column. Scrolling down, I found the `Plaintext Apache Zookeeper connection string` in the __Apache ZooKeeper connection__, where I copied the _Plaintext_ string. 

Now we can create the three following topics:
- `<your_UserId>.pin` for the Pinterest posts data
- `<your_UserId>.geo` for the post geolocation data
- `<your_UserId>.user` for the post user data

To do this (in the __EC2 client__) I navigated to `/home/ec2-user/kafka_2.12-2.8.1/bin`, then wrote the following command `./kafka-console-producer.sh --bootstrap-server <BootstrapServerString> --producer.config client.properties --group students --topic <topic_name>`. Where I made sure to replace the `BootstrapServerString` with the one I obtained previously, and also replaced `<topic_name>` appropriately (we need to make 3 topics, so I used this command three times each with the desired topic names.)

## 3. Connect a MSK Cluster to a S3 Bucket
------------------------------------------

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

For this project, I did not need to create my own __API__ since one had already been made for me. However, I still needed to configure it. 

### 4.1 Building a Kafka REST Proxy Integration Method for the API
------------------------------------------------------------------

First of all, I navigated to the __EC2 instances__ and found my __client EC2 machine__ and selected it. In the __Details__ section, I found the __Public IPv4 DNS__ and made note of it for later use.

On the __API Gateway__, I searched for the __API__ which had been setup for me. Upon finding it, I clicked on it which took me to the __Resources__ page. Here, I clicked on the __Create resource__ button, which took me to a set up screen. Now, I made sure to turn on __Proxy resource__, I set the __Resource path__ as `/{proxy+}` and set the __Resource name__ as `{proxy+}`. Lastly, I selected the __CORS (Cross Origin Resource Sharing)__ box and then hit __Create resource__.

When clicking on the new resource I had just created, I pressed the __Create method__ button as I wanted to create a __HTTP ANY method__. For the __Method type__ I selected __ANY__ in the dropdown box. For __Integration type__, I selected __HTTP Proxy__. Finally, for the __Endpoint URL__, I wrote the following `http://<Public IPv4 DNS>:8082/{proxy}`. I then clicked __Create Method__, which successfully created the method. 

Now, I needed to get __Invoke URL__. To do this, I simply pressed the __Deploy API__ button, created a new __Stage__ called `dev` and then deployed the API. It took me to the __Stages__ page on the __API Gateway__ where it gave me the __Invoke URL__, and I noted this down. 

### 4.2 Setting Up the Kafka REST Proxy on the EC2 Client
---------------------------------------------------------

On the __client EC2 machine__ I had to install __Confluent package__ for the __Kafka REST Proxy__. To do this, I ran the following commands: `sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz` (which downloads the __Confluent package__) and then `tar -xvzf confluent-7.2.0.tar.gz ` (which extracts and decompresses the contents of `confluent-7.2.0.tar.gz`)

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

Here, I edited the `user_posting_emulation.py` file which was supplied to us. I added a function which took in the arguments: data obtained from a source, and the topic name of the topic I needed to send that data to. For the details, please consult the __GitHub Wiki__.

## 5. Setting up Databricks
---------------------------

Before I was able to clean the data, I needed to read the data from my __S3 bucket__ into __Databricks__. This can be done by mounting the desired __S3 bucket__ to __Databricks__.

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

## 6. Data Cleaning and SQL Queries Using Spark on Databricks
-------------------------------------------------------------

### 6.1 Data Cleaning
---------------------

First, I had to read the data from the __mounted S3 bucket__. I had to read this data three times since I had three __S3 URL__\s where the seperate data was stored. The code for this was:

```
# File location and type
file_location = "/mnt/<mount_name>/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
<appropriate_df_name> = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
```
In the below subsections, I will explain how I cleaned each dataframe.

#### 6.1.1 Cleaning the Pinterest Post Data
-------------------------------------------

Here, there were rows of post data which were labled as `multi-video(story page format)` and a lot of its columns had 'empty' entries, such as:

| Column Label | Entry |
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

Lastly, I reordered the columns into an order which I was asked to do.

#### 6.1.2 Cleaning the Geolocation Data
----------------------------------------

#### 6.1.3 Cleaning the User Data
---------------------------------

### 6.2 SQL Queries
-------------------