##Mental Health Stream
Recent Palo Alto teen suicides motivated me to learn more about how our societies around the world view mental illness. This subsequently led me to analyze the digital world for mental illness related signals. The current iteration of my project consists of a generic data pipeline that analyzes the live twitter stream and can be configured to focus on any topic. Users can refine topic discovery by easily modifying the configuration without having any understanding of the pipeline internals.

###Architecture:
![Architecture Diagram](/capstone.png)

###Processing, Query and UI
* I wanted to understand the circumstances of the person tweeting about mental health so I decided to concentrate on hashtags. The spark streaming layer outputs a word count of the hashtags and stores it in hbase. The UI layer creates an updated wordcloud, from the information stored in the HBase, for the current hour.
For example if I hit update at 8:37 the UI will show me results for last 37 mins.
* The picture below is the wordcloud for following keywords: "mental health,depression,anxiety,suicidal thoughts":
![UI](/screenshot1.png)

###Storage
* HBase stores all the historic data so any further hashtag trending information  could be derived from the data stored in it.

###Running the Data Pipeline
1. Start the kakfa producer.
  
```
#!python

spark-submit --class "twitterclient.TwitterKafkaProducer" 
    --packages "com.google.guava:guava:18.0,com.twitter:hbc-core:2.2.0,org.apache.spark:spark-streaming-twitter_2.10:1.5.2,org.apache.hbase:hbase-client:1.1.2,org.apache.hbase:hbase-common:1.1.2,org.apache.spark:spark-streaming-kafka_2.10:1.5.2,com.databricks:spark-avro_2.10:2.0.1" 
    --master yarn-client streaming-1.0-SNAPSHOT.jar <consumerKey> <consumerSecret> <token> <secret> <kafkabroker> <comma seperated keywords> <kafkatopic>
```

2. Start the Spark Streaming Service.
   
```
#!python

spark-submit --class "kafkaStream.JsonHBaseAppWithArgs" --packages "net.liftweb:lift-json_2.9.1:2.6.2,org.apache.spark:spark-streaming-twitte2.10:1.5.2,org.apache.hbase:hbase-client:1.1.2,org.apache.hbase:hbase-common:1.1.2,org.apache.spark:spark-streaming-kafka_2.10:1.5.2,com.databricks:spark-avro_2.10:2.0.1" 
    --master local[*] streaming-1.0-SNAPSHOT.jar <hbaseTable>
```

3. Start the Scalatra App.

```
#!python

java -cp ScalatraDemo-1.0-SNAPSHOT-jar-with-dependencies.jar "dei.AppArgs" <hbaseTable>
```

  
###Future Enhancements