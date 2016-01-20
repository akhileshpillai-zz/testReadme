###Architecture:
![Architecture Diagram](/capstone.png)

###Query and UI
* I wanted to understand the circumstances of the person tweeting about mental health so I decided to concentrate on hashtags. Currently, the UI, on hitting the update button; gets most updated result for the current hour.
For example if I hit update at 8:37 the UI will show me results for last 37 mins.
* For keywords = "mental health,depression,anxiety,suicidal thoughts":
![UI](/screenshot1.png)

###Storage
* HBase stores all the historic data so any further information for example how a particular hashtag trended could be derived from the data in it.

###RUNNING 
1. Start the kakfa producer.
  * spark-submit --class "twitterclient.TwitterKafkaProducer" 
    --packages "com.google.guava:guava:18.0,com.twitter:hbc-core:2.2.0,org.apache.spark:spark-streaming-twitter_2.10:1.5.2,org.apache.hbase:hbase-client:1.1.2,org.apache.hbase:hbase-common:1.1.2,org.apache.spark:spark-streaming-kafka_2.10:1.5.2,com.databricks:spark-avro_2.10:2.0.1" 
    --master yarn-client streaming-1.0-SNAPSHOT.jar <consumerKey> <consumerSecret> <token> <secret> <kafkabroker> <comma seperated keywords> <kafkatopic>
2. Start the Spark Streaming Service.
  * spark-submit --class "kafkaStream.JsonHBaseAppWithArgs" --packages "net.liftweb:lift-json_2.9.1:2.6.2,org.apache.spark:spark-streaming-twitte2.10:1.5.2,org.apache.hbase:hbase-client:1.1.2,org.apache.hbase:hbase-common:1.1.2,org.apache.spark:spark-streaming-kafka_2.10:1.5.2,com.databricks:spark-avro_2.10:2.0.1" 
    --master local[*] streaming-1.0-SNAPSHOT.jar <hbaseTable>
3. Start the Scalatra App.
  * java -cp ScalatraDemo-1.0-SNAPSHOT-jar-with-dependencies.jar "dei.AppArgs" <hbaseTable>
  
###Future Enhancements

