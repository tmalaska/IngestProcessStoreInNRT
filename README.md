
#NyTaxi
## Setup 
- Create CDH Cluster with the following installed
 - Kudu
 - Impala-Kudu
 - SolR
 - Spark
 - HDFS
 - Kafka
- Create ny-taxi-trip-collection
 - Log in to gateway node
 - solrctl instancedir --generate ny-taxi-trip-collection
 - take the schema.xml and put it into the car-event-collection/conf folder on your local
 - solrctl instancedir --create ny-taxi-trip-collection ny-taxi-trip-collection
 - solrctl collection --create ny-taxi-trip-collection -s 3 -r 2 -m 3
- Create Kudu tables
 - open up hue and go to the impala tab
 - Run sql in impala/ny_taxi/create_ny_taxi_yellow_entity_table.impala
 - Run sql in impala/ny_taxi/create_ny_taxi_yellow_trip_table.impala
- Create Kafka Topic
 - kafka-topics --zookeeper ted-malaska-capone-two-2.vpc.cloudera.com:2181 --partition 2 --replication-factor 2  --create --topic ny-trip-topic
- Test Kafka Topic
 - kafka-console-producer --broker-list ted-malaska-capone-two-4.vpc.cloudera.com:9092,ted-malaska-capone-two-5.vpc.cloudera.com:9092 --topic ny-trip-topic
 - kafka-console-consumer --zookeeper ted-malaska-capone-two-2.vpc.cloudera.com:2181 --topic ny-trip-topic
- Build Jar
 - Go to entity360 folder
 - mvn package
 - scp jar to gateway node
- populate Kafka
 - scp yellow_tripdata_2009-01.10000.csv to edge node
 - java -cp IngestProcessStoreInNRT.jar com.cloudera.demo.common.CsvKafkaPublisher ted-malaska-capone-two-4.vpc.cloudera.com:9092,ted-malaska-capone-two-5.vpc.cloudera.com:9092 ny-trip-topic yellow_tripdata_2009-01.10000.csv 50 0 10 async 1000
 - If you want to test it
  - kafka-console-consumer --zookeeper 172.31.14.160:2181 --topic ny-trip-topic
- Start Spark Streaming to SolR And Kudu
 - spark-submit --class com.cloudera.demo.ny_taxi.NyTaxiYellowTripStreaming --master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 IngestProcessStoreInNRT.jar ted-malaska-capone-two-4.vpc.cloudera.com:9092,ted-malaska-capone-two-5.vpc.cloudera.com:9092 ted-malaska-capone-two-2.vpc.cloudera.com:2181/solr ny-trip-topic ted-malaska-capone-two-1.vpc.cloudera.com ny_taxi_yellow_entity ny_taxi_yellow_trip ny-taxi-trip-collection 2 t t /user/root/checkpointDirectory c
 - spark-submit --class com.cloudera.demo.ny_taxi.NyTaxiYellowTripStreaming --master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 IngestProcessStoreInNRT.jar ted-malaska-capone-two-4.vpc.cloudera.com:9092,ted-malaska-capone-two-5.vpc.cloudera.com:9092 ted-malaska-capone-two-2.vpc.cloudera.com:2181/solr ny-trip-topic ted-malaska-capone-two-1.vpc.cloudera.com ny_taxi_yellow_entity ny_taxi_yellow_trip ny-taxi-trip-collection 2 t f /user/root/checkpointDirectory c

##Shut Down
solrctl collection --delete ny-taxi-trip-collection

#Upgrading Java
service cloudera-scm-server stop



sudo wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u91-b14/jdk-8u91-linux-x64.tar.gz"

sudo tar xzf jdk-8u91-linux-x64.tar.gz

mv jdk1.8.0_91 /opt

cd /opt/jdk1.8.0_91/

sudo alternatives --install /usr/bin/java java /opt/jdk1.8.0_91/bin/java 1

sudo alternatives --config java

sudo alternatives --install /usr/bin/jar jar /opt/jdk1.8.0_91/bin/jar 1

sudo alternatives --install /usr/bin/javac javac /opt/jdk1.8.0_91/bin/javac 1

sudo alternatives --set jar /opt/jdk1.8.0_91/bin/jar

sudo alternatives --set javac /opt/jdk1.8.0_91/bin/javac

sudo ln -sf /opt/jdk1.8.0_91/ /usr/java/latest/

