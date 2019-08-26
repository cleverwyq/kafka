echo produce +  $1
java -cp .:/home/young/.m2/repository/org/apache/kafka/kafka-clients/2.3.0/kafka-clients-2.3.0.jar:/home/young/.m2/repository/org/slf4j/slf4j-api/1.7.26/slf4j-api-1.7.26.jar  HelloProducer $1 $2
