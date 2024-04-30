package org.ritesh;


import com.google.protobuf.GeneratedMessage;
import com.ritesh.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.sys.Prop;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static Properties getProperties(String filename) throws IOException {
        Properties properties = new Properties();
        try (InputStream stream = Main.class.getClassLoader().getResourceAsStream("kafkaproducer.properties")) {
            properties.load(stream);
        }

        return properties;
    }

    public static List<Person> createMockProtoData() {
        //mock the data
        // Create a mock data stream of protobuf person instances
        Person.Builder testPerson = Person.newBuilder().setName("Ritesh");
        List<Person> persons = new ArrayList<Person>();
        Random gen = new Random();
        for (int i = 0; i < 200; i++) {
            testPerson.setName("Ritesh_"+ String.valueOf(i));
            persons.add(testPerson.setAge(gen.nextInt(100)).build());
        };

        return persons;
    }

    public static  void  sendMockProtoDataToKafka(final StreamExecutionEnvironment env) throws Exception {
        Properties properties = null;
        try {
            properties = getProperties("kafkaproducer.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Person> persons = createMockProtoData();
        // Serializers
        GenericBinaryProtoSerializer<Person> genericSerializer = new GenericBinaryProtoSerializer<>(properties.getProperty("kafka.topic"));
        ////PersonSerializer specificSerializer = new PersonSerializer();

        //Create source stream
        DataStream<Person> stream = env.fromCollection(persons);

        KafkaSink<Person> sink = KafkaSink.<Person>builder()
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(genericSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("rit-trx-id-prefix")
                .build();

        stream.sinkTo(sink).name("person_sink_3");
    }

    public static  void  readMockProtoDataToKafka(final StreamExecutionEnvironment env) {
        // Deserializers
        GenericBinaryProtoDeserializer<Person> genericDeserializer = new GenericBinaryProtoDeserializer<>(Person.class);
        //PersonDeserializer specificDeserializer = new PersonDeserializer();

        Properties properties = null;
        try {
            properties = getProperties("kafkaproducer.properties");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        Random gen = new Random();
        Integer r = gen.nextInt(1000000);
        properties.setProperty("group.id", String.valueOf(r));

        KafkaSource<Person> source = KafkaSource.<Person>builder()
                .setProperties(properties)
                .setTopics(properties.getProperty("kafka.topic"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(genericDeserializer)
                .build();

        DataStream<Person> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "person_source");

        stream.print().name("MessageFromKafka");

        System.out.println();
    }

    public static void main(String[] args) throws Exception {


        LOG.info("Hello and welcome!");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

//        try {
//            sendMockProtoDataToKafka(env);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }

        readMockProtoDataToKafka(env);

        env.execute("PersonPipeline");
    }



//    public static void workingmain(String[] args) throws Exception {
//
//        System.out.printf("Hello and welcome!");
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
//
//        Properties properties = new Properties();
//        try (InputStream stream = Main.class.getClassLoader().getResourceAsStream("kafkaproducer.properties")) {
//            properties.load(stream);
//        }
//
//        //mock the data
//        // Create a mock data stream of protobuf person instances
//        Person.Builder testPerson = Person.newBuilder().setName("Ritesh");
//        List<Person> persons = new ArrayList<Person>();
//        Random gen = new Random();
//        for (int i = 0; i < 200; i++) {
//            testPerson.setName("Ritesh_"+ String.valueOf(i));
//            persons.add(testPerson.setAge(gen.nextInt(100)).build());
//        };
//
//        DataStream<Person> personsStreamIn = env.fromCollection(persons);
//
//
//        // Serializers
//        GenericBinaryProtoSerializer<Person> genericSerializer = new GenericBinaryProtoSerializer<Person>(properties.getProperty("kafka.topic"));
//        //PersonSerializer specificSerializer = new PersonSerializer();
//
//        // Deserializers
//        GenericBinaryProtoDeserializer<Person> genericDeserializer = new GenericBinaryProtoDeserializer<>(Person.class);
//        //PersonDeserializer specificDeserializer = new PersonDeserializer();
//
//        //KafkaSink
//        KafkaSink<Person> sink = KafkaSink.<Person>builder()
//                .setKafkaProducerConfig(properties)
//                .setRecordSerializer(genericSerializer)
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix("rit-trx-id-prefix")
//                .build();
//        //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
//
////        KafkaSource<Person> ksource = KafkaSource.<Person>builder()
////                .setProperties(properties)
////                .setTopics(properties.getProperty("kafka.topic"))
////                .setStartingOffsets(OffsetsInitializer.latest())
////                .setValueOnlyDeserializer(new JsonDeserializationSchema(SkyOneAirlinesFlightData.class))
////                .build();
//
//
//        // Producer
////        FlinkKafkaProducer011<Person> personsKafkaProducer =
////                new FlinkKafkaProducer011<Person>("PERSONS_TOPIC", genericSerializer, producerProperties);
//        personsStreamIn.sinkTo(sink).name("person_sink_3");
//
//        env.execute("PersonPopulater_3");
//    }
}