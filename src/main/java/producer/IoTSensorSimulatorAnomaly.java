package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath StreamAnalytics-0.0.3.0.jar producer.IoTSensorSimulatorAnomaly localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2021/08/07 14:28
 */

public class IoTSensorSimulatorAnomaly {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(IoTSensorSimulatorAnomaly.class);
    private static final Random random = new SecureRandom();
    private static final String LOGGERMSG = "Program prop set {}";

    private static String brokerURI = "kafka:9092";
    private static long sleeptime = 200;

    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            brokerURI = args[0];
            String parm = "'use customized URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        } else if (args.length == 2) {
            brokerURI = args[0];
            setsleeptime(Long.parseLong(args[1]));
            String parm = "'use customized URI' = " + brokerURI + " & 'use customized sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        } else {
            String parm = "'use default URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        }

        //create kafka producer
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-Kafka-IOT-Sensor-Anomaly");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        try (Producer<String, byte[]> producer = new KafkaProducer<>(config)) {

            WeightedRandomBag<Integer> itemDrops = new WeightedRandomBag<>();
            itemDrops.addEntry(0, 1.0);
            itemDrops.addEntry(1, 999.0);

            //prepare the record

            for (int i = 0; i < 1000000; i++) {
                Integer part = itemDrops.getRandom();

                if (part != 1) {
                    ObjectNode messageJsonObject = jsonObjectAnomaly();
                    byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

                    final ObjectNode node = new ObjectMapper().readValue(valueJson, ObjectNode.class);
                    String key = node.get("sensor_ts") + ":" + node.get("sensor_id");

                    ProducerRecord<String, byte[]> eventrecord = new ProducerRecord<>("healthyWorkplace", key, valueJson);
                    RecordMetadata msg = producer.send(eventrecord).get();
                    LOG.info(String.format("Published %s/%d/%d : %s", msg.topic(), msg.partition(), msg.offset(), jsonObjectAnomaly()));
                } else {
                    ObjectNode messageJsonObject = jsonObject();
                    byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

                    final ObjectNode node = new ObjectMapper().readValue(valueJson, ObjectNode.class);
                    String key = node.get("sensor_ts") + ":" + node.get("sensor_id");

                    ProducerRecord<String, byte[]> eventrecord = new ProducerRecord<>("healthyWorkplace", key, valueJson);
                    RecordMetadata msg = producer.send(eventrecord).get();
                    LOG.info(String.format("Published %s/%d/%d : %s", msg.topic(), msg.partition(), msg.offset(), jsonObject()));
                }


                // wait
                Thread.sleep(sleeptime);
            }
        }
    }

    // build random json object
    private static ObjectNode jsonObject() {

        ObjectNode report = objectMapper.createObjectNode();
        report.put("sensor_ts", Instant.now().toEpochMilli());
        report.put("sensor_id", (random.nextInt(41)));
        report.put("celsius", (random.nextInt(9)));
        report.put("lux", (random.nextInt(11)));
        report.put("ppm", (random.nextInt(22)));
        report.put("cm", (random.nextInt(33)));
        return report;
    }

    private static ObjectNode jsonObjectAnomaly() {

        ObjectNode report = objectMapper.createObjectNode();
        report.put("sensor_ts", Instant.now().toEpochMilli());
        report.put("sensor_id", (random.nextInt(3)));
        report.put("celsius", (random.nextInt(9)) + 99);
        report.put("lux", (random.nextInt(11)) * 11);
        report.put("ppm", (random.nextInt(22)));
        report.put("cm", (random.nextInt(33)));
        return report;
    }

    public static void setsleeptime(long sleeptime) {
        IoTSensorSimulatorAnomaly.sleeptime = sleeptime;
    }

    private static class WeightedRandomBag<T> {

        private final List<Entry> entries = new ArrayList<>();
        private final Random rand = new SecureRandom();
        private double accumulatedWeight;

        public void addEntry(T object, double weight) {
            accumulatedWeight += weight;
            Entry e = new Entry();
            e.object = object;
            e.accumulatedWeight = accumulatedWeight;
            entries.add(e);
        }

        public T getRandom() {
            double r = rand.nextDouble() * accumulatedWeight;

            for (Entry entry : entries) {
                if (entry.accumulatedWeight >= r) {
                    return entry.object;
                }
            }
            return null; //should only happen when there are no entries
        }

        private class Entry {
            double accumulatedWeight;
            T object;
        }
    }

}
