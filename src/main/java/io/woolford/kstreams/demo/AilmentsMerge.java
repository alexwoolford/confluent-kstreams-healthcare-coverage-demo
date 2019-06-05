package io.woolford.kstreams.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

public class AilmentsMerge {

    private static KeyValue rekeyJsonId(String message){
        JsonNode valueJsonNode = null;
        try {
            valueJsonNode = new ObjectMapper().readTree(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // TODO: handle NPE
        return new KeyValue<>(valueJsonNode.get("id").toString(), message);
    }

    private static JsonNode merge(JsonNode mainNode, JsonNode updateNode) {

        Iterator<String> fieldNames = updateNode.fieldNames();
        while (fieldNames.hasNext()) {

            String fieldName = fieldNames.next();
            JsonNode jsonNode = mainNode.get(fieldName);
            // if field exists and is an embedded object
            if (jsonNode != null && jsonNode.isObject()) {
                merge(jsonNode, updateNode.get(fieldName));
            }
            else {
                if (mainNode instanceof ObjectNode) {
                    // Overwrite field
                    JsonNode value = updateNode.get(fieldName);
                    ((ObjectNode) mainNode).put(fieldName, value);
                }
            }
        }

        return mainNode;
    }

    private static String merge(String mainNodeString, String updateNodeString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode mainNode = mapper.readTree(mainNodeString);
        JsonNode updateNode = mapper.readTree(updateNodeString);
        return merge(mainNode, updateNode).toString();
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        // stream the patient topic
        KStream<String, String> patient = builder.stream("patient");

        // re-key the patient topic so the ID is the key
        patient.map((k, v) -> (rekeyJsonId(v))).to("patient-rekey");

        // create a KTable from the re-keyed patient table
        KTable<String, String> patientKTable = builder.table("patient-rekey");

        // create a stream of ailment events (which are also keyed by the patient ID)
        KStream<String, String> ailment = builder.stream("ailment");

        // enrich the stream of ailments with the enrollment status of the patient enrollment dates
        KStream<String, String> joined = ailment.leftJoin(patientKTable,
                (mainNodeString, updateNodeString) -> {

                    String result = null;
                    try {
                        result = merge(mainNodeString, updateNodeString);
                    } catch (IOException e) {
                        // ignore
                    }
                    return result;

                }
        );

        // write the enriched ailments to a topic
        joined.to("ailment-enriched");

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));

    }

}
