package org.acme.kafka;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.quarkus.test.junit.QuarkusTest;
import java.util.HashMap;
import java.util.Properties;
import org.acme.kafka.quarkus.Movie;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class SerializeTest {

    @Test
    public void test_serialization() throws Exception {
        Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
        schemaRegistry.register("test-value", new AvroSchema(Movie.SCHEMA$));

        IndexedRecord movie = Movie.newBuilder().setTitle("Up").build();

        byte[] bytes = avroSerializer.serialize("test", movie);

        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
        Movie movie1 = (Movie) avroDeserializer.deserialize("test", bytes);

    }

}
