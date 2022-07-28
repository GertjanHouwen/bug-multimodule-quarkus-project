package org.acme.kafka;

import javax.inject.Inject;

import org.acme.kafka.quarkus.Movie;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
public class MovieSerializerIT {
//    @Inject MovieSerializer serializer;
    private final MovieSerializer serializer = new MovieSerializer();

    @Test
    void serializes_and_deserializes() throws Exception {
        Movie movie = new Movie("movie1");

        byte[] serializedMovie = serializer.serialize(movie);
        Movie deserialized = serializer.deserialize(serializedMovie);

        assertEquals(deserialized, movie);
    }

}
