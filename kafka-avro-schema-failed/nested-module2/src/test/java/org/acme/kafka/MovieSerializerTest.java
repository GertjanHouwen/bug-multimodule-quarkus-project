package org.acme.kafka;

import org.acme.kafka.quarkus.Movie;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MovieSerializerTest {
    private final MovieSerializer serializer = new MovieSerializer();

    @Test
    void serializes_and_deserializes() throws Exception {
        Movie movie = new Movie("movie1");

        byte[] serializedMovie = serializer.serialize(movie);
        Movie deserialized = serializer.deserialize(serializedMovie);

        assertEquals(deserialized, movie);
    }
}
