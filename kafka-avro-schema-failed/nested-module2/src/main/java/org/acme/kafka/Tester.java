package org.acme.kafka;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

import org.acme.kafka.quarkus.Movie;

import io.quarkus.runtime.StartupEvent;

@ApplicationScoped
public class Tester {
    private final MovieSerializer serializer;

    public Tester(MovieSerializer serializer) {
        this.serializer = serializer;
    }


    public void test(@Observes StartupEvent e) throws IOException {
        System.out.println("Start test...");
        Movie movie = new Movie("movie1");

        byte[] serializedMovie = serializer.serialize(movie);
        Movie deserialized = serializer.deserialize(serializedMovie);
        System.out.println("Serialized and deserialized successfully");
    }
}
