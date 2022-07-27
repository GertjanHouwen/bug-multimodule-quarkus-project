package org.acme.kafka;

import org.acme.kafka.quarkus.Movie;
import org.jboss.logging.Logger;


public class MovieResource {

    public void enqueueMovie(Movie movie) {
        System.out.println("hallo");
    }

}
