package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;

public class Producteur implements Runnable {
    private long id;
    private final Map<String, List<String>> batimentSalles = new HashMap<>();

    public Producteur(long id) {
        this.id = id;
        // Définir les bâtiments et leurs salles associées avec des numéros uniques
        batimentSalles.put("Batiment1", Arrays.asList("Salle1", "Salle2", "Salle3"));
        batimentSalles.put("Batiment2", Arrays.asList("Salle4", "Salle5", "Salle6"));
        batimentSalles.put("Batiment3", Arrays.asList("Salle7", "Salle8", "Salle9"));
    }

    public void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producteur = new KafkaProducer<>(props);

        Random random = new Random();

        while (true) {
            for (Map.Entry<String, List<String>> entry : batimentSalles.entrySet()) {
                String batiment = entry.getKey(); // Numéro du bâtiment
                for (String salle : entry.getValue()) {
                    String message = batiment + ";" + salle + ";"
                            + String.format("%.2f", 15 + (random.nextDouble() * 10)); // Température aléatoire
                    producteur.send(new ProducerRecord<String, String>("topic1", batiment, message));
                    System.out.println("envoyé: prod " + id + " " + message);
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }

        producteur.close();
        System.out.println("Producteur terminé.");
    }
}
