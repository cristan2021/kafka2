package com.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class TraitementTemperature {

        private static final double TEMP_MIN = 15.0;
        private static final double TEMP_MAX = 19.0;

        public static class TemperatureStats {
                double sum;
                long count;

                public TemperatureStats() {
                        this.sum = 0.0;
                        this.count = 0;
                }

                public TemperatureStats(double sum, long count) {
                        this.sum = sum;
                        this.count = count;
                }

                public TemperatureStats add(double newTemperature) {
                        return new TemperatureStats(this.sum + newTemperature, this.count + 1);
                }

                public double getAverage() {
                        return (count == 0) ? 0.0 : sum / count;
                }
        }

        public void run() {
                Properties props = new Properties();
                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "traitementTemperature");
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
                props.put(StreamsConfig.STATE_DIR_CONFIG, "data/kafka-streams-state");

                StreamsBuilder constructeur = new StreamsBuilder();
                KStream<String, String> flux = constructeur.stream("topic1");

                TimeWindows fenetreTemps = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));

        
                KStream<String, Double> fluxTemperature = flux
                                .map(new KeyValueMapper<String, String, KeyValue<String, Double>>() {
                                        @Override
                                        public KeyValue<String, Double> apply(String cle, String valeur) {
                                                String[] parties = valeur.split(";");
                                                String batiment = parties[0];
                                                String salle = parties[1];
                                                double temperature = 0.0;

                                                try {
                                                        String tempString = parties[2].replace(',', '.');
                                                        temperature = Double.parseDouble(tempString);
                                                        System.out.println("Transformation donnée : " + batiment + ";"
                                                                        + salle
                                                                        + " | Temp: " + temperature);
                                                } catch (NumberFormatException e) {
                                                        System.out.println("Erreur de format sur la température: "
                                                                        + parties[2]);
                                                }

                                                return new KeyValue<>(batiment + "-" + salle, temperature);
                                        }
                                });

                KTable<Windowed<String>, TemperatureStats> moyenneTemperatureParSalle = fluxTemperature
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                                .windowedBy(fenetreTemps)
                                .aggregate(
                                                new Initializer<TemperatureStats>() {
                                                        @Override
                                                        public TemperatureStats apply() {
                                                                return new TemperatureStats();
                                                        }
                                                },
                                                new Aggregator<String, Double, TemperatureStats>() {
                                                        @Override
                                                        public TemperatureStats apply(String key, Double newTemperature,
                                                                        TemperatureStats aggValue) {
                                                                System.out.println("Ajouter Temp: " + newTemperature
                                                                                + " à " + key);
                                                                return aggValue.add(newTemperature);
                                                        }
                                                },
                                                Materialized.with(Serdes.String(), Serdes.serdeFrom(
                                                                new TemperatureStatsSerde.TemperatureStatsSerializer(),
                                                                new TemperatureStatsSerde.TemperatureStatsDeserializer())));

                KStream<Windowed<String>, Double> temperatureMoyenne = moyenneTemperatureParSalle.toStream()
                                .mapValues(new ValueMapper<TemperatureStats, Double>() {
                                        @Override
                                        public Double apply(TemperatureStats value) {
                                                return value.getAverage();
                                        }
                                });

                temperatureMoyenne.peek(new ForeachAction<Windowed<String>, Double>() {
                        @Override
                        public void apply(Windowed<String> cléFenêtrée, Double tempMoyenne) {
                                System.out.println("Salle: " + cléFenêtrée.key() + " | Temp Moyenne: " + tempMoyenne);
                                // Vérification des alertes
                                if (tempMoyenne < TEMP_MIN) {
                                        System.out.println("ALERTE: Température trop basse dans " + cléFenêtrée.key());
                                } else if (tempMoyenne > TEMP_MAX) {
                                        System.out.println("ALERTE: Température trop haute dans " + cléFenêtrée.key());
                                }
                        }
                });

                KStream<String, String> alertes = temperatureMoyenne
                                .filter(new Predicate<Windowed<String>, Double>() {
                                        @Override
                                        public boolean test(Windowed<String> cléFenêtrée, Double tempMoyenne) {
                                                System.out.println("Vérification Alerte pour " + cléFenêtrée.key()
                                                                + " | Temp Moyenne: " + tempMoyenne);
                                                return tempMoyenne < TEMP_MIN || tempMoyenne > TEMP_MAX;
                                        }
                                })
                                .map(new KeyValueMapper<Windowed<String>, Double, KeyValue<String, String>>() {
                                        @Override
                                        public KeyValue<String, String> apply(Windowed<String> cléFenêtrée,
                                                        Double tempMoyenne) {
                                                String salle = cléFenêtrée.key();
                                                String messageAlerte = "Alerte Temperature : " + salle
                                                                + " | Temp Moyenne: " + tempMoyenne;
                                                System.out.println(messageAlerte);
                                                return new KeyValue<>(salle, messageAlerte);
                                        }
                                });

                alertes.to("topic2", Produced.with(Serdes.String(), Serdes.String()));

                KafkaStreams streams = new KafkaStreams(constructeur.build(), props);
                streams.start();

                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                        @Override
                        public void run() {
                                streams.close();
                        }
                }));

                try {
                        Thread.sleep(100000);
                } catch (InterruptedException e) {
                        streams.close();
                        Thread.currentThread().interrupt();
                }
        }

        public static void main(String[] args) {
                TraitementTemperature traitement = new TraitementTemperature();
                Thread thread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                                traitement.run();
                        }
                });
                thread.start();
        }
}
