package etu.supmti.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.Arrays;

public class WordProducer {

    private static final String TOPIC = "WordCount-Topic";
    // Utiliser les deux brokers actifs pour la robustesse
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";

    public static void main(String[] args) {
        // 1. Configuration du Producteur
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             Scanner scanner = new Scanner(System.in)) {
            
            System.out.println("WordProducer prêt. Entrez des lignes de texte (tapez 'exit' pour quitter) :");
            
            while (true) {
                System.out.print("> ");
                String line = scanner.nextLine();

                if ("exit".equalsIgnoreCase(line.trim())) {
                    break;
                }

                // Nettoyer et diviser la ligne en mots
                String[] words = line
                    .toLowerCase()
                    .replaceAll("[^a-z ]", "") // Enlève la ponctuation (sauf espace)
                    .split("\\s+"); // Divise par un ou plusieurs espaces

                // 2. Envoyer chaque mot comme un message Kafka
                Arrays.stream(words)
                      .filter(word -> !word.isEmpty())
                      .forEach(word -> {
                          // Nous utilisons le mot comme clé ET comme valeur
                          ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, word, word);
                          producer.send(record);
                          System.out.println("  -> Envoyé: " + word);
                      });
                
                producer.flush(); // Garantir que les messages sont envoyés immédiatement
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}