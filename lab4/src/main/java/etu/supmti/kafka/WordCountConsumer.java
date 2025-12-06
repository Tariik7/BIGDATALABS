package etu.supmti.kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WordCountConsumer {

    private static final String TOPIC = "WordCount-Topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    private static final String GROUP_ID = "word-count-group";

    public static void main(String[] args) {
        // 1. Configuration du Consommateur
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Commencez la lecture depuis le début si aucun offset n'est trouvé
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); 

        // Map pour stocker le décompte des mots (état local)
        Map<String, Long> wordCounts = new HashMap<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            
            // S'abonner au topic
            consumer.subscribe(Collections.singletonList(TOPIC));
            
            System.out.println("WordCountConsumer démarré. Attente de messages...");
            
            // 2. Boucle de Consommation
            while (true) {
                // Poll pour récupérer les enregistrements (attente max 100ms)
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    records.forEach(record -> {
                        String word = record.value();
                        
                        // Mettre à jour le décompte local
                        wordCounts.put(word, wordCounts.getOrDefault(word, 0L) + 1);
                    });

                    // 3. Afficher le décompte actuel après chaque lot
                    System.out.println("\n--- MISE À JOUR DU COMPTE (Total : " + records.count() + " messages) ---");
                    wordCounts.forEach((word, count) -> 
                        System.out.printf("'%s': %d\n", word, count)
                    );
                    System.out.println("------------------------------------------");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}