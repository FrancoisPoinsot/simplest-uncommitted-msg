package CustomProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Main {

	private static final String TOPIC = "topic-test";

	public static void main(final String[] args) {
		try {
			final Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

			props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-transactional-producer");
			props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
			props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, java.util.UUID.randomUUID().toString());

			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

			producer.initTransactions();
			for (long i = 0; i < 10; i++) {
				producer.beginTransaction();

				final String key = "key: " + i;
				final String message = "test msg: " + i;
				final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, key, message);
				producer.send(record);

				if (i % 3 == 0) {
					producer.commitTransaction();
				} else {
					producer.abortTransaction();
				}
			}

			producer.flush();
			producer.close();
			System.out.printf("I am done", TOPIC);
		} catch (Exception err) {
			System.out.println("got error");
			System.out.println(err);
		}
	}
}