package CustomProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Main {

	private static final String TOPIC = "topic-test";

	public static void main(final String[] args) {
		// TODO: setup a few different case on a few different topics.
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
			producer.beginTransaction();
			for (long i = 0; i < 10; i++) {
				boolean commit = i % 5 == 0;

				final String key = "key: " + i;
				final String message;
				if (commit) {
					message = "Committed: " + i;
				} else {
					message = "Uncommited: " + i;
				}

				final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, key, message);

				// if `get()` is not used, aborting the transaction may prevent this message to be send at all. Some kind of optimisation, i guess.
				// following documentation, use `get()` to simulate synchronous call and force the message to be actually sent.
				producer.send(record).get();

				if (commit) {
					producer.commitTransaction();
					producer.beginTransaction();
				} else if (i % 5 == 4) {
					producer.abortTransaction();
					producer.beginTransaction();
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