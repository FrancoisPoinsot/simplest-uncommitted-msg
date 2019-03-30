package CustomProducer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Main {

	private static String brokerList;
	public static void main(final String[] args) {
		Options options = new Options();
		Option brokers = new Option("b", "brokers", true, "");
		brokers.setRequired(true);
		options.addOption(brokers);
		try {
			CommandLine cmd = new DefaultParser().parse(options, args);
			brokerList = cmd.getOptionValue("brokers");

			produceMessagesInSameTransaction();
			produceMessagesEndWithUncommitted();
			produceMessagesInSameBatch();
			produceWithDifferentProducers();
			//produceABunchOfMessages();	// meh, takes too much time
		} catch (Exception err) {
			System.out.println("got error");
			System.out.println(err);
		}
		System.out.printf("\nsimplest uncommitted message producer finished\n");
	}

	private static void produceMessagesInSameTransaction() throws ExecutionException, InterruptedException {
		final String topic = "uncommitted-topic-test1";

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getDefaultProperties());
		producer.initTransactions();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.abortTransaction();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 1")).get();
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 2")).get();
		producer.commitTransaction();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.abortTransaction();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 3")).get();
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 4")).get();
		producer.commitTransaction();

		producer.flush();
		producer.close();

		System.out.println("finished produceMessagesInSameTransaction");
	}

	private static void produceMessagesEndWithUncommitted() throws ExecutionException, InterruptedException {
		final String topic = "uncommitted-topic-test-2";

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getDefaultProperties());
		producer.initTransactions();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.abortTransaction();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 1")).get();
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 2")).get();
		producer.commitTransaction();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.abortTransaction();

		producer.flush();
		producer.close();

		System.out.println("finished produceMessagesEndWithUncommitted");
	}

	private static void produceMessagesInSameBatch() throws ExecutionException, InterruptedException {
		final String topic = "uncommitted-topic-test-3";

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getDefaultProperties());
		producer.initTransactions();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted"));
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.abortTransaction();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 1"));
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 2")).get();
		producer.commitTransaction();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted"));
		producer.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();
		producer.abortTransaction();

		producer.beginTransaction();
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 3"));
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 4"));
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 5"));
		producer.send(new ProducerRecord<String, String>(topic, "key", "Committed 6")).get();
		producer.commitTransaction();

		producer.flush();
		producer.close();

		System.out.println("finished produceMessagesInSameBatch");
	}

	private static void produceWithDifferentProducers() throws ExecutionException, InterruptedException {
		final String topic = "uncommitted-topic-test-4";

		KafkaProducer<String, String> producer1 = new KafkaProducer<String, String>(getDefaultProperties());
		KafkaProducer<String, String> producer2 = new KafkaProducer<String, String>(getDefaultProperties());
		producer1.initTransactions();
		producer2.initTransactions();
		{
			producer1.beginTransaction();
			producer1.send(new ProducerRecord<String, String>(topic, "key", "uncommitted"));
			producer1.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();


			producer2.beginTransaction();
			producer2.send(new ProducerRecord<String, String>(topic, "key", "Committed 1"));
			producer2.send(new ProducerRecord<String, String>(topic, "key", "Committed 2")).get();

			producer2.commitTransaction();
			producer1.abortTransaction();
		}
		{
			producer2.beginTransaction();
			producer2.send(new ProducerRecord<String, String>(topic, "key", "uncommitted"));
			producer2.send(new ProducerRecord<String, String>(topic, "key", "uncommitted")).get();

			producer1.beginTransaction();
			producer1.send(new ProducerRecord<String, String>(topic, "key", "Committed 3"));
			producer1.send(new ProducerRecord<String, String>(topic, "key", "Committed 4"));
			producer1.send(new ProducerRecord<String, String>(topic, "key", "Committed 5"));
			producer1.send(new ProducerRecord<String, String>(topic, "key", "Committed 6")).get();


			producer2.abortTransaction();
			producer1.commitTransaction();
		}
		producer1.flush();
		producer1.close();
		producer2.flush();
		producer2.close();

		System.out.println("finished produceWithDifferentProducers");
	}


	// It goes:
	// - for 1M messages
	// - every 100k messages
	// - commit the first 100
	// - abort the rest
	private static void produceABunchOfMessages() {
		final String topic = "uncommitted-topic-test-5";

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getDefaultProperties());

		producer.initTransactions();
		producer.beginTransaction();
		int commitCount = 0;

		for (int messageCount = 0; messageCount < 1000000; messageCount++) {
			if (messageCount % 100000 == 0) {
				System.out.printf("produceABunchOfMessages : %d messages\n", messageCount);
			}

			if (messageCount % 100000 < 100) {
				producer.send(new ProducerRecord<String, String>(topic, "key", "Committed " + ++commitCount));
			}
			if (messageCount % 100000 == 99) {
				producer.commitTransaction();
				producer.beginTransaction();
			}
			if (messageCount % 100000 >= 100) {
				producer.send(new ProducerRecord<String, String>(topic, "key", "Uncommitted"));
			}
			if (messageCount % 100000 == 99999) {
				producer.abortTransaction();
				producer.beginTransaction();
			}
		}

		producer.flush();
		producer.close();

		System.out.println("finished produceABunchOfMessages");
	}

	private static Properties getDefaultProperties() {
		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-transactional-producer");
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, java.util.UUID.randomUUID().toString());

		return props;
	}
}