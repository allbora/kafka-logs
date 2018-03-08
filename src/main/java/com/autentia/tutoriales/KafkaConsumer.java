package com.autentia.tutoriales;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ibr.common.Constantes;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class KafkaConsumer {

	private static final Logger log = Logger.getLogger(KafkaConsumer.class);
	private static final int FETCH_SIZE = 100000;
	private static final int MAX_NUM_OFFSETS = 1;
	private static final int BUFFER_SIZE = 64 * 1024;
	private static final int TIMEOUT = 100000;
	private static final int PARTITION = 1;
	private static final int PORT = 9092;
	private static final String BROKER = "localhost";
	private static final String CLIENT = "testClient";
	private final SimpleConsumer consumer;
	
	public KafkaConsumer() {
		this.consumer = new SimpleConsumer(BROKER, PORT, TIMEOUT, BUFFER_SIZE, CLIENT);
	}
	
	public void run() throws Exception {
		long readOffset = getLastOffset(consumer, kafka.api.OffsetRequest.EarliestTime());

		//consumer never stops
		while (true) {
			final FetchRequest req = new FetchRequestBuilder().clientId(CLIENT).addFetch(Constantes.TOPIC, PARTITION, readOffset, FETCH_SIZE) .build();
			final FetchResponse fetchResponse = consumer.fetch(req);

			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(Constantes.TOPIC, PARTITION)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					continue;
				}
				
				readOffset = messageAndOffset.nextOffset();
				final ByteBuffer payload = messageAndOffset.message().payload();

				final byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				
				log.info("[" + messageAndOffset.offset() + "]: " + new String(bytes, "UTF-8"));
			}
		}
	}

	public static long getLastOffset(SimpleConsumer consumer, long whichTime) {
		long result = 0; 
		try {
			final TopicAndPartition topicAndPartition = new TopicAndPartition(Constantes.TOPIC, PARTITION);
			final Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, MAX_NUM_OFFSETS));
			
			final OffsetRequest offsetRequest = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), CLIENT);
			
			OffsetResponse offsetsBefore = consumer.getOffsetsBefore(offsetRequest);
			if (!offsetsBefore.hasError()) {
				return offsetsBefore.offsets(Constantes.TOPIC, PARTITION)[0];
			}
			else {
				log.error("IBR >> Error: " + offsetsBefore.toString());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("IBR >> Error getLastOffset:" + e);
			e.printStackTrace();
		}
		return result; 
	}
	
	public static void main(String args[]) {
		try {
			log.info("IBR >> RUN!!!");
			new KafkaConsumer().run();
		} catch (Exception e) {
			log.error("Error:" + e);
		}
	}
}