package kr.co.admonster.collect.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import kr.co.admonster.collect.service.BiddingResultService;
import kr.co.admonster.kafka.domain.BiddingResultMessage;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BiddingResultListener {
	
	private final BiddingResultService biddingResultService;
	
	public BiddingResultListener(BiddingResultService biddingResultService) {
		this.biddingResultService = biddingResultService;
	}
	
	@KafkaListener(
			id = "bidding-result-listener",
			topics = "#{@kafkaTopicProvider.BIDDING_RESULT}",
			groupId = "am-collect",
			containerFactory = "kafkaListenerContainerFactory")
	public void onMessage(ConsumerRecord<String, BiddingResultMessage> record, Acknowledgment ack) {
		String key = record.key();
		BiddingResultMessage value = record.value();
		
		int partition = record.partition();
		long offset = record.offset();
		
		log.info("Received bidding_result message. key={} partition={} offset={}", key, partition, offset);
		log.debug("Message value={}", value);
		
		try {
			this.biddingResultService.apply(record.value());
			ack.acknowledge();
			
			log.info("Applied bidding_result successfully. keyword_id={} offset={}", value.getKeywordId(), offset);
		} catch (Exception e) {
			log.error("Failed to apply bidding_result. key={} offset={}", key, offset, e);
		}
	}
	
}
