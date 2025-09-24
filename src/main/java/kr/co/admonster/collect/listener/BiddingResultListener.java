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
		
		log.info("Received message key={} partition={} offset={} value={}", key, partition, offset, value);
		
		try {
			this.biddingResultService.apply(record.value());
			ack.acknowledge();
			
			log.info("Successfully applied result keywordId={} offset={}", value.getKeywordId(), offset);
		} catch (Exception e) {
			 log.error("Failed to apply result key={} offset={}. Error={}", key, offset, e.getMessage(), e);
		}
	}
	
}
