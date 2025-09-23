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
		log.info("Collect message: {}", record.value());
		try {
			this.biddingResultService.save(record.value());
			ack.acknowledge();
		} catch (Exception e) {
			log.error("Failed to process task {}. {}", record.value(), e.getMessage(), e);
		}
	}
	
}
