package kr.co.admonster.collect.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import kr.co.admonster.collect.db.dao.BiddingTaskDao;
import kr.co.admonster.kafka.domain.BiddingResultMessage;
import kr.co.admonster.opensearch.domain.OsBiddingResult;
import kr.co.admonster.opensearch.naming.IndexType;
import kr.co.admonster.opensearch.service.OpenSearchService;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class BiddingResultService {
	
	private final BiddingTaskDao biddingTaskDao;
	
	private final OpenSearchService openSearchService;
	
	public BiddingResultService(
			BiddingTaskDao biddingTaskDao,
			OpenSearchService openSearchService) {
		this.biddingTaskDao = biddingTaskDao;
		
		this.openSearchService = openSearchService;
	}
	
	@Transactional
	public void apply(BiddingResultMessage resultMessage) {
		Map<String, Object> props = new HashMap<>();
		props.put("viewedRank", resultMessage.getViewedRank());
		props.put("currentBid", resultMessage.getFinalPrice());
		props.put("resultSt", resultMessage.getResultSt());
		props.put("resultDesc", resultMessage.getResultDesc());
		props.put("previousError", resultMessage.getUpdatedPreviousError());
		props.put("integralError", resultMessage.getUpdatedIntegralError());
		props.put("keywordId", resultMessage.getKeywordId());
		
		this.biddingTaskDao.updateResult(props);
		
		OsBiddingResult osBiddingResult = OsBiddingResult.builder()
				.keywordId(resultMessage.getKeywordId())
				.keyword(resultMessage.getKeyword())
				.displayUrl(resultMessage.getDisplayUrl())
				
				.deviceType(resultMessage.getDeviceType().name())
				.campaignType(resultMessage.getCampaignType().name())
				.biddingType(resultMessage.getBiddingType().name())
				
				.targetRank(resultMessage.getTargetRank())
				.viewedRank(resultMessage.getViewedRank())
				.viewedSlot(resultMessage.getViewedSlot())
				.newPrice(resultMessage.getFinalPrice())
				.oldPrice(resultMessage.getCurrentBid())
				
				.resultSt(resultMessage.getResultSt())
				.resultDesc(resultMessage.getResultDesc())
				
				.timestamps(resultMessage.getTimestamps())
				
				.accountNo(resultMessage.getAccountNo())
				.pidClusterId(resultMessage.getPidClusterId())
				.build();
				
		this.openSearchService.upsert(IndexType.BIDDING_RESULT, osBiddingResult, OsBiddingResult.class);
		log.info("Save {} with {}.", IndexType.BIDDING_RESULT, osBiddingResult);
	}
	
}
