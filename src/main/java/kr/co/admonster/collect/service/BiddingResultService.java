package kr.co.admonster.collect.service;

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
	public void save(BiddingResultMessage resultMessage) {
		this.biddingTaskDao.updateBiddingResult(resultMessage);
		
		OsBiddingResult osBiddingResult = OsBiddingResult.builder()
				.keywordId(resultMessage.getKeywordId())
				.pidClusterId(resultMessage.getPidClusterId())
				.biddingType(resultMessage.getBiddingType().getValue())
				
				.targetRank(resultMessage.getTargetRank())
				.currentRank(resultMessage.getCurrentRank())
				.newBid(resultMessage.getNewBid())
				.oldBid(resultMessage.getOldBid())
				
				.resultSt(resultMessage.getResultSt())
				.resultDesc(resultMessage.getResultDesc())
				
				.compatitorsRank(resultMessage.getCompetitorRanks())
				.timestamps(resultMessage.getTimestamps())
				.build();
				
		this.openSearchService.upsert(IndexType.BIDDING_RESULT, osBiddingResult, OsBiddingResult.class);
		log.info("Save {} with {}.", IndexType.BIDDING_RESULT, osBiddingResult);
	}
	
}
