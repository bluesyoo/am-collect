package kr.co.admonster.collect.db.dao;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import kr.co.admonster.kafka.domain.BiddingResultMessage;

@Repository
public class BiddingTaskDao {
	
	private final JdbcTemplate jdbcTemplate;
	
	public BiddingTaskDao(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
	public void updateBiddingResult(BiddingResultMessage resultMessage) {
		String updateQuery = """
				UPDATE tb_bidding
				SET
					PRICE_CUR = ?,
					ORDER_CUR = ?,
					RST_TP = ?,
					RST_DTL = ?,
					PREVIOUS_ERROR = ?,
					INTEGRAL_ERROR = ?,
					NEXT_TM = UNIX_TIMESTAMP() + 300
				WHERE KEYWORD_ID = ?
				""";
		
		this.jdbcTemplate.update(updateQuery, new Object[]{
				resultMessage.getNewBid(),
				resultMessage.getCurrentRank(),
				resultMessage.getResultSt(),
				resultMessage.getResultDesc(),
				resultMessage.getUpdatedPreviousError(),
				resultMessage.getUpdatedIntegralError(),
				resultMessage.getKeywordId() });
	}
	
}
