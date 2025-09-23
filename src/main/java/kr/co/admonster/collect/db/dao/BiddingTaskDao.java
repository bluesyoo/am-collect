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
				UPDATE tb_bidding_task
				SET
					current_rank = ?,
					current_bid = ?,
					result_st = ?,
					result_desc = ?,
					previous_error = ?,
					integral_error = ?,
					last_tm = UNIX_TIMESTAMP(),
					next_tm = UNIX_TIMESTAMP() + 300
				WHERE keyword_id = ?
				""";
		
		this.jdbcTemplate.update(updateQuery, new Object[]{
				resultMessage.getCurrentRank(),
				resultMessage.getNewBid(),
				resultMessage.getResultSt(),
				resultMessage.getResultDesc(),
				resultMessage.getUpdatedPreviousError(),
				resultMessage.getUpdatedIntegralError(),
				resultMessage.getKeywordId() });
	}
	
}
