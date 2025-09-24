package kr.co.admonster.collect.db.dao;

import java.util.Map;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import lombok.extern.slf4j.Slf4j;

@Repository
@Slf4j
public class BiddingTaskDao {

	private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	public BiddingTaskDao(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
		this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
	}
	
	public void updateResult(Map<String, Object> props) {
		String query = """
				UPDATE tb_bidding_task
				SET
					viewed_rank = :viewedRank,
					current_bid = :currentBid,
					result_st = :resultSt,
					result_desc = :resultDesc,
					previous_error = :previousError,
					integral_error = :integralError,
					last_tm = UNIX_TIMESTAMP(),
					next_tm = UNIX_TIMESTAMP() + 300
				WHERE keyword_id = :keywordId
				""";
		
		log.debug("Executing updateResult query: {}", query);
		log.debug("Parameters={}", props);
		
		int updated = this.namedParameterJdbcTemplate.update(query, props);
		
		if (updated == 0) {
			log.error("Failed to update bidding_task. keywordId={}", props.get("keywordId"));
		} else {
			log.info("Successfully updated bidding_task. keywordId={}", props.get("keywordId"));
		}
	}
	
}
