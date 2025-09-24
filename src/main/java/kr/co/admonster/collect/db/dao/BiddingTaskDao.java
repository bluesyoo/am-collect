package kr.co.admonster.collect.db.dao;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import kr.co.admonster.common.constants.Constants;
import lombok.extern.slf4j.Slf4j;

@Repository
@Slf4j
public class BiddingTaskDao {

	private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	public BiddingTaskDao(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
		this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
	}
	
	private final AtomicLong lastTm = new AtomicLong(0);
	
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
		
		long now = System.currentTimeMillis();
		if (now - this.lastTm.get() > Constants.MILLISECOND_FOR_HOUR) {
			log.info("Query: {}", query);
			log.info("Parameters: {}", props.entrySet().stream()
					.map(e -> e.getKey() + "=" + e.getValue())
					.collect(Collectors.joining(", ")));
			this.lastTm.set(now);
		}
		
		int updated = this.namedParameterJdbcTemplate.update(query, props);
		
		if (updated == 0) {
			log.error("Failed to update bidding_task. keyword_id={}", props.get("keyword_id"));
		} else {
			log.info("Updated bidding_task successfully. keyword_id={}, updatedRows={}", props.get("keyword_id"), updated);
		}
	}
	
}
