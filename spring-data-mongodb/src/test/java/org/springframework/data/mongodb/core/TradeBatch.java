package org.springframework.data.mongodb.core;

import java.util.List;

public class TradeBatch {

	private String batchId;

	private Trade[] trades;

	private List<Trade> tradeList;

	public String getBatchId() {
		return batchId;
	}

	public void setBatchId(String batchId) {
		this.batchId = batchId;
	}

	public Trade[] getTrades() {
		return trades;
	}

	public void setTrades(Trade[] trades) {
		this.trades = trades;
	}

	public List<Trade> getTradeList() {
		return tradeList;
	}

	public void setTradeList(List<Trade> tradeList) {
		this.tradeList = tradeList;
	}

}
