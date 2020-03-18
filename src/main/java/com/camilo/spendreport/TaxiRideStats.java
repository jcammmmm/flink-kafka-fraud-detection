package com.camilo.spendreport;

public class TaxiRideStats {
	private String driverId;
	private Double totalAggregate;
	private Long counter;
	private Long lastModified;
	
	public TaxiRideStats() {
		this.driverId = "";
		this.totalAggregate = new Double(0);
		this.counter = new Long(0L);
		this.setLastModified(0L);
	}
	
	public TaxiRideStats(String driverId, Double totalAggregate, Long counter, Long lastModified) {
		super();
		this.driverId = driverId;
		this.totalAggregate = totalAggregate;
		this.counter = counter;
		this.lastModified = lastModified;
	}



	public TaxiRideStats(String driverId, Double totalAggreage) {
		this.driverId = driverId;
		this.totalAggregate = totalAggreage;
	}

	public String getDriverId() {
		return driverId;
	}

	public void setDriverId(String driverId) {
		this.driverId = driverId;
	}

	public Double getTotalAggregate() {
		return totalAggregate;
	}

	public void setTotalAggregate(Double totalAggregate) {
		this.totalAggregate = totalAggregate;
	}
	
	public void updateTotalAggregate(Double value) {
		this.totalAggregate += value;
	}

	public Long getCounter() {
		return counter;
	}

	public void setCounter(Long counter) {
		this.counter = counter;
	}
	
	public void countOne() {
		this.counter++;
	}

	public Long getLastModified() {
		return lastModified;
	}

	public void setLastModified(Long lastModified) {
		this.lastModified = lastModified;
	}

	@Override
	public String toString() {
		return "TaxiRideStats [driverId=" + driverId + ", totalAggregate=" + totalAggregate + ", counter=" + counter
				+ ", lastModified=" + lastModified + "]";
	}

	
}
