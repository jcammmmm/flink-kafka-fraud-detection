package com.camilo.spendreport;

public class TaxiRideStats {
	private String driverId;
	private Double totalAggregate;
	
	public TaxiRideStats() {
		this.driverId = "";
		this.totalAggregate = new Double(0);
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

	public Double getTotalAggreage() {
		return totalAggregate;
	}

	public void setTotalAggreage(Double totalAggreage) {
		this.totalAggregate = totalAggreage;
	}

	@Override
	public String toString() {
		return "TaxiRideStats [driverId=" + driverId + ", totalAggreage=" + totalAggregate + "]";
	}
	
}
