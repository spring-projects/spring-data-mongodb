package org.springframework.data.mongodb.crossstore.test;

public class Address {
	
	private Integer streetNumber;
	private String streetName;
	private String city;
	private String state;
	private String zip;
	
	public Address(Integer streetNumber, String streetName, String city,
			String state, String zip) {
		super();
		this.streetNumber = streetNumber;
		this.streetName = streetName;
		this.city = city;
		this.state = state;
		this.zip = zip;
	}
	
	public Integer getStreetNumber() {
		return streetNumber;
	}
	public void setStreetNumber(Integer streetNumber) {
		this.streetNumber = streetNumber;
	}
	public String getStreetName() {
		return streetName;
	}
	public void setStreetName(String streetName) {
		this.streetName = streetName;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getZip() {
		return zip;
	}
	public void setZip(String zip) {
		this.zip = zip;
	}
	
	
	
}
