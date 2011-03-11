package org.springframework.persistence.document.test;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Account {
	
	@Id private Long id;

    private String name;

    private float balance;

    private String friendlyName;

    private String whatever;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public float getBalance() {
		return balance;
	}

	public void setBalance(float balance) {
		this.balance = balance;
	}

	public String getFriendlyName() {
		return friendlyName;
	}

	public void setFriendlyName(String friendlyName) {
		this.friendlyName = friendlyName;
	}

	public String getWhatever() {
		return whatever;
	}

	public void setWhatever(String whatever) {
		this.whatever = whatever;
	}

	@Override
	public String toString() {
		return "Account [id=" + id + ", name=" + name + ", balance=" + balance
				+ ", friendlyName=" + friendlyName + "]";
	}    

}
