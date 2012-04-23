package org.springframework.data.mongodb.core.mapping.event;

import javax.validation.constraints.Min;
import javax.validation.constraints.Size;

/**
 * Class used to test JSR-303 validation @{link org.springframework.data.mongodb.core.mapping.event.BeforeSaveValidator}
 *
 * @author Maciej Walkowiak
 */
public class User {
	@Size(min = 10)
	private String name;

	@Min(18)
	private Integer age;

	public User(String name, Integer age) {
		this.name = name;
		this.age = age;
	}

	public String getName() {
		return name;
	}

	public Integer getAge() {
		return age;
	}
}
