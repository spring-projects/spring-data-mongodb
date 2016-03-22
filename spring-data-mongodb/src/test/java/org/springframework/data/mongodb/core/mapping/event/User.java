/*
 * Copyright 2012-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapping.event;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.Min;
import javax.validation.constraints.Size;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.event.User.AddressRelation.Type;

/**
 * Class used to test JSR-303 validation
 * {@link org.springframework.data.mongodb.core.mapping.event.ValidatingMongoEventListener}
 * 
 * @see DATAMONGO-36
 * @author Maciej Walkowiak
 * @author Paul Sterl
 */
@Document(collection = "USER")
public class User {
	
	public static class AddressRelation {
		public enum Type {
			PRIVATE,
			WORK
		}
		private Type type = Type.PRIVATE;
		@DBRef(lazy = true)
		private Address address;
		// needed otherwise spring doesn't create the proxy for some reason ...
		public AddressRelation() {
			super();
		}
		public AddressRelation(Type type, Address address) {
			this();
			this.type = type;
			this.address = address;
		}
		public Type getType() {
			return type;
		}
		public void setType(Type type) {
			this.type = type;
		}
		public Address getAddress() {
			return address;
		}
		public void setAddress(Address address) {
			this.address = address;
		}
	}

	@Id
	private ObjectId id;

	@Size(min = 10)
	private String name;

	@Min(18)
	private Integer age;
	
	private List<AddressRelation> addresses = new ArrayList<AddressRelation>();

	public User() {
		super();
	}
	public User(String name, Integer age) {
		this();
		this.name = name;
		this.age = age;
	}
	public User(String name, Integer age, Address...addresses) {
		this(name, age);
		for (Address address : addresses) {
			this.addresses.add(new AddressRelation(Type.WORK, address));
		}
	}

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public Integer getAge() {
		return age;
	}
	public List<AddressRelation> getAddresses() {
		return addresses;
	}
	public void setAddresses(List<AddressRelation> addresses) {
		this.addresses = addresses;
	}
}
