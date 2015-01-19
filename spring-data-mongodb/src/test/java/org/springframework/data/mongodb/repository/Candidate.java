/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Sample Candidate domain class extending {@link Person}.
 * Candidate objects are stored in the {@link Person} collection.
 *
 * @author Mateusz Rasiñski
 */
@Document(collection = "person")
public class Candidate extends Person {

	private String title;

	public Candidate() {
	}

	public Candidate(String firstName, String lastName) {
		super(firstName, lastName);
	}

	public Candidate(String firstName, String lastName, String title) {
		super(firstName, lastName);
		this.title = title;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
}
