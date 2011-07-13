/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.Arrays;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "newyork")
public class Venue {

	@Id
	private String id;
	private String name;
	private double[] location;

	@PersistenceConstructor
	Venue(String name, double[] location) {
		super();
		this.name = name;
		this.location = location;
	}

	public Venue(String name, double x, double y) {
		super();
		this.name = name;
		this.location = new double[] { x, y };
	}

	public String getName() {
		return name;
	}

	public double[] getLocation() {
		return location;
	}

	@Override
	public String toString() {
		return "Venue [id=" + id + ", name=" + name + ", location=" + Arrays.toString(location) + "]";
	}
}
