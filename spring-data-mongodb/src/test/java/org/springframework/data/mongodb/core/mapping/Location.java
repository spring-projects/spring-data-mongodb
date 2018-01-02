/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
@Document(collection = "places")
public class Location {

	private ObjectId id;
	private double[] latlong;
	private int[] numbers;
	private float[] amounts;

	public Location(double[] latlong, int[] numbers, float[] amounts) {
		this.latlong = latlong;
		this.numbers = numbers;
		this.amounts = amounts;
	}

	public ObjectId getId() {
		return id;
	}

	public double[] getLatlong() {
		return latlong;
	}

	public void setLatlong(double[] latlong) {
		this.latlong = latlong;
	}

	public int[] getNumbers() {
		return numbers;
	}

	public void setNumbers(int[] numbers) {
		this.numbers = numbers;
	}

	public float[] getAmounts() {
		return amounts;
	}

	public void setAmounts(float[] amounts) {
		this.amounts = amounts;
	}
}
