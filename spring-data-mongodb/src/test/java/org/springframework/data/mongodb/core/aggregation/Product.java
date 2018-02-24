/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

/**
 * @author Thomas Darimont
 */
public class Product {
	String id;
	String name;
	double netPrice;
	int spaceUnits;
	double discountRate;
	double taxRate;

	public Product() {}

	public Product(String id, String name, double netPrice, int spaceUnits, double discountRate, double taxRate) {
		this.id = id;
		this.name = name;
		this.netPrice = netPrice;
		this.spaceUnits = spaceUnits;
		this.discountRate = discountRate;
		this.taxRate = taxRate;
	}
}
