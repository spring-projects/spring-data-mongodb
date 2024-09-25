/*
 * Copyright 2014-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
public class LineItem {

	String id;

	String caption;

	double price;

	int quantity = 1;

	@SuppressWarnings("unused")
	private LineItem() {
		this(null, null, 0.0, 0);
	}

	public LineItem(String id, String caption, double price) {
		this.id = id;
		this.caption = caption;
		this.price = price;
	}

	public LineItem(String id, String caption, double price, int quantity) {
		this(id, caption, price);
		this.quantity = quantity;
	}
}
