/*
 * Copyright 2014 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @author Thomas Darimont
 */
public class Order {

	final String id;

	final String customerId;

	final Date orderDate;

	final List<LineItem> items;

	public Order(String id, String customerId, Date orderDate) {
		this(id, customerId, orderDate, new ArrayList<LineItem>());
	}

	public Order(String id, String customerId, Date orderDate, List<LineItem> items) {
		this.id = id;
		this.customerId = customerId;
		this.orderDate = orderDate;
		this.items = items;
	}

	public Order addItem(LineItem item) {

		List<LineItem> newItems = new ArrayList<LineItem>(items != null ? items : Collections.<LineItem> emptyList());
		newItems.add(item);

		return new Order(id, customerId, orderDate, newItems);
	}

	public String getId() {
		return id;
	}

	public String getCustomerId() {
		return customerId;
	}

	public Date getOrderDate() {
		return orderDate;
	}

	public List<LineItem> getItems() {
		return items;
	}
}
