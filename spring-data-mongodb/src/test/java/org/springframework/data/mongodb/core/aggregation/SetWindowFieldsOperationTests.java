/*
 * Copyright 2021-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.aggregation.DateOperators.Year;
import org.springframework.data.mongodb.core.aggregation.SetWindowFieldsOperation.Windows;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

/**
 * Integration tests for {@link SetWindowFieldsOperation}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MongoTemplateExtension.class)
@EnableIfMongoServerVersion(isGreaterThanEqual = "5.0")
class SetWindowFieldsOperationTests {

	@Template //
	private static MongoTestTemplate mongoTemplate;

	@AfterEach
	void afterEach() {
		mongoTemplate.flush(CakeSale.class);
	}

	@Test // GH-3711
	void executesSetWindowFieldsOperationCorrectly() {

		initCakeSales();

		SetWindowFieldsOperation setWindowFieldsOperation = SetWindowFieldsOperation.builder() //
				.partitionByField("state") // resolves to field ref "$state"
				.sortBy(Sort.by(Direction.ASC, "date")) // resolves to "orderDate"
				.output(AccumulatorOperators.valueOf("qty").sum()) // resolves to "$quantity"
				.within(Windows.documents().fromUnbounded().toCurrent().build()) //
				.as("cumulativeQuantityForState") //
				.build(); //

		AggregationResults<Document> results = mongoTemplate.aggregateAndReturn(Document.class)
				.by(Aggregation.newAggregation(CakeSale.class, setWindowFieldsOperation)).all();

		assertThat(results.getMappedResults()).map(it -> it.get("cumulativeQuantityForState")).contains(162, 282, 427, 134,
				238, 378);
	}

	@Test // GH-3711
	void executesSetWindowFieldsOperationWithPartitionExpressionCorrectly() {

		initCakeSales();

		SetWindowFieldsOperation setWindowFieldsOperation = SetWindowFieldsOperation.builder() //
				.partitionByExpression(Year.yearOf("date")) // resolves to $year: "$orderDate"
				.sortBy(Sort.by(Direction.ASC, "date")) // resolves to "orderDate"
				.output(AccumulatorOperators.valueOf("qty").sum()) // resolves to "$quantity"
				.within(Windows.documents().fromUnbounded().toCurrent().build()) //
				.as("cumulativeQuantityForState") //
				.build(); //

		AggregationResults<Document> results = mongoTemplate.aggregateAndReturn(Document.class)
				.by(Aggregation.newAggregation(CakeSale.class, setWindowFieldsOperation)).all();

		assertThat(results.getMappedResults()).map(it -> it.get("cumulativeQuantityForState")).contains(134, 296, 104, 224,
				145, 285);
	}

	void initCakeSales() {

		mongoTemplate.execute(CakeSale.class, collection -> {

			List<Document> source = Arrays.asList(Document.parse(
					"{ _id: 0, type: \"chocolate\", orderDate: { $date : \"2020-05-18T14:10:30Z\" }, state: \"CA\", price: 13, quantity: 120 }"),
					Document.parse(
							"{ _id: 1, type: \"chocolate\", orderDate: { $date : \"2021-03-20T11:30:05Z\"}, state: \"WA\", price: 14, quantity: 140 }"),
					Document.parse(
							"{ _id: 2, type: \"vanilla\", orderDate: { $date : \"2021-01-11T06:31:15Z\"}, state: \"CA\", price: 12, quantity: 145 }"),
					Document.parse(
							"{ _id: 3, type: \"vanilla\", orderDate: { $date : \"2020-02-08T13:13:23Z\"}, state: \"WA\", price: 13, quantity: 104 }"),
					Document.parse(
							"{ _id: 4, type: \"strawberry\", orderDate: { $date : \"2019-05-18T16:09:01Z\"}, state: \"CA\", price: 41, quantity: 162 }"),
					Document.parse(
							"{ _id: 5, type: \"strawberry\", orderDate: { $date : \"2019-01-08T06:12:03Z\"}, state: \"WA\", price: 43, quantity: 134 }"));

			collection.insertMany(source);
			return "OK";
		});
	}

	static class CakeSale {

		@Id Integer id;

		String state;

		@Field("orderDate") //
		Date date;

		@Field("quantity") //
		Integer qty;

		String type;

		public Integer getId() {
			return this.id;
		}

		public String getState() {
			return this.state;
		}

		public Date getDate() {
			return this.date;
		}

		public Integer getQty() {
			return this.qty;
		}

		public String getType() {
			return this.type;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public void setState(String state) {
			this.state = state;
		}

		public void setDate(Date date) {
			this.date = date;
		}

		public void setQty(Integer qty) {
			this.qty = qty;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String toString() {
			return "SetWindowFieldsOperationTests.CakeSale(id=" + this.getId() + ", state=" + this.getState() + ", date="
					+ this.getDate() + ", qty=" + this.getQty() + ", type=" + this.getType() + ")";
		}
	}

}
