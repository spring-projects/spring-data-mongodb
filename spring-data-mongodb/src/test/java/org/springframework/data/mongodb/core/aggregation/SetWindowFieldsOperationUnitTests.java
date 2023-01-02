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

import java.util.Date;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.SetWindowFieldsOperation.Windows;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link SetWindowFieldsOperation}.
 *
 * @author Christoph Strobl
 */
class SetWindowFieldsOperationUnitTests {

	@Test // GH-3711
	void rendersTargetFieldNamesCorrectly() {

		SetWindowFieldsOperation setWindowFieldsOperation = SetWindowFieldsOperation.builder() //
				.partitionByField("state") // resolves to field ref "$state"
				.sortBy(Sort.by(Direction.ASC, "date")) // resolves to "orderDate"
				.output(AccumulatorOperators.valueOf("qty").sum()) // resolves to "$quantity"
				.within(Windows.documents().fromUnbounded().toCurrent().build()) //
				.as("cumulativeQuantityForState") //
				.build(); //

		Document document = setWindowFieldsOperation.toDocument(contextFor(CakeSale.class));
		assertThat(document).isEqualTo(Document.parse(
				"{ $setWindowFields: { partitionBy: \"$state\", sortBy: { orderDate: 1 }, output: { cumulativeQuantityForState: { $sum: \"$quantity\", window: { documents: [ \"unbounded\", \"current\" ] } } } } }"));
	}

	@Test // GH-3711
	void exposesTargetFieldNames() {

		SetWindowFieldsOperation setWindowFieldsOperation = SetWindowFieldsOperation.builder() //
				.output(AccumulatorOperators.valueOf("qty").sum()) // resolves to "$quantity"
				.within(Windows.documents().fromUnbounded().toCurrent().build()) //
				.as("f1") //
				.output(AccumulatorOperators.valueOf("qty").avg()) // resolves to "$quantity"
				.within(Windows.documents().from(-1).to(0).build()) //
				.as("f2") //
				.build(); //

		assertThat(setWindowFieldsOperation.getFields()).map(ExposedField::getName).containsExactly("f1", "f2");
	}

	@Test // GH-3711
	void rendersMuiltipleOutputFields() {

		SetWindowFieldsOperation setWindowFieldsOperation = SetWindowFieldsOperation.builder() //
				.output(AccumulatorOperators.valueOf("qty").sum()) // resolves to "$quantity"
				.within(Windows.documents().fromUnbounded().toCurrent().build()) //
				.as("f1") //
				.output(AccumulatorOperators.valueOf("qty").avg()) // resolves to "$quantity"
				.within(Windows.documents().from(-1).to(0).build()) //
				.as("f2") //
				.build(); //

		Document document = setWindowFieldsOperation.toDocument(contextFor(CakeSale.class));
		assertThat(document).isEqualTo(Document.parse(
				"{ $setWindowFields: { output: { f1 : { $sum: \"$quantity\", window: { documents: [ \"unbounded\", \"current\" ] } }, f2 : { $avg: \"$quantity\", window: { documents: [ -1, 0 ] } } } } }"));
	}

	private static AggregationOperationContext contextFor(@Nullable Class<?> type) {

		if (type == null) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				new MongoMappingContext());
		mongoConverter.afterPropertiesSet();

		return new TypeBasedAggregationOperationContext(type, mongoConverter.getMappingContext(),
				new QueryMapper(mongoConverter));
	}

	static class CakeSale {

		String state;

		@Field("orderDate") Date date;

		@Field("quantity") Integer qty;

	}
}
