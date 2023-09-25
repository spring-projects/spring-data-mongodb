/*
 * Copyright 2018-2023 the original author or authors.
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

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.aggregation.ObjectOperators.MergeObjects;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Unit tests for {@link ObjectOperators}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @currentRead Royal Assassin - Robin Hobb
 */
public class ObjectOperatorsUnitTests {

	static final String EXPRESSION_STRING = "{ \"$king-in-waiting\" : \"verity\" }";
	static final Document EXPRESSION_DOC = Document.parse(EXPRESSION_STRING);
	static final AggregationExpression EXPRESSION = context -> EXPRESSION_DOC;

	@Test // DATAMONGO-2053
	public void mergeSingleFieldReference() {

		assertThat(ObjectOperators.valueOf("kettricken").merge().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $mergeObjects: \"$kettricken\" } "));
	}

	@Test // DATAMONGO-2053
	public void mergeSingleExpression() {

		assertThat(ObjectOperators.valueOf(EXPRESSION).merge().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $mergeObjects: " + EXPRESSION_STRING + " } "));
	}

	@Test // DATAMONGO-2053
	public void mergeEmpty() {

		assertThat(MergeObjects.merge().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $mergeObjects: [] } "));
	}

	@Test // DATAMONGO-2053
	public void mergeMuliFieldReference() {

		assertThat(
				ObjectOperators.valueOf("kettricken").mergeWithValuesOf("verity").toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo(Document.parse("{ $mergeObjects: [ \"$kettricken\", \"$verity\" ] } "));
	}

	@Test // DATAMONGO-2053
	public void mergeMixed() {

		assertThat(ObjectOperators.valueOf("kettricken").mergeWithValuesOf(EXPRESSION).mergeWithValuesOf("verity")
				.toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo(
						Document.parse("{ $mergeObjects: [ \"$kettricken\", " + EXPRESSION_STRING + ", \"$verity\" ] } "));
	}

	@Test // DATAMONGO-2053
	public void mergeWithSystemVariable() {

		assertThat(
				ObjectOperators.valueOf(EXPRESSION).mergeWith(SystemVariable.ROOT).toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo(Document.parse("{ $mergeObjects: [ " + EXPRESSION_STRING + ", \"$$ROOT\" ] } "));
	}

	@Test // DATAMONGO-2053
	public void mergeMany() {

		assertThat(ObjectOperators.valueOf("kettricken").mergeWithValuesOf(EXPRESSION)
				.mergeWith(new Document("fitz", "chivalry")).toDocument(Aggregation.DEFAULT_CONTEXT))
						.isEqualTo(Document.parse(
								"{ $mergeObjects: [ \"$kettricken\", " + EXPRESSION_STRING + ", { \"fitz\" : \"chivalry\" } ] } "));
	}

	@Test // DATAMONGO-2052
	public void toArrayWithFieldReference() {

		assertThat(ObjectOperators.valueOf("verity").toArray().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $objectToArray : \"$verity\" }"));
	}

	@Test // DATAMONGO-2052
	public void toArrayWithExpression() {

		assertThat(ObjectOperators.valueOf(EXPRESSION).toArray().toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $objectToArray : " + EXPRESSION_STRING + " }"));
	}

	@Test // GH-4139
	public void getField() {

		assertThat(ObjectOperators.valueOf("batman").getField("robin").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $getField : { field : \"robin\", input : \"$batman\" }}"));
	}

	@Test // GH-4464
	public void getFieldOfCurrent() {

		assertThat(ObjectOperators.valueOf(Aggregation.CURRENT).getField("robin").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $getField : { field : \"robin\", input : \"$$CURRENT\" }}"));
	}

	@Test // GH-4464
	public void getFieldOfMappedKey() {

		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, new MongoMappingContext());
		converter.afterPropertiesSet();

		assertThat(ObjectOperators.getValueOf("population").toDocument(new RelaxedTypeBasedAggregationOperationContext(ZipInfo.class, converter.getMappingContext(), new QueryMapper(converter))))
				.isEqualTo(Document.parse("{ $getField : { field :  \"pop\", input : \"$$CURRENT\" } }"));
	}

	@Test // GH-4139
	public void setField() {

		assertThat(ObjectOperators.valueOf("batman").setField("friend").toValue("robin").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $setField : { field : \"friend\", value : \"robin\", input : \"$batman\" }}"));
	}

	@Test // GH-4464
	public void setFieldOfMappedKey() {

		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, new MongoMappingContext());
		converter.afterPropertiesSet();

		assertThat(ObjectOperators.setValueTo("population", "robin").toDocument(new RelaxedTypeBasedAggregationOperationContext(ZipInfo.class, converter.getMappingContext(), new QueryMapper(converter))))
				.isEqualTo(Document.parse("{ $setField : { field : \"pop\", value : \"robin\", input : \"$$CURRENT\" }}"));
	}

	@Test // GH-4139
	public void removeField() {

		assertThat(ObjectOperators.valueOf("batman").removeField("joker").toDocument(Aggregation.DEFAULT_CONTEXT))
				.isEqualTo(Document.parse("{ $setField : { field : \"joker\", value : \"$$REMOVE\", input : \"$batman\" }}"));
	}

}
