/*
 * Copyright 2026-present the original author or authors.
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

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce.Variable;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for mapping {@code $match} criteria against the document shape produced by previous stages.
 *
 * @author Christoph Strobl
 */
class AggregationPathMappingUnitTests {

	MongoMappingContext mappingContext = new MongoMappingContext();
	AggregationOperationContext context = new TypeBasedAggregationOperationContext(Root.class, mappingContext,
			new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)));

	@Test // GH-4516
	void mapsNestedPathOfProjectedProperty() {
		assertThat(lastMatch(project("address"), match(where("address.street").is("x"))))
				.isEqualTo(new Document("address.s", "x"));
	}

	@Test // GH-4516
	void mapsNestedPathOfAliasedProperty() {
		assertThat(lastMatch(project().and("address").as("a"), match(where("a.street").is("x"))))
				.isEqualTo(new Document("a.s", "x"));
	}

	@Test // GH-4516
	void doesNotMapNestedPathOfComputedField() {
		assertThat(lastMatch(project().and(ArithmeticOperators.Add.valueOf("address.number").add(1)).as("address"),
				match(where("address.street").is("x")))).isEqualTo(new Document("address.street", "x"));
	}

	@Test // GH-4516
	void mapsGroupKeyToId() {

		assertThat(lastMatch(group("name").count().as("c"), match(where("name").is("x"))))
				.isEqualTo(new Document("_id", "x"));
		assertThat(lastMatch(group("name", "address.street").count().as("c"), match(where("street").is("x"))))
				.isEqualTo(new Document("_id.street", "x"));
	}

	@Test // GH-4516
	void mapsGroupKeyWithinLogicalOperators() {

		Document match = lastMatch(group("name").count().as("c"),
				match(new Criteria().orOperator(where("name").is("x"), where("c").gt(1))));

		assertThat(match).isEqualTo(new Document("$or",
				List.of(new Document("_id", "x"), new Document("c", new Document("$gt", 1)))));
	}

	@Test // GH-4516
	void stopsDomainMappingForFieldsOverwrittenByLookup() {
		assertThat(lastMatch(lookup("other", "name", "_id", "address"), match(where("address.street").is("x"))))
				.isEqualTo(new Document("address.street", "x"));
	}

	@Test // GH-4516
	void mapsNestedPathOfFieldSetToReference() {
		assertThat(lastMatch(AddFieldsOperation.addField("copy").withValueOf("address").build(),
				match(where("copy.street").is("x")))).isEqualTo(new Document("copy.s", "x"));
	}

	@Test // GH-4516
	void retainsDomainMappingForFieldsNotShadowed() {
		assertThat(lastMatch(lookup("other", "name", "_id", "joined"), match(where("address.street").is("x"))))
				.isEqualTo(new Document("addr.s", "x"));
	}

	@Test // GH-4516
	void mapsLookupLocalField() {

		Document lookup = render(lookup("other", "address.street", "address.street", "joined")).get("$lookup",
				Document.class);

		assertThat(lookup).containsEntry("localField", "addr.s").containsEntry("foreignField", "address.street");
	}

	@Test // GH-5011, GH-4516
	void stopsDomainMappingForAssociationOverwrittenByLookup() {

		assertThat(lastMatch(lookup("person", "person.$id", "_id", "person"), match(where("person.deleted").is(true))))
				.isEqualTo(new Document("person.deleted", true));
		assertThat(lastMatch(lookup("person", "docRef", "_id", "docRef"), match(where("docRef.deleted").is(true))))
				.isEqualTo(new Document("docRef.deleted", true));
	}

	@Test // GH-5011, GH-4516
	void retainsDbRefIdLocalField() {

		AggregationOperationContext strictContext = new TypeBasedAggregationOperationContext(Root.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)),
				FieldLookupPolicy.strict());

		for (AggregationOperationContext ctx : List.of(context, strictContext)) {

			List<Document> pipeline = newAggregation(Root.class, lookup("person", "person.$id", "_id", "p"),
					match(where("p.deleted").is(true))).toPipeline(ctx);

			assertThat(pipeline.get(0).get("$lookup", Document.class)).containsEntry("localField", "person.$id");
			assertThat(pipeline.get(1)).isEqualTo(new Document("$match", new Document("p.deleted", true)));
		}
	}

	@Test // GH-5011, GH-4516
	void retainsDbRefRefAndDbReferences() {

		AggregationOperationContext strictContext = new TypeBasedAggregationOperationContext(Root.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)),
				FieldLookupPolicy.strict());

		for (AggregationOperationContext ctx : List.of(context, strictContext)) {

			List<Document> pipeline = newAggregation(Root.class,
					project().and("person.$ref").as("collection").and("person.$db").as("database")).toPipeline(ctx);

			assertThat(pipeline.get(0).get("$project", Document.class)).containsEntry("collection", "$person.$ref")
					.containsEntry("database", "$person.$db");
		}
	}

	@Test // GH-5011, GH-4516
	void doesNotMapLookupPipelineAgainstOuterType() {

		Document lookup = render(LookupOperation.newLookup().from("other")
				.let(VariableOperators.Let.ExpressionVariable.newVariable("v").forField("name"))
				.pipeline(match(where("name").is("x")), project("address")).as("joined")).get("$lookup", Document.class);

		assertThat(lookup.getList("pipeline", Document.class)).containsExactly(new Document("$match",
				new Document("name", "x")), new Document("$project", new Document("address", 1)));
		assertThat(lookup.get("let")).isEqualTo(new Document("v", "$n"));
	}

	@Test // GH-5011, GH-4516
	void mapsForeignFieldForOwnCollectionOnly() {

		assertThat(render(lookup("root", "name", "address.street", "joined")).get("$lookup", Document.class))
				.containsEntry("localField", "n").containsEntry("foreignField", "addr.s");
		assertThat(render(lookup("other", "name", "address.street", "joined")).get("$lookup", Document.class))
				.containsEntry("localField", "n").containsEntry("foreignField", "address.street");
	}

	@Test // GH-5011, GH-4516
	void mapsForeignSideOfSelfLookupAgainstDomainType() {

		Document lookup = newAggregation(Root.class, project("name"), lookup("root", "name", "address.street", "joined"))
				.toPipeline(context).get(1).get("$lookup", Document.class);

		assertThat(lookup).containsEntry("localField", "name").containsEntry("foreignField", "addr.s");

		Document typed = newAggregation(Root.class, project("name"),
				LookupOperation.newLookup().from(Root.class).pipeline(match(where("address.street").is("x"))).as("joined"))
				.toPipeline(context).get(1).get("$lookup", Document.class);

		assertThat(typed.getList("pipeline", Document.class))
				.containsExactly(new Document("$match", new Document("addr.s", "x")));
	}

	@Test // GH-5011, GH-4516
	void mapsLookupPipelineAgainstOwnCollection() {

		Document lookup = render(LookupOperation.newLookup().from("root").pipeline(match(where("name").is("x"))).as("joined"))
				.get("$lookup", Document.class);

		assertThat(lookup.getList("pipeline", Document.class))
				.containsExactly(new Document("$match", new Document("n", "x")));
	}

	@Test // GH-5011, GH-4516
	void mapsForeignSideAgainstTypedFromCollection() {

		assertThat(render(lookup(Other.class, "name", "label", "joined")).get("$lookup", Document.class))
				.containsEntry("from", "other_collection").containsEntry("localField", "n").containsEntry("foreignField", "l");

		Document lookup = render(LookupOperation.newLookup().from(Other.class)
				.let(VariableOperators.Let.ExpressionVariable.newVariable("v").forField("name"))
				.pipeline(match(where("label").is("x"))).as("joined")).get("$lookup", Document.class);

		assertThat(lookup.getList("pipeline", Document.class))
				.containsExactly(new Document("$match", new Document("l", "x")));
		assertThat(lookup.get("let")).isEqualTo(new Document("v", "$n"));
	}

	@Test // GH-5011, GH-4516
	void inheritsLookupPolicyForTypedFromCollection() {

		AggregationOperationContext relaxedContext = new TypeBasedAggregationOperationContext(Root.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)),
				FieldLookupPolicy.relaxed());

		assertThat(newAggregation(Root.class, lookup(Other.class, "name", "nope", "joined")).toPipeline(relaxedContext)
				.get(0).get("$lookup", Document.class)).containsEntry("foreignField", "nope");
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(
				() -> newAggregation(Root.class, lookup(Other.class, "name", "nope", "joined")).toPipeline(context));
	}

	@Test // GH-5011, GH-4516
	void convertsValuesOfExposedReferences() {

		assertThat(lastMatch(SetOperation.set("code").toValueOf("code"), match(where("code").is(42))))
				.isEqualTo(new Document("code", "42"));
		assertThat(lastMatch(AddFieldsOperation.addField("code").withValueOf("code").build(), match(where("code").is(42))))
				.isEqualTo(new Document("code", "42"));
		assertThat(lastMatch(unwind("codes"), match(where("codes").is(42)))).isEqualTo(new Document("codes", "42"));
		assertThat(lastMatch(project("code"), match(where("code").is(42)))).isEqualTo(new Document("code", "42"));
		assertThat(lastMatch(project().and("code").as("alias"), match(where("alias").is(42))))
				.isEqualTo(new Document("alias", "42"));
		assertThat(lastMatch(group("code").count().as("c"), match(where("code").is(42))))
				.isEqualTo(new Document("_id", "42"));
	}

	@Test // GH-5011, GH-4516
	void retainsAssociationSemanticsOfExposedReferences() {

		assertThat(lastMatch(SetOperation.set("flag").toValueOf("docRef.deleted"), match(where("flag").is(true))))
				.isEqualTo(new Document("flag", true));
		assertThat(lastMatch(SetOperation.set("owner").toValueOf("docRef"), match(where("owner").is("abc"))))
				.isEqualTo(new Document("owner", "abc"));
	}

	@Test // GH-5011, GH-4516
	void doesNotConvertValuesOfComputedFields() {
		assertThat(lastMatch(project().and(ArithmeticOperators.Add.valueOf("code").add(1)).as("code"),
				match(where("code").is(42)))).isEqualTo(new Document("code", 42));
	}

	@Test // GH-5011, GH-4516
	void convertsValuesOfChainedReferences() {

		assertThat(lastMatch(SetOperation.set("copy").toValueOf("code"), project().and("copy").as("alias"),
				match(where("alias").is(42)))).isEqualTo(new Document("alias", "42"));
		assertThat(lastMatch(project().and("code").as("copy"), project().and("copy").as("alias"),
				match(where("alias").is(42)))).isEqualTo(new Document("alias", "42"));
	}

	@Test // GH-5011, GH-4516
	void convertsValuesOfUndeclaredFieldsOfCustomInheritingStage() {

		ExposedFields fields = ExposedFields.synthetic(Fields.fields("code"));

		assertThat(lastMatch(new InheritingStage(fields), match(where("code").is(42))))
				.isEqualTo(new Document("code", "42"));
	}

	@Test // GH-5011, GH-4516
	void convertsValuesOfDeclaredReferenceOfCustomStage() {

		ExposedFields fields = ExposedFields.synthetic(Fields.fields("alias")).withReference("alias", "code", false);

		assertThat(lastMatch(new CustomStage(fields), match(where("alias").is(42))))
				.isEqualTo(new Document("alias", "42"));
	}

	@Test // GH-4516
	void mapsVariablesOfFilterAndMapAgainstElementType() {

		assertThat(render(project().and(ArrayOperators.Filter.filter("addresses").as("a")
				.by(ComparisonOperators.valueOf("a.street").equalToValue("x"))).as("f")).toJson())
				.contains("\"$$a.s\"");
		assertThat(render(project().and(VariableOperators.mapItemsOf("addresses").as("a")
				.andApply(StringOperators.valueOf("a.street").toUpper())).as("m")).toJson()).contains("\"$$a.s\"");
	}

	@Test // GH-4516
	void mapsLetVariableReferencingField() {

		assertThat(render(project().and(VariableOperators.Let
				.define(VariableOperators.Let.ExpressionVariable.newVariable("ad").forField("address"))
				.andApply(StringOperators.valueOf("ad.street").toUpper())).as("l")).toJson()).contains("\"$$ad.s\"");
	}

	@Test // GH-4516
	void mapsReduceThisAgainstElementType() {

		Document project = render(project().and(ArrayOperators.arrayOf("addresses")
				.reduce(ArithmeticOperators.valueOf(Variable.VALUE.getName())
						.add(Variable.THIS.referringTo("number").getName()))
				.startingWith(0)).as("total"));

		assertThat(project).isEqualTo(Document.parse(
				"{ $project : { total : { $reduce : { input : '$addresses', initialValue : 0, in : { $add : ['$$value', '$$this.no'] } } } } }"));
	}

	@Test // GH-4516
	void doesNotMapReduceValue() {

		Document project = render(project().and(ArrayOperators.arrayOf("addresses")
				.reduce(ArithmeticOperators.valueOf(Variable.VALUE.referringTo("number").getName())
						.add(Variable.THIS.referringTo("number").getName()))
				.startingWith(new Document("number", 0))).as("total"));

		assertThat(project.toJson()).contains("\"$$value.number\"").contains("\"$$this.no\"");
	}

	@Test // GH-4516
	void mapsRawAggregationExpression() {

		Document project = render(project()
				.and(AggregationExpression.from(MongoExpression.create("{ $add : ['$address.number', 1] }"))).as("x"));

		assertThat(project).isEqualTo(Document.parse("{ $project : { x : { $add : ['$addr.no', 1] } } }"));
	}

	@Test // GH-5011, GH-4516
	void mapsTypedExprAgainstStageShape() {

		Criteria expr = Criteria.expr(ComparisonOperators.valueOf("address.street").equalToValue("x"));

		assertThat(lastMatch(lookup("other", "name", "_id", "joined"), match(expr)))
				.isEqualTo(Document.parse("{ $expr : { $eq : ['$addr.s', 'x'] } }"));
		assertThat(lastMatch(SetOperation.set("x").toValue(1), match(new Criteria().andOperator(expr))))
				.isEqualTo(Document.parse("{ $and : [ { $expr : { $eq : ['$addr.s', 'x'] } } ] }"));
		assertThat(lastMatch(project().and("address").as("a"),
				match(Criteria.expr(ComparisonOperators.valueOf("a.street").equalToValue("x")))))
				.isEqualTo(Document.parse("{ $expr : { $eq : ['$a.s', 'x'] } }"));
		assertThat(lastMatch(group("address").count().as("c"), match(expr)))
				.isEqualTo(Document.parse("{ $expr : { $eq : ['$_id.s', 'x'] } }"));
		assertThat(lastMatch(project().and(ArithmeticOperators.valueOf("name").add(1)).as("address"), match(expr)))
				.isEqualTo(Document.parse("{ $expr : { $eq : ['$address.street', 'x'] } }"));
	}

	@Test // GH-4516
	void retainsRawExprInMatch() {

		Document match = lastMatch(match(Criteria.expr(MongoExpression.create("{ $gt : ['$address.number', 1] }"))));

		assertThat(match).isEqualTo(Document.parse("{ $expr : { $gt : ['$address.number', 1] } }"));
	}

	private Document render(AggregationOperation operation) {
		return newAggregation(Root.class, operation).toPipeline(context).get(0);
	}

	private Document lastMatch(AggregationOperation... operations) {

		List<Document> pipeline = newAggregation(Root.class, operations).toPipeline(context);
		return pipeline.get(pipeline.size() - 1).get("$match", Document.class);
	}

	static class CustomStage implements FieldsExposingAggregationOperation {

		private final ExposedFields fields;

		CustomStage(ExposedFields fields) {
			this.fields = fields;
		}

		@Override
		public Document toDocument(AggregationOperationContext context) {
			return new Document("$custom", new Document());
		}

		@Override
		public ExposedFields getFields() {
			return fields;
		}
	}

	static class InheritingStage extends CustomStage
			implements FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation {

		InheritingStage(ExposedFields fields) {
			super(fields);
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document("root")
	static class Root {

		@Field("n") String name;
		@Field("addr") Address address;
		List<Address> addresses;
		@Field(targetType = FieldType.STRING) Integer code;
		@Field(targetType = FieldType.STRING) List<Integer> codes;
		@DBRef Person person;
		@DocumentReference Person docRef;
	}

	@org.springframework.data.mongodb.core.mapping.Document("other_collection")
	static class Other {

		@Field("l") String label;
	}

	static class Person {

		@Id String id;
		@Field("d") boolean deleted;
	}

	static class Address {

		@Field("s") String street;
		@Field("no") int number;
	}
}
