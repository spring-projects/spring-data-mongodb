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
package org.springframework.data.mongodb.core.mapping.path;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Unit tests for {@link ExpressionPathMapper}.
 *
 * @author Christoph Strobl
 */
class ExpressionPathMapperUnitTests {

	MongoMappingContext context = new MongoMappingContext();
	ExpressionPathMapper mapper = new ExpressionPathMapper(new MongoPathBinder(context));
	Scope root = Scope.of(context.getRequiredPersistentEntity(Root.class));

	@Test // GH-4516
	void mapsFieldPathValues() {
		assertThat(map("{ $add : ['$address.number', 1] }")).isEqualTo(Document.parse("{ $add : ['$addr.no', 1] }"));
	}

	@Test // GH-4516
	void doesNotMapKeys() {
		assertThat(map("{ $mergeObjects : [ { name : '$name' } ] }"))
				.isEqualTo(Document.parse("{ $mergeObjects : [ { name : '$n' } ] }"));
	}

	@Test // GH-4516
	void retainsLiteral() {
		assertThat(map("{ $literal : '$name' }")).isEqualTo(Document.parse("{ $literal : '$name' }"));
	}

	@Test // GH-4516
	void retainsUnresolvablePathsAndPlainStrings() {

		assertThat(map("{ $concat : ['$unknown.path', '$', 'text'] }"))
				.isEqualTo(Document.parse("{ $concat : ['$unknown.path', '$', 'text'] }"));
	}

	@Test // GH-4516
	void mapsMapAndFilterVariablesAgainstElementType() {

		assertThat(map("{ $map : { input : '$addresses', as : 'a', in : '$$a.street' } }"))
				.isEqualTo(Document.parse("{ $map : { input : '$addresses', as : 'a', in : '$$a.s' } }"));
		assertThat(map("{ $filter : { input : '$addresses', cond : { $eq : ['$$this.street', 'x'] } } }"))
				.isEqualTo(Document.parse("{ $filter : { input : '$addresses', cond : { $eq : ['$$this.s', 'x'] } } }"));
	}

	@Test // GH-4516
	void mapsReduceThisButNotValue() {

		assertThat(map("{ $reduce : { input : '$addresses', initialValue : 0, in : { $add : ['$$value', '$$this.number'] } } }"))
				.isEqualTo(Document.parse(
						"{ $reduce : { input : '$addresses', initialValue : 0, in : { $add : ['$$value', '$$this.no'] } } }"));
	}

	@Test // GH-4516
	void mapsLetVariablesReferencingFields() {

		assertThat(map("{ $let : { vars : { ad : '$address', n : 1 }, in : ['$$ad.street', '$$n.x'] } }")).isEqualTo(
				Document.parse("{ $let : { vars : { ad : '$addr', n : 1 }, in : ['$$ad.s', '$$n.x'] } }"));
	}

	@Test // GH-4516
	void mapsRootAndCurrentVariables() {

		assertThat(map("{ $toUpper : '$$ROOT.address.street' }"))
				.isEqualTo(Document.parse("{ $toUpper : '$$ROOT.addr.s' }"));
		assertThat(map("{ $toUpper : '$$CURRENT.name' }")).isEqualTo(Document.parse("{ $toUpper : '$$CURRENT.n' }"));
	}

	@Test // GH-4516
	void rendersDbRefIdAsStoredIdField() {
		assertThat(map("{ $eq : ['$ref.id', 1] }")).isEqualTo(Document.parse("{ $eq : ['$ref.$id', 1] }"));
	}

	@Test // GH-5011, GH-4516
	void rendersDbRefIdOfVariable() {

		assertThat(map("{ $let : { vars : { r : '$ref' }, in : ['$$r.id', '$$r.$id', '$$r'] } }"))
				.isEqualTo(Document.parse("{ $let : { vars : { r : '$ref' }, in : ['$$r.$id', '$$r.$id', '$$r'] } }"));
	}

	@Test // GH-4516
	void doesNotMapExpressionAsFieldName() {

		assertThat(mapper.mapFieldName("$name", null, root)).isNull();
		assertThat(mapper.mapFieldName("name", null, root)).isEqualTo("n");
		assertThat(mapper.mapFieldName("name", "$$CURRENT", root)).isEqualTo("n");
		assertThat(mapper.mapFieldName("street", "$address", root)).isEqualTo("s");
	}

	@Test // GH-5011, GH-4516
	void shadowsOuterValueVariableWithinReduce() {

		assertThat(map(
				"{ $let : { vars : { value : '$address' }, in : { $reduce : { input : '$addresses', initialValue : { street : '$$value.street' }, in : { street : { $add : ['$$value.street', '$$this.number'] } } } } } }"))
				.isEqualTo(Document.parse(
						"{ $let : { vars : { value : '$addr' }, in : { $reduce : { input : '$addresses', initialValue : { street : '$$value.s' }, in : { street : { $add : ['$$value.street', '$$this.no'] } } } } } }"));
	}

	@Test // GH-4516
	void restoresOuterVariablesAfterNestedScope() {

		assertThat(map(
				"{ $let : { vars : { a : '$name' }, in : [ { $let : { vars : { a : '$address' }, in : '$$a.street' } }, '$$a.street', { $map : { input : '$addresses', as : 'b', in : '$$b.street' } }, '$$b.street' ] } }"))
				.isEqualTo(Document.parse(
						"{ $let : { vars : { a : '$n' }, in : [ { $let : { vars : { a : '$addr' }, in : '$$a.s' } }, '$$a.street', { $map : { input : '$addresses', as : 'b', in : '$$b.s' } }, '$$b.street' ] } }"));
	}

	@Test // GH-5011, GH-4516
	void mapsFieldNameOfFieldOperators() {

		assertThat(map("{ $getField : { field : 'street', input : '$address' } }"))
				.isEqualTo(Document.parse("{ $getField : { field : 's', input : '$addr' } }"));
		assertThat(map("{ $setField : { field : 'street', input : '$address', value : '$name' } }"))
				.isEqualTo(Document.parse("{ $setField : { field : 's', input : '$addr', value : '$n' } }"));
		assertThat(map("{ $unsetField : { field : 'street', input : '$address' } }"))
				.isEqualTo(Document.parse("{ $unsetField : { field : 's', input : '$addr' } }"));
	}

	@Test // GH-5011, GH-4516
	void mapsFieldNameAgainstCurrentDocumentIfInputIsAbsentOrRoot() {

		assertThat(map("{ $getField : { field : 'name' } }")).isEqualTo(Document.parse("{ $getField : { field : 'n' } }"));
		assertThat(map("{ $getField : { field : 'name', input : '$$ROOT' } }"))
				.isEqualTo(Document.parse("{ $getField : { field : 'n', input : '$$ROOT' } }"));
		assertThat(map("{ $getField : 'name' }")).isEqualTo(Document.parse("{ $getField : 'n' }"));
	}

	@Test // GH-5011, GH-4516
	void mapsFieldNameToStoredKeyContainingDots() {
		assertThat(map("{ $getField : { field : 'dotted' } }"))
				.isEqualTo(Document.parse("{ $getField : { field : 'with.dots' } }"));
	}

	@Test // GH-5011, GH-4516
	void mapsFieldNameAgainstVariableInput() {

		assertThat(map("{ $filter : { input : '$addresses', as : 'a', cond : { $getField : { field : 'street', input : '$$a' } } } }"))
				.isEqualTo(Document.parse(
						"{ $filter : { input : '$addresses', as : 'a', cond : { $getField : { field : 's', input : '$$a' } } } }"));
		assertThat(map("{ $let : { vars : { v : '$address' }, in : { $getField : { field : 'street', input : '$$v' } } } }"))
				.isEqualTo(Document
						.parse("{ $let : { vars : { v : '$addr' }, in : { $getField : { field : 's', input : '$$v' } } } }"));
	}

	@Test // GH-5011, GH-4516
	void retainsFieldNameThatCannotBeResolved() {

		assertThat(map("{ $getField : { field : { $literal : '$a.b' }, input : '$$ROOT' } }"))
				.isEqualTo(Document.parse("{ $getField : { field : { $literal : '$a.b' }, input : '$$ROOT' } }"));
		assertThat(map("{ $getField : { field : '$name', input : '$$ROOT' } }"))
				.isEqualTo(Document.parse("{ $getField : { field : '$n', input : '$$ROOT' } }"));
		assertThat(map("{ $getField : { field : 'nope', input : '$address' } }"))
				.isEqualTo(Document.parse("{ $getField : { field : 'nope', input : '$addr' } }"));
		assertThat(map("{ $getField : { field : 'street', input : '$$zzz' } }"))
				.isEqualTo(Document.parse("{ $getField : { field : 'street', input : '$$zzz' } }"));
		assertThat(map("{ $getField : { field : 'street', input : { $mergeObjects : ['$address'] } } }"))
				.isEqualTo(Document.parse("{ $getField : { field : 'street', input : { $mergeObjects : ['$addr'] } } }"));
	}

	private Object map(String json) {
		return mapper.map(Document.parse(json), root);
	}

	static class Root {

		@Id String id;
		@Field("n") String name;
		@Field("addr") Address address;
		List<Address> addresses;
		@DBRef Root ref;
		@Field(name = "with.dots", nameType = FieldName.Type.KEY) String dotted;
	}

	static class Address {

		@Field("s") String street;
		@Field("no") int number;
	}
}
