/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mongodb.core.mapping.MongoPath.AssociationPath;
import org.springframework.data.mongodb.core.mapping.MongoPath.MappedMongoPath;
import org.springframework.data.mongodb.core.mapping.MongoPath.MappedMongoPathImpl.MappedPropertySegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.PathSegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.PathSegment.PositionSegment;
import org.springframework.data.mongodb.core.mapping.MongoPath.PathSegment.PropertySegment;
import org.springframework.data.mongodb.core.mapping.Unwrapped.OnEmpty;
import org.springframework.data.mongodb.test.util.MongoTestMappingContext;

/**
 * Unit tests for {@link MongoPaths}
 * 
 * @author Christoph Strobl
 */
class MongoPathsUnitTests {

	MongoPaths paths;
	MongoTestMappingContext mappingContext;

	@BeforeEach
	void beforeEach() {

		mappingContext = MongoTestMappingContext.newTestContext();
		paths = new MongoPaths(mappingContext);
	}

	@Test // GH-4516
	void rawPathCaching() {

		MongoPath sourcePath = paths.create("inner.value.num");
		MongoPath samePathAgain = paths.create("inner.value.num");

		assertThat(sourcePath).isSameAs(samePathAgain);
	}

	@Test // GH-4516
	void mappedPathCaching() {

		MongoPath sourcePath = paths.create("inner.value.num");

		MappedMongoPath mappedPath = paths.mappedPath(sourcePath, Outer.class);
		MappedMongoPath pathMappedAgain = paths.mappedPath(sourcePath, Outer.class);
		assertThat(mappedPath).isSameAs(pathMappedAgain) //
				.isNotEqualTo(paths.mappedPath(sourcePath, Inner.class));
	}

	@Test // GH-4516
	void simplePath() {

		MongoPath mongoPath = paths.create("inner.value.num");

		assertThat(mongoPath.segments()).hasOnlyElementsOfType(PathSegment.PropertySegment.class);
		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);

		assertThat(mappedMongoPath.path()).isEqualTo("inner.val.f_val");
		assertThat(mappedMongoPath.segments()).hasOnlyElementsOfType(MappedPropertySegment.class);
		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.value.num", Outer.class));
	}

	@Test // GH-4516
	void mappedPathWithArrayPosition() {

		MongoPath mongoPath = paths.create("inner.valueList.0.num");

		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class,
				PositionSegment.class, PropertySegment.class);
		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);

		assertThat(mappedMongoPath.path()).isEqualTo("inner.valueList.0.f_val");
		assertThat(mappedMongoPath.segments()).hasExactlyElementsOfTypes(MappedPropertySegment.class,
				MappedPropertySegment.class, PositionSegment.class, MappedPropertySegment.class);
		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.valueList.num", Outer.class));
	}

	@Test // GH-4516
	void mappedPathWithReferenceToNonDomainTypeField() {

		MongoPath mongoPath = paths.create("inner.valueList.0.xxx");

		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class,
				PositionSegment.class, PropertySegment.class);
		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);

		assertThat(mappedMongoPath.path()).isEqualTo("inner.valueList.0.xxx");
		assertThat(mappedMongoPath.segments()).hasExactlyElementsOfTypes(MappedPropertySegment.class,
				MappedPropertySegment.class, PositionSegment.class, PropertySegment.class);
		assertThat(mappedMongoPath.propertyPath()).isNull();
	}

	@Test // GH-4516
	void mappedPathToPropertyWithinUnwrappedUnwrappedProperty() {

		MongoPath mongoPath = paths.create("inner.wrapper.v1");
		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class,
				PropertySegment.class);

		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);
		assertThat(mappedMongoPath.path()).isEqualTo("inner.pre-fix-v_1");

		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.wrapper.v1", Outer.class));
	}

	@Test // GH-4516
	void mappedPathToUnwrappedProperty() { // eg. for update mapping

		MongoPath mongoPath = paths.create("inner.wrapper");
		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class);

		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);
		assertThat(mappedMongoPath.path()).isEqualTo("inner");

		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.wrapper", Outer.class));
	}

	@Test // GH-4516
	void justPropertySegments() {

		MongoPath mongoPath = paths.create("inner.value");
		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class);

		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);
		assertThat(mappedMongoPath.path()).isEqualTo("inner.val");
		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.value", Outer.class));
	}

	@Test // GH-4516
	void withPositionalOperatorForUpdates() {

		MongoPath mongoPath = paths.create("inner.value.$");
		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class,
				PositionSegment.class);

		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);
		assertThat(mappedMongoPath.path()).isEqualTo("inner.val.$");
		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.value", Outer.class));
	}

	@Test // GH-4516
	void withProjectionOperatorForArray() {

		MongoPath mongoPath = paths.create("inner.value.$.num");
		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class,
				PositionSegment.class, PropertySegment.class);

		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);
		assertThat(mappedMongoPath.path()).isEqualTo("inner.val.$.f_val");
		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.value.num", Outer.class));
	}

	@Test // GH-4516
	void withAllPositionalOperatorForUpdates() {

		MongoPath mongoPath = paths.create("inner.value.$[].num");
		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class,
				PositionSegment.class, PropertySegment.class);

		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);
		assertThat(mappedMongoPath.path()).isEqualTo("inner.val.$[].f_val");
		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.value.num", Outer.class));
	}

	@Test // GH-4516
	void withNumericFilteredPositionalOperatorForUpdates() {

		MongoPath mongoPath = paths.create("inner.value.$[1].num");
		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class,
				PositionSegment.class, PropertySegment.class);

		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);
		assertThat(mappedMongoPath.path()).isEqualTo("inner.val.$[1].f_val");
		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.value.num", Outer.class));
	}

	@Test // GH-4516
	void withFilteredPositionalOperatorForUpdates() {

		MongoPath mongoPath = paths.create("inner.value.$[elem].num");
		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class,
				PositionSegment.class, PropertySegment.class);

		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);
		assertThat(mappedMongoPath.path()).isEqualTo("inner.val.$[elem].f_val");
		assertThat(mappedMongoPath.propertyPath()).isEqualTo(PropertyPath.from("inner.value.num", Outer.class));
	}

	@Test // GH-4516
	void unwrappedWithNonDomainTypeAndPathThatPointsToPropertyOfUnwrappedType() {

		MongoPath mongoPath = paths.create("inner.wrapper.document.v2");
		assertThat(mongoPath.segments()).hasExactlyElementsOfTypes(PropertySegment.class, PropertySegment.class,
				PropertySegment.class, PropertySegment.class);

		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);
		assertThat(mappedMongoPath.path()).isEqualTo("inner.pre-fix-document.v2");
		assertThat(mappedMongoPath.propertyPath()).isNull();
	}

	@Test // GH-4516
	void notAnAssociationPath() {

		MongoPath mongoPath = paths.create("inner.value");
		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);

		assertThat(mappedMongoPath.associationPath()).isNull();
	}

	@Test // GH-4516
	void rootAssociationPath() {

		MongoPath mongoPath = paths.create("ref");
		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);

		assertThat(mappedMongoPath.associationPath()).isNotNull().extracting(AssociationPath::propertyPath)
				.isEqualTo(PropertyPath.from("ref", Outer.class));
	}

	@Test // GH-4516
	void nestedAssociationPath() {

		MongoPath mongoPath = paths.create("inner.docRef");
		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);

		assertThat(mappedMongoPath.associationPath()).isNotNull().extracting(AssociationPath::propertyPath)
				.isEqualTo(PropertyPath.from("inner.docRef", Outer.class));
	}

	@Test // GH-4516
	void associationPathAsPartOfFullPath() {

		MongoPath mongoPath = paths.create("inner.docRef.id");
		MappedMongoPath mappedMongoPath = paths.mappedPath(mongoPath, Outer.class);

		assertThat(mappedMongoPath.associationPath()).isNotNull().satisfies(associationPath -> {
			assertThat(associationPath.propertyPath()).isEqualTo(PropertyPath.from("inner.docRef", Outer.class));
			assertThat(associationPath.targetPropertyPath()).isEqualTo(PropertyPath.from("inner.docRef.id", Outer.class));
			assertThat(associationPath.targetPath()).isEqualTo(mappedMongoPath);
		});
	}

	static class Outer {

		String id;
		Inner inner;

		@DBRef //
		Referenced ref;

	}

	static class Inner {

		@Field("val") //
		Value value;

		@Unwrapped(prefix = "pre-fix-", onEmpty = OnEmpty.USE_NULL) //
		Wrapper wrapper;

		List<Value> valueList;

		@DocumentReference //
		Referenced docRef;
	}

	static class Referenced {

		@Id String id;
		String value;
	}

	static class Wrapper {

		@Field("v_1") String v1;
		String v2;
		org.bson.Document document;
	}

	static class Value {

		String s_val;

		@Field("f_val") Float num;
	}
}
