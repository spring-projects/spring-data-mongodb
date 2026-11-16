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
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.core.PropertyPath;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.DocumentReference;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.Unwrapped;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath.Match;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath.Rendering;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath.Segment;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;

/**
 * Unit tests for {@link MongoPathBinder}.
 *
 * @author Christoph Strobl
 */
class MongoPathBinderUnitTests {

	MongoMappingContext context = new MongoMappingContext();
	MongoPathBinder binder = new MongoPathBinder(context);
	Scope root = Scope.of(context.getRequiredPersistentEntity(Root.class));

	@Test // GH-4516
	void mapsPropertyToFieldName() {

		BoundMongoPath path = query("address.street");

		assertThat(path.toMappedPath()).isEqualTo("address.s");
		assertThat(path.isResolved()).isTrue();
		assertThat(path.leafProperty().getName()).isEqualTo("street");
		assertThat(path.properties()).extracting("name").containsExactly("address", "street");
	}

	@Test // GH-4516
	void resolvesByUniqueFieldName() {

		BoundMongoPath path = query("n");

		assertThat(path.toMappedPath()).isEqualTo("n");
		assertThat(path.segments()).singleElement().isInstanceOfSatisfying(Segment.Property.class, it -> {
			assertThat(it.property().getName()).isEqualTo("name");
			assertThat(it.match()).isEqualTo(Match.FIELD_NAME);
			assertThat(it.isMapped()).isTrue();
		});
	}

	@Test // GH-4516
	void prefersPropertyNameOverFieldName() {

		assertThat(query("a").toMappedPath()).isEqualTo("b");
		assertThat(query("b").toMappedPath()).isEqualTo("c");
	}

	@Test // GH-4516
	void doesNotSplitOnCamelCaseOrUnderscore() {

		assertThat(query("addressStreet").isResolved()).isFalse();
		assertThat(query("addressStreet").toMappedPath()).isEqualTo("addressStreet");

		assertThat(query("user_id").toMappedPath()).isEqualTo("user_id");
		assertThat(query("user_id").leafProperty().getName()).isEqualTo("user_id");
	}

	@Test // GH-4516
	void resolvesIdByPropertyAndFieldName() {

		assertThat(query("id").toMappedPath()).isEqualTo("_id");
		assertThat(query("_id").toMappedPath()).isEqualTo("_id");
		assertThat(((Segment.Property) query("_id").leaf()).isId()).isTrue();
	}

	@Test // GH-4516
	void traversesCollectionImplicitly() {
		assertThat(query("addresses.street").toMappedPath()).isEqualTo("addresses.s");
	}

	@Test // GH-4516
	void bindsListIndex() {

		BoundMongoPath path = query("addresses.0.street");

		assertThat(path.toMappedPath()).isEqualTo("addresses.0.s");
		assertThat(path.segments().get(1)).isEqualTo(new Segment.ListIndex("0", 0));
	}

	@Test // GH-4516
	void bindsListIndexesOfNestedLists() {
		assertThat(query("nested.0.1.street").toMappedPath()).isEqualTo("nested.0.1.s");
	}

	@Test // GH-4516
	void bindsMapKeysAndContinuesIntoValueType() {

		BoundMongoPath path = query("addressMap.key.street");

		assertThat(path.toMappedPath()).isEqualTo("addressMap.key.s");
		assertThat(path.segments().get(1)).isEqualTo(new Segment.MapKey("key"));

		assertThat(query("addressMap.0.street").segments().get(1)).isEqualTo(new Segment.MapKey("0"));
	}

	@Test // GH-4516
	void passesThroughBelowSimpleMapValue() {

		BoundMongoPath path = query("simpleMap.key.nested");

		assertThat(path.toMappedPath()).isEqualTo("simpleMap.key.nested");
		assertThat(path.segments().get(2)).isEqualTo(new Segment.Unresolved("nested"));
	}

	@Test // GH-4516
	void bindsPositionalOperators() {

		assertThat(update("addresses.$.street").toMappedPath()).isEqualTo("addresses.$.s");
		assertThat(update("addresses.$[].street").toMappedPath()).isEqualTo("addresses.$[].s");
		assertThat(update("addresses.$[elem].street").toMappedPath()).isEqualTo("addresses.$[elem].s");
		assertThat(update("nested.$[].$[x].street").toMappedPath()).isEqualTo("nested.$[].$[x].s");
	}

	@Test // GH-4516
	void passesThroughRemainderAfterUnresolvedSegment() {

		BoundMongoPath path = query("address.unknown.street");

		assertThat(path.isResolved()).isFalse();
		assertThat(path.toMappedPath()).isEqualTo("address.unknown.street");
		assertThat(path.segments()).hasSize(3).last().isEqualTo(new Segment.Unresolved("street"));
	}

	@Test // GH-4516
	void doesNotTraverseIntoClass() {
		assertThat(query("type.name").toMappedPath()).isEqualTo("type.name");
		assertThat(query("type.name").isResolved()).isFalse();
	}

	@Test // GH-4516
	void rendersDbRefIdAsReferenceItself() {

		assertThat(query("dbRef.id").toMappedPath()).isEqualTo("dbRef");
		assertThat(query("dbRef").association().getName()).isEqualTo("dbRef");
		assertThat(update("dbRefs.$").toMappedPath()).isEqualTo("dbRefs.$");
	}

	@Test // GH-4516
	void rejectsNonIdPropertyOfDbRef() {
		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> query("dbRef.name"));
	}

	@Test // GH-5011, GH-4516
	void bindsDbRefIdInAggregation() {

		BoundMongoPath path = aggregation("dbRef.$id");
		assertThat(path.isResolved()).isTrue();
		assertThat(path.toMappedPath(Rendering.EXPRESSION)).isEqualTo("dbRef.$id");
		assertThat(((Segment.Property) path.leaf()).isId()).isTrue();

		assertThat(aggregation("$dbRef.$id").toMappedPath(Rendering.EXPRESSION)).isEqualTo("$dbRef.$id");
		assertThat(aggregation("dbRefs.$id").toMappedPath(Rendering.EXPRESSION)).isEqualTo("dbRefs.$id");
		assertThat(binder.bind(MongoPath.parse("dbRef.$id", PathType.AGGREGATION), root, LookupPolicy.STRICT).isResolved())
				.isTrue();
	}

	@Test // GH-4516
	void derivesDefaultRenderingFromPathType() {

		assertThat(aggregation("dbRef.$id").toMappedPath()).isEqualTo("dbRef.$id");
		assertThat(aggregation("dbRef.$id").toRelativeMappedPath(1)).isEqualTo("$id");
		assertThat(aggregation("dbRef.$id").toMappedPath(Rendering.QUERY)).isEqualTo("dbRef");
		assertThat(query("dbRef.id").toMappedPath()).isEqualTo("dbRef");
		assertThat(query("dbRef.id").toMappedPath(Rendering.EXPRESSION)).isEqualTo("dbRef.$id");
	}

	@Test // GH-5011, GH-4516
	void bindsDbRefRefAndDbInAggregation() {

		for (String field : new String[] { "$ref", "$db" }) {

			BoundMongoPath path = aggregation("dbRef." + field);
			assertThat(path.isResolved()).isTrue();
			assertThat(path.leaf()).isEqualTo(new Segment.DbRefMember(field));
			assertThat(path.targetType().getType()).isEqualTo(String.class);
			assertThat(path.toMappedPath(Rendering.EXPRESSION)).isEqualTo("dbRef." + field);
			assertThat(aggregation("$dbRefs." + field).toMappedPath(Rendering.EXPRESSION)).isEqualTo("$dbRefs." + field);
			assertThat(
					binder.bind(MongoPath.parse("dbRef." + field, PathType.AGGREGATION), root, LookupPolicy.STRICT).isResolved())
					.isTrue();
		}
	}

	@Test // GH-5011, GH-4516
	void doesNotTraverseDbRefRef() {
		assertThat(aggregation("dbRef.$ref.length").isResolved()).isFalse();
	}

	@Test // GH-5011, GH-4516
	void doesNotBindDbRefIdOutsideOfDbRefOrAggregation() {

		assertThat(aggregation("docRef.$id").isResolved()).isFalse();
		assertThat(aggregation("address.$id").isResolved()).isFalse();
		assertThat(aggregation("dbRef.id.$id").isResolved()).isFalse();
		assertThat(aggregation("docRef.$ref").isResolved()).isFalse();
		assertThat(aggregation("address.$db").isResolved()).isFalse();
		assertThatExceptionOfType(MappingException.class).isThrownBy(
				() -> binder.bind(MongoPath.parse("docRef.$id", PathType.AGGREGATION), root, LookupPolicy.STRICT));

		assertThat(query("dbRef.$id").isResolved()).isFalse();
		assertThat(query("dbRef.$id").toMappedPath()).isEqualTo("dbRef.$id");
		assertThat(query("dbRef.$ref").isResolved()).isFalse();
		assertThat(query("dbRef.$ref").toMappedPath()).isEqualTo("dbRef.$ref");
	}

	@Test // GH-4516
	void bindsTargetPropertiesOfDocumentReference() {

		assertThat(query("docRef.id").toMappedPath()).isEqualTo("docRef");

		BoundMongoPath path = query("docRef.name");
		assertThat(path.toMappedPath()).isEqualTo("docRef.tn");
		assertThat(((Segment.Property) path.leaf()).isReferenced()).isTrue();
	}

	@Test // GH-4516
	void rendersUnwrappedPropertiesWithPrefix() {
		assertThat(query("unwrapped.street").toMappedPath()).isEqualTo("u_s");
	}

	@Test // GH-4516
	void allowsKeyFieldNameAsLeafOnly() {

		assertThat(query("dotted").toMappedPath()).isEqualTo("with.dots");
		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> query("dotted.street"));
	}

	@Test // GH-4516
	void strictPolicyFailsOnUnresolvedSegment() {

		MongoPath path = MongoPath.parse("address.unknown", PathType.QUERY);

		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> binder.bind(path, root, LookupPolicy.STRICT)).withMessageContaining("unknown");
	}

	@Test // GH-4516
	void strictPolicyFailsOnAmbiguousSegment() {

		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> binder.bind("0", PathType.QUERY, Scope.none(), LookupPolicy.STRICT))
				.withMessageContaining("'0'");
		assertThat(binder.bind("address.street", PathType.QUERY, root, LookupPolicy.STRICT).isResolved()).isTrue();
	}

	@Test // GH-4516
	void strictPolicyFailsOnFieldsNotExposed() {

		Scope exposed = Scope.exposing("name", Scope.Exposure.computed("name"));

		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> binder.bind("passwordHash", PathType.QUERY, exposed, LookupPolicy.STRICT))
				.withMessageContaining("'passwordHash'");
		assertThatExceptionOfType(MappingException.class)
				.isThrownBy(() -> binder.bind(".$where", PathType.QUERY, exposed, LookupPolicy.STRICT));
		assertThat(binder.bind("name", PathType.QUERY, exposed, LookupPolicy.STRICT).toMappedPath()).isEqualTo("name");
	}

	@Test // GH-4516
	void strictPolicyFailsWhenAppendingToUnresolvedPath() {

		BoundMongoPath parent = query("unknown");

		assertThatExceptionOfType(MappingException.class).isThrownBy(
				() -> binder.append(parent, MongoPath.parse("x", PathType.QUERY), LookupPolicy.STRICT));
	}

	@Test // GH-4516
	void noScopeResolvesNothing() {

		BoundMongoPath path = binder.bind("a.b", PathType.QUERY, Scope.none());

		assertThat(path.toMappedPath()).isEqualTo("a.b");
		assertThat(path.segments()).containsExactly(new Segment.Unresolved("a"), new Segment.Unresolved("b"));
		assertThat(binder.bind("0", PathType.QUERY, Scope.none()).segments())
				.containsExactly(new Segment.Ambiguous("0"));
	}

	@Test // GH-4516
	void bindsPropertyPathByPropertyNamesOnly() {

		BoundMongoPath path = binder.bind(MongoPath.from(PropertyPath.from("address.street", Root.class)), root);

		assertThat(path.toMappedPath()).isEqualTo("address.s");
		assertThat(path.source().isPropertyReference()).isTrue();
	}

	@Test // GH-4516
	void appendsToBoundPath() {

		BoundMongoPath parent = query("addressMap");
		BoundMongoPath path = binder.append(parent, MongoPath.parse("key.street", PathType.QUERY));

		assertThat(path.toMappedPath()).isEqualTo("addressMap.key.s");
		assertThat(path.source()).isEqualTo(MongoPath.parse("addressMap.key.street", PathType.QUERY));
	}

	@Test // GH-4516
	void rendersAggregationAnchors() {

		assertThat(binder.bind("$address.street", PathType.AGGREGATION, root).toMappedPath()).isEqualTo("$address.s");
	}

	@Test // GH-4516
	void cachesBoundPaths() {
		assertThat(query("address.street")).isSameAs(query("address.street"));
	}

	@Test // GH-4516
	void doesNotCacheOrResolveAgainstCustomScope() {

		Scope custom = new Scope() {};

		BoundMongoPath path = binder.bind("address", PathType.QUERY, custom);

		assertThat(path.segments()).containsExactly(new Segment.Unresolved("address"));
		assertThat(binder.bind("address", PathType.QUERY, custom)).isNotSameAs(path);
	}

	@Test // GH-4516
	void bindsLongPathsWithoutCaching() {

		String path = "address." + "x".repeat(MongoPathParser.MAX_CACHEABLE_PATH_LENGTH);

		assertThat(query(path)).isNotSameAs(query(path));
		assertThat(query(path).toMappedPath()).isEqualTo("address." + "x".repeat(MongoPathParser.MAX_CACHEABLE_PATH_LENGTH));
	}

	@Test // GH-4516
	void bindsLongVariablesWithoutCaching() {

		MongoPath path = MongoPath.parse("$$" + "x".repeat(MongoPathParser.MAX_CACHEABLE_PATH_LENGTH),
				PathType.AGGREGATION);

		assertThat(binder.bind(path, root)).isNotSameAs(binder.bind(path, root));
	}

	@Test // GH-4516
	void rendersEmptySegments() {

		assertThat(query("..name").toMappedPath()).isEqualTo("..name");
		assertThat(query("address..street").toMappedPath()).isEqualTo("address..street");
		assertThat(query(".$where").toMappedPath()).isEqualTo(".$where");
		assertThat(query("name.").toMappedPath()).isEqualTo("n.");
	}

	private BoundMongoPath query(String path) {
		return binder.bind(path, PathType.QUERY, root);
	}

	private BoundMongoPath update(String path) {
		return binder.bind(path, PathType.UPDATE, root);
	}

	private BoundMongoPath aggregation(String path) {
		return binder.bind(path, PathType.AGGREGATION, root);
	}

	static class Root {

		@Id String id;
		@Field("n") String name;
		Address address;
		List<Address> addresses;
		List<List<Address>> nested;
		Map<String, Address> addressMap;
		Map<String, String> simpleMap;
		@Field("b") String a;
		@Field("c") String b;
		@DBRef Target dbRef;
		@DBRef List<Target> dbRefs;
		@DocumentReference Target docRef;
		@Unwrapped.Nullable("u_") Address unwrapped;
		@Field(name = "with.dots", nameType = FieldName.Type.KEY) Address dotted;
		Class<?> type;
		String user_id;
	}

	static class Address {

		@Field("s") String street;
		String city;
	}

	static class Target {

		@Id String id;
		@Field("tn") String name;
	}
}
