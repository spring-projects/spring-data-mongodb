/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.convert.LazyLoadingProxy;
import org.springframework.data.mongodb.core.convert.LazyLoadingTestUtils;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

import com.mongodb.client.model.Filters;

/**
 * {@link org.springframework.data.mongodb.core.mapping.DBRef} related integration tests for
 * {@link org.springframework.data.mongodb.core.MongoTemplate}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MongoTemplateExtension.class)
public class MongoTemplateDbRefTests {

	@Template(database = "mongo-template-dbref-tests",
			initialEntitySet = { RefCycleLoadingIntoDifferentTypeRoot.class,
					RefCycleLoadingIntoDifferentTypeIntermediate.class, RefCycleLoadingIntoDifferentTypeRootView.class,
					WithDBRefOnRawStringId.class, WithLazyDBRefOnRawStringId.class, WithRefToAnotherDb.class,
					WithLazyRefToAnotherDb.class, WithListRefToAnotherDb.class, WithLazyListRefToAnotherDb.class }) //
	static MongoTestTemplate template;

	@Template(database = "mongo-template-dbref-tests-other-db", initialEntitySet = JustSomeType.class) //
	static MongoTestTemplate otherDbTemplate;

	@BeforeEach
	public void setUp() {

		template.flush();
		otherDbTemplate.flush();
	}

	@Test // DATAMONGO-1703
	public void shouldLoadRefIntoDifferentTypeCorrectly() {

		// init root
		RefCycleLoadingIntoDifferentTypeRoot root = new RefCycleLoadingIntoDifferentTypeRoot();
		root.id = "root-1";
		root.content = "jon snow";
		template.save(root);

		// init one and set view id ref to root.id
		RefCycleLoadingIntoDifferentTypeIntermediate intermediate = new RefCycleLoadingIntoDifferentTypeIntermediate();
		intermediate.id = "one-1";
		intermediate.refToRootView = new RefCycleLoadingIntoDifferentTypeRootView();
		intermediate.refToRootView.id = root.id;

		template.save(intermediate);

		// add one ref to root
		root.refToIntermediate = intermediate;
		template.save(root);

		RefCycleLoadingIntoDifferentTypeRoot loaded = template.findOne(query(where("id").is(root.id)),
				RefCycleLoadingIntoDifferentTypeRoot.class);

		assertThat(loaded.content).isEqualTo("jon snow");
		assertThat(loaded.getRefToIntermediate()).isInstanceOf(RefCycleLoadingIntoDifferentTypeIntermediate.class);
		assertThat(loaded.getRefToIntermediate().getRefToRootView())
				.isInstanceOf(RefCycleLoadingIntoDifferentTypeRootView.class);
		assertThat(loaded.getRefToIntermediate().getRefToRootView().getContent()).isEqualTo("jon snow");
	}

	@Test // DATAMONGO-1798
	public void stringDBRefLoading() {

		RawStringId ref = new RawStringId();
		ref.id = new ObjectId().toHexString();
		ref.value = "new value";

		template.save(ref);

		WithDBRefOnRawStringId source = new WithDBRefOnRawStringId();
		source.id = "foo";
		source.value = ref;

		template.save(source);

		org.bson.Document result = template
				.execute(db -> (org.bson.Document) db.getCollection(template.getCollectionName(WithDBRefOnRawStringId.class))
						.find(Filters.eq("_id", source.id)).limit(1).into(new ArrayList()).iterator().next());

		assertThat(result).isNotNull();
		assertThat(result.get("value"))
				.isEqualTo(new com.mongodb.DBRef(template.getCollectionName(RawStringId.class), ref.getId()));

		WithDBRefOnRawStringId target = template.findOne(query(where("id").is(source.id)), WithDBRefOnRawStringId.class);
		assertThat(target.value).isEqualTo(ref);
	}

	@Test // DATAMONGO-1798
	public void stringDBRefLazyLoading() {

		RawStringId ref = new RawStringId();
		ref.id = new ObjectId().toHexString();
		ref.value = "new value";

		template.save(ref);

		WithLazyDBRefOnRawStringId source = new WithLazyDBRefOnRawStringId();
		source.id = "foo";
		source.value = ref;

		template.save(source);

		org.bson.Document result = template.execute(
				db -> (org.bson.Document) db.getCollection(template.getCollectionName(WithLazyDBRefOnRawStringId.class))
						.find(Filters.eq("_id", source.id)).limit(1).into(new ArrayList()).iterator().next());

		assertThat(result).isNotNull();
		assertThat(result.get("value"))
				.isEqualTo(new com.mongodb.DBRef(template.getCollectionName(RawStringId.class), ref.getId()));

		WithLazyDBRefOnRawStringId target = template.findOne(query(where("id").is(source.id)),
				WithLazyDBRefOnRawStringId.class);

		assertThat(target.value).isInstanceOf(LazyLoadingProxy.class);
		assertThat(target.getValue()).isEqualTo(ref);
	}

	@Test // DATAMONGO-2223
	public void shouldResolveSingleDBRefToAnotherDb() {

		JustSomeType one = new JustSomeType();
		one.value = "one";

		otherDbTemplate.insert(one);

		WithRefToAnotherDb source = new WithRefToAnotherDb();
		source.value = one;

		template.save(source);

		WithRefToAnotherDb target = template.findOne(query(where("id").is(source.id)), WithRefToAnotherDb.class);
		assertThat(target.getValue()).isEqualTo(one);
	}

	@Test // DATAMONGO-2223
	public void shouldResolveSingleLazyDBRefToAnotherDb() {

		JustSomeType one = new JustSomeType();
		one.value = "one";

		otherDbTemplate.insert(one);

		WithLazyRefToAnotherDb source = new WithLazyRefToAnotherDb();
		source.value = one;

		template.save(source);

		WithLazyRefToAnotherDb target = template.findOne(query(where("id").is(source.id)), WithLazyRefToAnotherDb.class);
		LazyLoadingTestUtils.assertProxyIsResolved(target.value, false);
		assertThat(target.getValue()).isEqualTo(one);
	}

	@Test // DATAMONGO-2223
	public void shouldResolveListDBRefToAnotherDb() {

		JustSomeType one = new JustSomeType();
		one.value = "one";

		JustSomeType two = new JustSomeType();
		two.value = "two";

		otherDbTemplate.insertAll(Arrays.asList(one, two));

		WithListRefToAnotherDb source = new WithListRefToAnotherDb();
		source.value = Arrays.asList(one, two);

		template.save(source);

		WithListRefToAnotherDb target = template.findOne(query(where("id").is(source.id)), WithListRefToAnotherDb.class);
		assertThat(target.getValue()).containsExactlyInAnyOrder(one, two);
	}

	@Test // DATAMONGO-2223
	public void shouldResolveLazyListDBRefToAnotherDb() {

		JustSomeType one = new JustSomeType();
		one.value = "one";

		JustSomeType two = new JustSomeType();
		two.value = "two";

		otherDbTemplate.insertAll(Arrays.asList(one, two));

		WithLazyListRefToAnotherDb source = new WithLazyListRefToAnotherDb();
		source.value = Arrays.asList(one, two);

		template.save(source);

		WithLazyListRefToAnotherDb target = template.findOne(query(where("id").is(source.id)),
				WithLazyListRefToAnotherDb.class);
		LazyLoadingTestUtils.assertProxyIsResolved(target.value, false);
		assertThat(target.getValue()).containsExactlyInAnyOrder(one, two);
	}

	@Test // GH-2191
	void shouldAllowToSliceCollectionOfDbRefs() {

		JustSomeType one = new JustSomeType();
		one.value = "one";

		JustSomeType two = new JustSomeType();
		two.value = "two";

		template.insertAll(Arrays.asList(one, two));

		WithCollectionDbRef source = new WithCollectionDbRef();
		source.refs = Arrays.asList(one, two);

		template.save(source);

		Query theQuery = query(where("id").is(source.id));
		theQuery.fields().slice("refs", 1, 1);

		WithCollectionDbRef target = template.findOne(theQuery, WithCollectionDbRef.class);
		assertThat(target.getRefs()).containsExactly(two);
	}

	@Test // GH-2191
	void shouldAllowToSliceCollectionOfLazyDbRefs() {

		JustSomeType one = new JustSomeType();
		one.value = "one";

		JustSomeType two = new JustSomeType();
		two.value = "two";

		template.insertAll(Arrays.asList(one, two));

		WithCollectionDbRef source = new WithCollectionDbRef();
		source.lazyrefs = Arrays.asList(one, two);

		template.save(source);

		Query theQuery = query(where("id").is(source.id));
		theQuery.fields().slice("lazyrefs", 1, 1);

		WithCollectionDbRef target = template.findOne(theQuery, WithCollectionDbRef.class);
		LazyLoadingTestUtils.assertProxyIsResolved(target.lazyrefs, false);
		assertThat(target.getLazyrefs()).containsExactly(two);
	}

	@Document("cycle-with-different-type-root")
	static class RefCycleLoadingIntoDifferentTypeRoot {

		@Id String id;
		String content;
		@DBRef RefCycleLoadingIntoDifferentTypeIntermediate refToIntermediate;

		public String getId() {
			return this.id;
		}

		public String getContent() {
			return this.content;
		}

		public RefCycleLoadingIntoDifferentTypeIntermediate getRefToIntermediate() {
			return this.refToIntermediate;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setContent(String content) {
			this.content = content;
		}

		public void setRefToIntermediate(RefCycleLoadingIntoDifferentTypeIntermediate refToIntermediate) {
			this.refToIntermediate = refToIntermediate;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.RefCycleLoadingIntoDifferentTypeRoot(id=" + this.getId() + ", content="
					+ this.getContent() + ", refToIntermediate=" + this.getRefToIntermediate() + ")";
		}
	}

	@Document("cycle-with-different-type-intermediate")
	static class RefCycleLoadingIntoDifferentTypeIntermediate {

		@Id String id;
		@DBRef RefCycleLoadingIntoDifferentTypeRootView refToRootView;

		public String getId() {
			return this.id;
		}

		public RefCycleLoadingIntoDifferentTypeRootView getRefToRootView() {
			return this.refToRootView;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setRefToRootView(RefCycleLoadingIntoDifferentTypeRootView refToRootView) {
			this.refToRootView = refToRootView;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.RefCycleLoadingIntoDifferentTypeIntermediate(id=" + this.getId()
					+ ", refToRootView=" + this.getRefToRootView() + ")";
		}
	}

	@Document("cycle-with-different-type-root")
	static class RefCycleLoadingIntoDifferentTypeRootView {

		@Id String id;
		String content;

		public String getId() {
			return this.id;
		}

		public String getContent() {
			return this.content;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setContent(String content) {
			this.content = content;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.RefCycleLoadingIntoDifferentTypeRootView(id=" + this.getId() + ", content="
					+ this.getContent() + ")";
		}
	}

	static class RawStringId {

		@MongoId String id;
		String value;

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			RawStringId that = (RawStringId) o;
			return Objects.equals(id, that.id) && Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value);
		}

		public String toString() {
			return "MongoTemplateDbRefTests.RawStringId(id=" + this.getId() + ", value=" + this.getValue() + ")";
		}
	}

	static class WithCollectionDbRef {

		@Id String id;

		@DBRef List<JustSomeType> refs;

		@DBRef(lazy = true) List<JustSomeType> lazyrefs;

		public String getId() {
			return this.id;
		}

		public List<JustSomeType> getRefs() {
			return this.refs;
		}

		public List<JustSomeType> getLazyrefs() {
			return this.lazyrefs;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setRefs(List<JustSomeType> refs) {
			this.refs = refs;
		}

		public void setLazyrefs(List<JustSomeType> lazyrefs) {
			this.lazyrefs = lazyrefs;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.WithCollectionDbRef(id=" + this.getId() + ", refs=" + this.getRefs()
					+ ", lazyrefs=" + this.getLazyrefs() + ")";
		}
	}

	static class WithDBRefOnRawStringId {

		@Id String id;
		@DBRef RawStringId value;

		public String getId() {
			return this.id;
		}

		public RawStringId getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(RawStringId value) {
			this.value = value;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.WithDBRefOnRawStringId(id=" + this.getId() + ", value=" + this.getValue() + ")";
		}
	}

	static class WithLazyDBRefOnRawStringId {

		@Id String id;
		@DBRef(lazy = true) RawStringId value;

		public String getId() {
			return this.id;
		}

		public RawStringId getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(RawStringId value) {
			this.value = value;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.WithLazyDBRefOnRawStringId(id=" + this.getId() + ", value=" + this.getValue()
					+ ")";
		}
	}

	static class WithRefToAnotherDb {

		@Id String id;
		@DBRef(db = "mongo-template-dbref-tests-other-db") JustSomeType value;

		public String getId() {
			return this.id;
		}

		public JustSomeType getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(JustSomeType value) {
			this.value = value;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.WithRefToAnotherDb(id=" + this.getId() + ", value=" + this.getValue() + ")";
		}
	}

	static class WithLazyRefToAnotherDb {

		@Id String id;
		@DBRef(lazy = true, db = "mongo-template-dbref-tests-other-db") JustSomeType value;

		public String getId() {
			return this.id;
		}

		public JustSomeType getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(JustSomeType value) {
			this.value = value;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.WithLazyRefToAnotherDb(id=" + this.getId() + ", value=" + this.getValue() + ")";
		}
	}

	static class WithListRefToAnotherDb {

		@Id String id;
		@DBRef(db = "mongo-template-dbref-tests-other-db") List<JustSomeType> value;

		public String getId() {
			return this.id;
		}

		public List<JustSomeType> getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(List<JustSomeType> value) {
			this.value = value;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.WithListRefToAnotherDb(id=" + this.getId() + ", value=" + this.getValue() + ")";
		}
	}

	static class WithLazyListRefToAnotherDb {

		@Id String id;
		@DBRef(lazy = true, db = "mongo-template-dbref-tests-other-db") List<JustSomeType> value;

		public String getId() {
			return this.id;
		}

		public List<JustSomeType> getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(List<JustSomeType> value) {
			this.value = value;
		}

		public String toString() {
			return "MongoTemplateDbRefTests.WithLazyListRefToAnotherDb(id=" + this.getId() + ", value=" + this.getValue()
					+ ")";
		}
	}

	static class JustSomeType {

		@Id String id;
		String value;

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			JustSomeType that = (JustSomeType) o;
			return Objects.equals(id, that.id) && Objects.equals(value, that.value);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value);
		}

		public String toString() {
			return "MongoTemplateDbRefTests.JustSomeType(id=" + this.getId() + ", value=" + this.getValue() + ")";
		}
	}
}
