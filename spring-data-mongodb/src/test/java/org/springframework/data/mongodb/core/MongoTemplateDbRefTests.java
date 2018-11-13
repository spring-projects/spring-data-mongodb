/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;

import java.util.ArrayList;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.convert.LazyLoadingProxy;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Filters;

/**
 * {@link org.springframework.data.mongodb.core.mapping.DBRef} related integration tests for
 * {@link org.springframework.data.mongodb.core.MongoTemplate}.
 *
 * @author Christoph Strobl
 */
public class MongoTemplateDbRefTests {

	MongoTemplate template;

	@Before
	public void setUp() {

		template = new MongoTemplate(new MongoClient(), "mongo-template-dbref-tests");

		template.dropCollection(RefCycleLoadingIntoDifferentTypeRoot.class);
		template.dropCollection(RefCycleLoadingIntoDifferentTypeIntermediate.class);
		template.dropCollection(RefCycleLoadingIntoDifferentTypeRootView.class);
		template.dropCollection(WithDBRefOnRawStringId.class);
		template.dropCollection(WithLazyDBRefOnRawStringId.class);
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

	@Data
	@Document("cycle-with-different-type-root")
	static class RefCycleLoadingIntoDifferentTypeRoot {

		@Id String id;
		String content;
		@DBRef RefCycleLoadingIntoDifferentTypeIntermediate refToIntermediate;
	}

	@Data
	@Document("cycle-with-different-type-intermediate")
	static class RefCycleLoadingIntoDifferentTypeIntermediate {

		@Id String id;
		@DBRef RefCycleLoadingIntoDifferentTypeRootView refToRootView;
	}

	@Data
	@Document("cycle-with-different-type-root")
	static class RefCycleLoadingIntoDifferentTypeRootView {

		@Id String id;
		String content;
	}

	@Data
	static class RawStringId {

		@MongoId String id;
		String value;
	}

	@Data
	static class WithDBRefOnRawStringId {

		@Id String id;
		@org.springframework.data.mongodb.core.mapping.DBRef RawStringId value;
	}

	@Data
	static class WithLazyDBRefOnRawStringId {

		@Id String id;
		@org.springframework.data.mongodb.core.mapping.DBRef(lazy = true) RawStringId value;
	}

}
