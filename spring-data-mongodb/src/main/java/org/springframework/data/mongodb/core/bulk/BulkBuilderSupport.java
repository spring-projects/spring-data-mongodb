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
package org.springframework.data.mongodb.core.bulk;

import java.util.List;

import org.springframework.data.mongodb.core.bulk.Bulk.BulkBuilder;
import org.springframework.data.mongodb.core.bulk.BulkOperation.RemoveFirst;
import org.springframework.data.mongodb.core.bulk.BulkOperation.UpdateFirst;
import org.springframework.data.mongodb.core.bulk.BulkOperationContext.TypedNamespace;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

/**
 * Default implementation of {@link BulkBuilder} and {@link TypedBulkBuilder} that tracks the current collection
 * (namespace) and builds a list of {@link BulkOperation bulk operations} for execution. Supports bulk writes across
 * multiple collections as in MongoDB's bulk write model.
 *
 * @author Christoph Strobl
 * @since 5.1
 */
abstract class BulkBuilderSupport implements Bulk.BulkSpec {

	final List<BulkOperation> bulkOperations;

	public BulkBuilderSupport(List<BulkOperation> bulkOperations) {
		this.bulkOperations = bulkOperations;
	}

	abstract TypedNamespace getNamespace();

	@Override
	public Bulk.BulkSpec insert(Object object) {
		bulkOperations.add(new BulkInsert(object, getNamespace()));
		return this;
	}

	@Override
	public Bulk.BulkSpec insertAll(Iterable<? extends Object> objects) {
		objects.forEach(this::insert);
		return this;
	}

	@Override
	public Bulk.BulkSpec updateOne(Query filter, UpdateDefinition update) {
		bulkOperations.add(new BulkUpdateFirst(filter, update, false, getNamespace()));
		return this;
	}

	@Override
	public Bulk.BulkSpec updateMulti(Query filter, UpdateDefinition update) {
		bulkOperations.add(new BulkUpdate(filter, update, false, getNamespace()));
		return this;
	}

	@Override
	public Bulk.BulkSpec upsert(Query filter, UpdateDefinition update) {
		bulkOperations.add(new BulkUpdate(filter, update, true, getNamespace()));
		return this;
	}

	@Override
	public Bulk.BulkSpec remove(Query filter) {
		bulkOperations.add(new BulkRemove(filter, getNamespace()));
		return this;
	}

	@Override
	public Bulk.BulkSpec replaceOne(Query filter, Object replacement) {
		bulkOperations.add(new BulkReplace(filter, replacement, true, getNamespace()));
		return this;
	}

	@Override
	public Bulk.BulkSpec replaceIfExists(Query filter, Object replacement) {
		bulkOperations.add(new BulkReplace(filter, replacement, false, getNamespace()));
		return this;
	}

	private static class ContextAware {

		protected final BulkOperationContext context;

		public ContextAware(TypedNamespace namespace) {
			this(new DefaultBulkOperationContext(namespace));
		}

		public ContextAware(BulkOperationContext context) {
			this.context = context;
		}

		public BulkOperationContext context() {
			return context;
		}
	}

	private static class DefaultBulkOperationContext implements BulkOperationContext {

		TypedNamespace namespace;

		public DefaultBulkOperationContext(TypedNamespace namespace) {
			this.namespace = namespace;
		}

		@Override
		public TypedNamespace namespace() {
			return namespace;
		}
	}

	static class BulkInsert extends ContextAware implements BulkOperation.Insert {

		private final Object value;

		public BulkInsert(Object value, TypedNamespace namespace) {
			super(namespace);
			this.value = value;
		}

		@Override
		public Object value() {
			return value;
		}
	}

	static class BulkUpdate extends ContextAware implements BulkOperation.Update {

		private final Query query;
		private final UpdateDefinition update;
		private final boolean upsert;

		public BulkUpdate(Query query, UpdateDefinition update, boolean upsert, TypedNamespace namespace) {

			super(namespace);
			this.query = query;
			this.update = update;
			this.upsert = upsert;
		}

		@Override
		public UpdateDefinition update() {
			return update;
		}

		@Override
		public Query query() {
			return query;
		}

		@Override
		public boolean upsert() {
			return upsert;
		}
	}

	static class BulkUpdateFirst extends BulkUpdate implements UpdateFirst {

		public BulkUpdateFirst(Query query, UpdateDefinition update, boolean upsert, TypedNamespace namespace) {
			super(query, update, upsert, namespace);
		}

	}

	static class BulkRemove extends ContextAware implements BulkOperation.Remove {

		private final Query query;

		public BulkRemove(Query query, TypedNamespace namespace) {
			super(namespace);
			this.query = query;
		}

		@Override
		public Query query() {
			return query;
		}
	}

	static class BulkRemoveFirst extends BulkRemove implements RemoveFirst {

		public BulkRemoveFirst(Query query, TypedNamespace namespace) {
			super(query, namespace);
		}
	}

	static class BulkReplace extends ContextAware implements BulkOperation.Replace {

		private final Query query;
		private final Object replacement;
		private final boolean upsert;

		BulkReplace(Query query, Object replacement, boolean upsert, TypedNamespace namespace) {

			super(namespace);

			this.query = query;
			this.replacement = replacement;
			this.upsert = upsert;
		}

		@Override
		public Query query() {
			return query;
		}

		@Override
		public Object replacement() {
			return replacement;
		}

		@Override
		public boolean upsert() {
			return upsert;
		}
	}

}
