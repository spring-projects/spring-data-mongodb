/*
 * Copyright 2026. the original author or authors.
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

/*
 * Copyright 2026 the original author or authors.
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

import java.util.List;

import com.mongodb.bulk.BulkWriteResult;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

/**
 * @author Christoph Strobl
 * @since 2026/01
 */
public interface NamespaceBulkOperations {

	NamespacedBulkOperations<Object> inNamespace(Namespace namespace);
	<T> NamespacedBulkOperations<T> inNamespace(Class<T> type);

	interface NamespacedBulkOperations<T> {
		NamespacedBulkInsertOperations<T> insert();
		NamespacedBulkUpdateOperations<T> update();
		NamespacedBulkDeleteOperations<T> delete();
		NamespacedBulkOperations<Object> inNamespace(Namespace namespace);
		<S> NamespacedBulkOperations<S> inNamespace(Class<S> type);

		BulkWriteResult execute();
	}

	interface NamespacedBulkInsertOperations<T> extends NamespacedBulkOperations<T> {
		NamespacedBulkInsertOperations<T> one(T object);
		NamespacedBulkInsertOperations<T> many(List<T> object);
	}

	interface NamespacedBulkUpdateOperations<T> extends NamespacedBulkOperations<T> {
		NamespacedBulkUpdateOperations<T> one(Query query, UpdateDefinition update);
		NamespacedBulkUpdateOperations<T> one(CriteriaDefinition where, UpdateDefinition update);
	}
	interface NamespacedBulkDeleteOperations <T>{
		NamespacedBulkDeleteOperations<T> delete(Query query);
	}


}
