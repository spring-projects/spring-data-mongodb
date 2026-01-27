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

	NamespacedBulkOperations inNamespace(Namespace namespace);

	interface NamespacedBulkOperations {
		NamespacedBulkInsertOperations insert();
		NamespacedBulkUpdateOperations update();
		NamespacedBulkDeleteOperations delete();
		NamespacedBulkOperations inNamespace(Namespace namespace);

		BulkWriteResult execute();
	}

	interface NamespacedBulkInsertOperations extends NamespacedBulkOperations {
		NamespacedBulkInsertOperations one(Object object);
		NamespacedBulkInsertOperations many(List<Object> object);
	}

	interface NamespacedBulkUpdateOperations extends NamespacedBulkOperations {
		NamespacedBulkUpdateOperations one(Query query, UpdateDefinition update);
		NamespacedBulkUpdateOperations one(CriteriaDefinition where, UpdateDefinition update);
	}
	interface NamespacedBulkDeleteOperations{
		NamespacedBulkDeleteOperations delete(Query query);
	}


}
