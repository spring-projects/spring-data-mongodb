/*
 * Copyright 2023. the original author or authors.
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
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import org.springframework.data.spel.EvaluationContextProvider;

/**
 * @author Christoph Strobl
 * @since 2023/10
 */
public class DefaultRepositoryActionPreparer implements RepositoryActionPreparer {

	EvaluationContextProvider evaluationContextProvider;

	public DefaultRepositoryActionPreparer(EvaluationContextProvider evaluationContextProvider) {
		this.evaluationContextProvider = evaluationContextProvider;
	}

	@Override
	public MongoRepositoryAction prepare(MongoRepositoryAction action, CrudMethodMetadata metadata) {

		if(action instanceof QueryAction queryAction) {
			prepareQuery(queryAction, metadata);
		}

		return action;
	}

	void prepareQuery(QueryAction action, CrudMethodMetadata metadata) {

		metadata.getReadPreference().ifPresent(it -> action.getQuery().withReadPreference(it));
		if(!action.getQuery().getCollation().isPresent()) {
			metadata.getCollation().map(org.springframework.data.mongodb.core.query.Collation::of).ifPresent(it -> action.getQuery().collation(it));
		}
	}
}
