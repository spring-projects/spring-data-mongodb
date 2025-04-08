/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import org.bson.Document;
import org.springframework.data.mongodb.BindableMongoExpression;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;

/**
 * @author Christoph Strobl
 */
public class MongoAotRepositoryFragmentSupport {

	private final RepositoryMetadata repositoryMetadata;
	private final MongoOperations mongoOperations;
	private final MongoConverter mongoConverter;
	private final ProjectionFactory projectionFactory;

	protected MongoAotRepositoryFragmentSupport(MongoOperations mongoOperations,
			RepositoryFactoryBeanSupport.FragmentCreationContext context) {
		this(mongoOperations, context.getRepositoryMetadata(), context.getProjectionFactory());
	}

	protected MongoAotRepositoryFragmentSupport(MongoOperations mongoOperations, RepositoryMetadata repositoryMetadata,
			ProjectionFactory projectionFactory) {

		this.mongoOperations = mongoOperations;
		this.mongoConverter = mongoOperations.getConverter();
		this.repositoryMetadata = repositoryMetadata;
		this.projectionFactory = projectionFactory;
	}

	protected Document bindParameters(String source, Object[] parameters) {
		return new BindableMongoExpression(source, this.mongoConverter, parameters).toDocument();
	}

	protected BasicQuery createQuery(String queryString, Object[] parameters) {

		Document queryDocument = bindParameters(queryString, parameters);
		return new BasicQuery(queryDocument);
	}

}
