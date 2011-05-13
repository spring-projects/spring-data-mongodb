/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.data.document.mongodb.repository;

import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.query.Query;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * {@link RepositoryQuery} implementation for Mongo.
 * 
 * @author Oliver Gierke
 */
public class PartTreeMongoQuery extends AbstractMongoQuery {

	private final PartTree tree;

	/**
	 * Creates a new {@link PartTreeMongoQuery} from the given {@link QueryMethod} and {@link MongoTemplate}.
	 * 
	 * @param method
	 * @param template
	 */
	public PartTreeMongoQuery(MongoQueryMethod method, MongoTemplate template) {

		super(method, template);
		this.tree = new PartTree(method.getName(), method.getEntityInformation().getJavaType());
	}

	/**
	 * @return the tree
	 */
	public PartTree getTree() {
		return tree;
	}

	/*
	  * (non-Javadoc)
	  *
	  * @see
	  * org.springframework.data.document.mongodb.repository.AbstractMongoQuery#createQuery(org.springframework.data.
	  * document.mongodb.repository.ConvertingParameterAccessor)
	  */
	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {

		MongoQueryCreator creator = new MongoQueryCreator(tree, accessor);
		return creator.createQuery();
	}
}
