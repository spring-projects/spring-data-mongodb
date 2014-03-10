/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.util.JSON;

/**
 * Query to use a plain JSON String to create the {@link Query} to actually execute.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class StringBasedMongoQuery extends AbstractMongoQuery {

	private static final Pattern PLACEHOLDER = Pattern.compile("\\?(\\d+)");
	private static final Logger LOG = LoggerFactory.getLogger(StringBasedMongoQuery.class);

	private final String query;
	private final String fieldSpec;
	private final boolean isCountQuery;
	private final boolean isDeleteQuery;

	/**
	 * Creates a new {@link StringBasedMongoQuery}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public StringBasedMongoQuery(String query, MongoQueryMethod method, MongoOperations mongoOperations) {

		super(method, mongoOperations);

		this.query = query;
		this.fieldSpec = method.getFieldSpecification();
		this.isCountQuery = method.hasAnnotatedQuery() ? method.getQueryAnnotation().count() : false;
		this.isDeleteQuery = method.hasAnnotatedQuery() ? method.getQueryAnnotation().delete() : false;
	}

	public StringBasedMongoQuery(MongoQueryMethod method, MongoOperations mongoOperations) {
		this(method.getAnnotatedQuery(), method, mongoOperations);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#createQuery(org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {

		String queryString = replacePlaceholders(query, accessor);

		Query query = null;

		if (fieldSpec != null) {
			String fieldString = replacePlaceholders(fieldSpec, accessor);
			query = new BasicQuery(queryString, fieldString);
		} else {
			query = new BasicQuery(queryString);
		}

		query.with(accessor.getSort());

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Created query %s", query.getQueryObject()));
		}

		return query;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return isCountQuery;
	}

	@Override
	protected boolean isDeleteQuery() {
		return this.isDeleteQuery;
	}

	private String replacePlaceholders(String input, ConvertingParameterAccessor accessor) {

		Matcher matcher = PLACEHOLDER.matcher(input);
		String result = input;

		while (matcher.find()) {
			String group = matcher.group();
			int index = Integer.parseInt(matcher.group(1));
			result = result.replace(group, getParameterWithIndex(accessor, index));
		}

		return result;
	}

	private String getParameterWithIndex(ConvertingParameterAccessor accessor, int index) {
		return JSON.serialize(accessor.getBindableValue(index));
	}
}
