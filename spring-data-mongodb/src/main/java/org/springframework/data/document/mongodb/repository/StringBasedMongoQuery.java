/*
 * Copyright 2011 the original author or authors.
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

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.query.BasicQuery;
import org.springframework.data.document.mongodb.query.Query;

/**
 * Query to use a plain JSON String to create the {@link Query} to actually execute.
 * 
 * @author Oliver Gierke
 */
public class StringBasedMongoQuery extends AbstractMongoQuery {

	private static final Pattern PLACEHOLDER = Pattern.compile("\\?");
	private static final Logger LOG = LoggerFactory.getLogger(StringBasedMongoQuery.class);
	
	private final String query;

	/**
	 * Creates a new {@link StringBasedMongoQuery}.
	 * 
	 * @param method
	 * @param template
	 */
	public StringBasedMongoQuery(MongoQueryMethod method, MongoTemplate template) {
		super(method, template);
		this.query = method.getAnnotatedQuery();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.document.mongodb.repository.AbstractMongoQuery#createQuery(org.springframework.data.
	 * repository.query.SimpleParameterAccessor, org.springframework.data.document.mongodb.MongoConverter)
	 */
	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {
		
		Matcher matcher = PLACEHOLDER.matcher(query);
		Iterator<Object> iterator = accessor.iterator();
		String result = null;
		
		while (matcher.find()) {
			String group = matcher.group();
			result = query.replace(group, String.format("\"%s\"", iterator.next()));
		}
		
		Query query = new BasicQuery(result);
		LOG.debug("Created query {}", query.getQueryObject());
		
		return query;
	}
}
