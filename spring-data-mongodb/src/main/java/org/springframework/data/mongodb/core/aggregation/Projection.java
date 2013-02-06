/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.core.query.Field;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Projection of field to be used in an {@link AggregationPipeline}.
 * <p/>
 * A projection is similar to a {@link Field} inclusion/exclusion but more powerful. It can generate new fields, change
 * values of given field etc.
 * 
 * @author Tobias Trelle
 */
public class Projection {

	private static final String REFERENCE_PREFIX = "$";
	
	private DBObject document = new BasicDBObject();

	/** Key of the current field. */
	private String ref;

	private String modifier;

	/** Create an empty projection. */
	public Projection() {
	}

	/**
	 * This convenience constructor excludes the field <code>_id</code> and includes the given fields.
	 * 
	 * @param includes Keys of field to include.
	 */
	public Projection(String... includes) {
		Assert.notEmpty(includes);
		exclude("_id");
		for (String key : includes) {
			include(key);
		}
	}

	/**
	 * Excludes a given field.
	 * 
	 * @param key The key of the field.
	 */
	public final void exclude(String key) {
		Assert.notNull(key, "Missing key");
		document.put(key, 0);
	}

	/**
	 * Includes a given field.
	 * 
	 * @param key The key of the field.
	 */
	public final Projection include(String key) {
		Assert.notNull(key, "Missing key");
		if (canPop()) {
			document.put(pop(), 1);
		}
		push(key);
		
		return this;
	}

	/**
	 * Sets the key for a computed field.
	 * 
v	 */
	public final Projection as(String key) {
		Assert.notNull(key, "Missing key");

		document.put( key, safeReference(pop()) );
		return this;
	}

	private void push(String r) {
		if (ref != null) {
			throw new InvalidDataAccessApiUsageException("No field selected");
		}
		ref = r;
	}

	private String pop() {
		if (ref == null) {
			throw new InvalidDataAccessApiUsageException("No field selected");
		}
		String r = ref;
		ref = null;
		modifier = null;

		return r;
	}

	private boolean canPop() {
		return ref != null;
	}

	private String safeReference(String key) {
		Assert.notNull(key);
		
		if ( !key.startsWith(REFERENCE_PREFIX) ) {
			return REFERENCE_PREFIX + key;
		} else {
			return key;
		}
	}
	
	DBObject toDBObject() {
		if (canPop()) {
			document.put(pop(), 1);
		}
		return document;
	}

}
