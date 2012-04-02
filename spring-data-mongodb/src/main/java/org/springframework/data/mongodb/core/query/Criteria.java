/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.BSON;
import org.bson.types.BasicBSONList;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.geo.Circle;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.core.geo.Shape;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Central class for creating queries. It follows a fluent API style so that you can easily chain together multiple
 * criteria. Static import of the 'Criteria.where' method will improve readability.
 */
public class Criteria implements CriteriaDefinition {

	/**
	 * Custom "not-null" object as we have to be able to work with {@literal null} values as well.
	 */
	private static final Object NOT_SET = new Object();

	private String key;

	private List<Criteria> criteriaChain;

	private LinkedHashMap<String, Object> criteria = new LinkedHashMap<String, Object>();

	private Object isValue = NOT_SET;

	public Criteria() {
		this.criteriaChain = new ArrayList<Criteria>();
	}

	public Criteria(String key) {
		this.criteriaChain = new ArrayList<Criteria>();
		this.criteriaChain.add(this);
		this.key = key;
	}

	protected Criteria(List<Criteria> criteriaChain, String key) {
		this.criteriaChain = criteriaChain;
		this.criteriaChain.add(this);
		this.key = key;
	}

	/**
	 * Static factory method to create a Criteria using the provided key
	 * 
	 * @param key
	 * @return
	 */
	public static Criteria where(String key) {
		return new Criteria(key);
	}

	/**
	 * Static factory method to create a Criteria using the provided key
	 * 
	 * @return
	 */
	public Criteria and(String key) {
		return new Criteria(this.criteriaChain, key);
	}

	/**
	 * Creates a criterion using equality
	 * 
	 * @param o
	 * @return
	 */
	public Criteria is(Object o) {
		if (isValue != NOT_SET) {
			throw new InvalidMongoDbApiUsageException(
					"Multiple 'is' values declared. You need to use 'and' with multiple criteria");
		}
		if (lastOperatorWasNot()) {
			throw new InvalidMongoDbApiUsageException("Invalid query: 'not' can't be used with 'is' - use 'ne' instead.");
		}
		this.isValue = o;
		return this;
	}

	private boolean lastOperatorWasNot() {
		return this.criteria.size() > 0 && "$not".equals(this.criteria.keySet().toArray()[this.criteria.size() - 1]);
	}

	/**
	 * Creates a criterion using the $ne operator
	 * 
	 * @param o
	 * @return
	 */
	public Criteria ne(Object o) {
		criteria.put("$ne", o);
		return this;
	}

	/**
	 * Creates a criterion using the $lt operator
	 * 
	 * @param o
	 * @return
	 */
	public Criteria lt(Object o) {
		criteria.put("$lt", o);
		return this;
	}

	/**
	 * Creates a criterion using the $lte operator
	 * 
	 * @param o
	 * @return
	 */
	public Criteria lte(Object o) {
		criteria.put("$lte", o);
		return this;
	}

	/**
	 * Creates a criterion using the $gt operator
	 * 
	 * @param o
	 * @return
	 */
	public Criteria gt(Object o) {
		criteria.put("$gt", o);
		return this;
	}

	/**
	 * Creates a criterion using the $gte operator
	 * 
	 * @param o
	 * @return
	 */
	public Criteria gte(Object o) {
		criteria.put("$gte", o);
		return this;
	}

	/**
	 * Creates a criterion using the $in operator
	 * 
	 * @param o the values to match against
	 * @return
	 */
	public Criteria in(Object... o) {
		if (o.length > 1 && o[1] instanceof Collection) {
			throw new InvalidMongoDbApiUsageException("You can only pass in one argument of type "
					+ o[1].getClass().getName());
		}
		criteria.put("$in", Arrays.asList(o));
		return this;
	}

	/**
	 * Creates a criterion using the $in operator
	 * 
	 * @param c the collection containing the values to match against
	 * @return
	 */
	public Criteria in(Collection<?> c) {
		criteria.put("$in", c);
		return this;
	}

	/**
	 * Creates a criterion using the $nin operator
	 * 
	 * @param o
	 * @return
	 */
	public Criteria nin(Object... o) {
		return nin(Arrays.asList(o));
	}

	public Criteria nin(Collection<?> o) {
		criteria.put("$nin", o);
		return this;
	}

	/**
	 * Creates a criterion using the $mod operator
	 * 
	 * @param value
	 * @param remainder
	 * @return
	 */
	public Criteria mod(Number value, Number remainder) {
		List<Object> l = new ArrayList<Object>();
		l.add(value);
		l.add(remainder);
		criteria.put("$mod", l);
		return this;
	}

	/**
	 * Creates a criterion using the $all operator
	 * 
	 * @param o
	 * @return
	 */
	public Criteria all(Object... o) {
		return all(Arrays.asList(o));
	}

	public Criteria all(Collection<?> o) {
		criteria.put("$all", o);
		return this;
	}

	/**
	 * Creates a criterion using the $size operator
	 * 
	 * @param s
	 * @return
	 */
	public Criteria size(int s) {
		criteria.put("$size", s);
		return this;
	}

	/**
	 * Creates a criterion using the $exists operator
	 * 
	 * @param b
	 * @return
	 */
	public Criteria exists(boolean b) {
		criteria.put("$exists", b);
		return this;
	}

	/**
	 * Creates a criterion using the $type operator
	 * 
	 * @param t
	 * @return
	 */
	public Criteria type(int t) {
		criteria.put("$type", t);
		return this;
	}

	/**
	 * Creates a criterion using the $not meta operator which affects the clause directly following
	 * 
	 * @return
	 */
	public Criteria not() {
		return not(null);
	}

	private Criteria not(Object value) {
		criteria.put("$not", value);
		return this;
	}

	/**
	 * Creates a criterion using a $regex
	 * 
	 * @param re
	 * @return
	 */
	public Criteria regex(String re) {
		return regex(re, null);
	}

	/**
	 * Creates a criterion using a $regex and $options
	 * 
	 * @param re
	 * @param options
	 * @return
	 */
	public Criteria regex(String re, String options) {
		return regex(toPattern(re, options));
	}

	/**
	 * Syntactical sugar for {@link #is(Object)} making obvious that we create a regex predicate.
	 * 
	 * @param pattern
	 * @return
	 */
	public Criteria regex(Pattern pattern) {

		Assert.notNull(pattern);

		if (lastOperatorWasNot()) {
			return not(pattern);
		}

		this.isValue = pattern;
		return this;
	}

	private Pattern toPattern(String regex, String options) {
		Assert.notNull(regex);
		return Pattern.compile(regex, options == null ? 0 : BSON.regexFlags(options));
	}

	/**
	 * Creates a geospatial criterion using a $within $center operation. This is only available for Mongo 1.7 and higher.
	 * 
	 * @param circle must not be {@literal null}
	 * @return
	 */
	public Criteria withinSphere(Circle circle) {
		Assert.notNull(circle);
		criteria.put("$within", new BasicDBObject("$centerSphere", circle.asList()));
		return this;
	}

	public Criteria within(Shape shape) {

		Assert.notNull(shape);
		criteria.put("$within", new BasicDBObject(shape.getCommand(), shape.asList()));
		return this;
	}

	/**
	 * Creates a geospatial criterion using a $near operation
	 * 
	 * @param point must not be {@literal null}
	 * @return
	 */
	public Criteria near(Point point) {
		Assert.notNull(point);
		criteria.put("$near", point.asList());
		return this;
	}

	/**
	 * Creates a geospatial criterion using a $nearSphere operation. This is only available for Mongo 1.7 and higher.
	 * 
	 * @param point must not be {@literal null}
	 * @return
	 */
	public Criteria nearSphere(Point point) {
		Assert.notNull(point);
		criteria.put("$nearSphere", point.asList());
		return this;
	}

	/**
	 * Creates a geospatical criterion using a $maxDistance operation, for use with $near
	 * 
	 * @param maxDistance
	 * @return
	 */
	public Criteria maxDistance(double maxDistance) {
		criteria.put("$maxDistance", maxDistance);
		return this;
	}

	/**
	 * Creates a criterion using the $elemMatch operator
	 * 
	 * @param c
	 * @return
	 */
	public Criteria elemMatch(Criteria c) {
		criteria.put("$elemMatch", c.getCriteriaObject());
		return this;
	}

	/**
	 * Creates an 'or' criteria using the $or operator for all of the provided criteria
	 * 
	 * @param criteria
	 */
	public Criteria orOperator(Criteria... criteria) {
		BasicBSONList bsonList = createCriteriaList(criteria);
		criteriaChain.add(new Criteria("$or").is(bsonList));
		return this;
	}

	/**
	 * Creates a 'nor' criteria using the $nor operator for all of the provided criteria
	 * 
	 * @param criteria
	 */
	public Criteria norOperator(Criteria... criteria) {
		BasicBSONList bsonList = createCriteriaList(criteria);
		criteriaChain.add(new Criteria("$nor").is(bsonList));
		return this;
	}

	/**
	 * Creates an 'and' criteria using the $and operator for all of the provided criteria
	 * 
	 * @param criteria
	 */
	public Criteria andOperator(Criteria... criteria) {
		BasicBSONList bsonList = createCriteriaList(criteria);
		criteriaChain.add(new Criteria("$and").is(bsonList));
		return this;
	}

	public String getKey() {
		return this.key;
	}

	/*
		 * (non-Javadoc)
		 *
		 * @see org.springframework.datastore.document.mongodb.query.Criteria#
		 * getCriteriaObject(java.lang.String)
		 */
	public DBObject getCriteriaObject() {
		if (this.criteriaChain.size() == 1) {
			return criteriaChain.get(0).getSingleCriteriaObject();
		} else {
			DBObject criteriaObject = new BasicDBObject();
			for (Criteria c : this.criteriaChain) {
				DBObject dbo = c.getSingleCriteriaObject();
				for (String k : dbo.keySet()) {
					setValue(criteriaObject, k, dbo.get(k));
				}
			}
			return criteriaObject;
		}
	}

	protected DBObject getSingleCriteriaObject() {
		DBObject dbo = new BasicDBObject();
		boolean not = false;
		for (String k : this.criteria.keySet()) {
			Object value = this.criteria.get(k);
			if (not) {
				DBObject notDbo = new BasicDBObject();
				notDbo.put(k, value);
				dbo.put("$not", notDbo);
				not = false;
			} else {
				if ("$not".equals(k) && value == null) {
					not = true;
				} else {
					dbo.put(k, value);
				}
			}
		}
		DBObject queryCriteria = new BasicDBObject();
		if (isValue != NOT_SET) {
			queryCriteria.put(this.key, this.isValue);
			queryCriteria.putAll(dbo);
		} else {
			queryCriteria.put(this.key, dbo);
		}
		return queryCriteria;
	}

	private BasicBSONList createCriteriaList(Criteria[] criteria) {
		BasicBSONList bsonList = new BasicBSONList();
		for (Criteria c : criteria) {
			bsonList.add(c.getCriteriaObject());
		}
		return bsonList;
	}

	private void setValue(DBObject dbo, String key, Object value) {
		Object existing = dbo.get(key);
		if (existing == null) {
			dbo.put(key, value);
		} else {
			throw new InvalidMongoDbApiUsageException("Due to limitations of the com.mongodb.BasicDBObject, "
					+ "you can't add a second '" + key + "' expression specified as '" + key + " : " + value + "'. "
					+ "Criteria already contains '" + key + " : " + existing + "'.");
		}
	}

}
