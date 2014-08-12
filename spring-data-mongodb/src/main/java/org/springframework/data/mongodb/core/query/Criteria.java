/*
 * Copyright 2010-2014 the original author or authors.
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

import static org.springframework.util.ObjectUtils.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.bson.BSON;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.Shape;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.geo.Sphere;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Central class for creating queries. It follows a fluent API style so that you can easily chain together multiple
 * criteria. Static import of the 'Criteria.where' method will improve readability.
 * 
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
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

		if (!isValue.equals(NOT_SET)) {
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
	 * Creates a criterion using the {@literal $ne} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/ne/
	 * @param o
	 * @return
	 */
	public Criteria ne(Object o) {
		criteria.put("$ne", o);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $lt} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/lt/
	 * @param o
	 * @return
	 */
	public Criteria lt(Object o) {
		criteria.put("$lt", o);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $lte} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/lte/
	 * @param o
	 * @return
	 */
	public Criteria lte(Object o) {
		criteria.put("$lte", o);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $gt} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/gt/
	 * @param o
	 * @return
	 */
	public Criteria gt(Object o) {
		criteria.put("$gt", o);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $gte} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/gte/
	 * @param o
	 * @return
	 */
	public Criteria gte(Object o) {
		criteria.put("$gte", o);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $in} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/in/
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
	 * Creates a criterion using the {@literal $in} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/in/
	 * @param c the collection containing the values to match against
	 * @return
	 */
	public Criteria in(Collection<?> c) {
		criteria.put("$in", c);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $nin} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/nin/
	 * @param o
	 * @return
	 */
	public Criteria nin(Object... o) {
		return nin(Arrays.asList(o));
	}

	/**
	 * Creates a criterion using the {@literal $nin} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/nin/
	 * @param o
	 * @return
	 */
	public Criteria nin(Collection<?> o) {
		criteria.put("$nin", o);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $mod} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/mod/
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
	 * Creates a criterion using the {@literal $all} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/all/
	 * @param o
	 * @return
	 */
	public Criteria all(Object... o) {
		return all(Arrays.asList(o));
	}

	/**
	 * Creates a criterion using the {@literal $all} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/all/
	 * @param o
	 * @return
	 */
	public Criteria all(Collection<?> o) {
		criteria.put("$all", o);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $size} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/size/
	 * @param s
	 * @return
	 */
	public Criteria size(int s) {
		criteria.put("$size", s);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $exists} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/exists/
	 * @param b
	 * @return
	 */
	public Criteria exists(boolean b) {
		criteria.put("$exists", b);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $type} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/type/
	 * @param t
	 * @return
	 */
	public Criteria type(int t) {
		criteria.put("$type", t);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $not} meta operator which affects the clause directly following
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/not/
	 * @return
	 */
	public Criteria not() {
		return not(null);
	}

	/**
	 * Creates a criterion using the {@literal $not} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/not/
	 * @param value
	 * @return
	 */
	private Criteria not(Object value) {
		criteria.put("$not", value);
		return this;
	}

	/**
	 * Creates a criterion using a {@literal $regex} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/regex/
	 * @param re
	 * @return
	 */
	public Criteria regex(String re) {
		return regex(re, null);
	}

	/**
	 * Creates a criterion using a {@literal $regex} and {@literal $options} operator.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/regex/
	 * @see http://docs.mongodb.org/manual/reference/operator/query/regex/#op._S_options
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
	 * Creates a geospatial criterion using a {@literal $within $centerSphere} operation. This is only available for Mongo
	 * 1.7 and higher.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/geoWithin/
	 * @see http://docs.mongodb.org/manual/reference/operator/query/centerSphere/
	 * @param circle must not be {@literal null}
	 * @return
	 */
	public Criteria withinSphere(Circle circle) {
		Assert.notNull(circle);
		criteria.put("$within", new GeoCommand(new Sphere(circle)));
		return this;
	}

	/**
	 * Creates a geospatial criterion using a {@literal $within} operation.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/geoWithin/
	 * @param shape
	 * @return
	 */
	public Criteria within(Shape shape) {

		Assert.notNull(shape);
		criteria.put("$within", new GeoCommand(shape));
		return this;
	}

	/**
	 * Creates a geospatial criterion using a {@literal $near} operation.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/near/
	 * @param point must not be {@literal null}
	 * @return
	 */
	public Criteria near(Point point) {
		Assert.notNull(point);
		criteria.put("$near", point);
		return this;
	}

	/**
	 * Creates a geospatial criterion using a {@literal $nearSphere} operation. This is only available for Mongo 1.7 and
	 * higher.
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/nearSphere/
	 * @param point must not be {@literal null}
	 * @return
	 */
	public Criteria nearSphere(Point point) {
		Assert.notNull(point);
		criteria.put("$nearSphere", point);
		return this;
	}

	/**
	 * Creates a geospatical criterion using a {@literal $maxDistance} operation, for use with $near
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/maxDistance/
	 * @param maxDistance
	 * @return
	 */
	public Criteria maxDistance(double maxDistance) {
		criteria.put("$maxDistance", maxDistance);
		return this;
	}

	/**
	 * Creates a criterion using the {@literal $elemMatch} operator
	 * 
	 * @see http://docs.mongodb.org/manual/reference/operator/query/elemMatch/
	 * @param c
	 * @return
	 */
	public Criteria elemMatch(Criteria c) {
		criteria.put("$elemMatch", c.getCriteriaObject());
		return this;
	}

	/**
	 * Creates an 'or' criteria using the $or operator for all of the provided criteria
	 * <p>
	 * Note that mongodb doesn't support an $or operator to be wrapped in a $not operator.
	 * <p>
	 * 
	 * @throws IllegalArgumentException if {@link #orOperator(Criteria...)} follows a not() call directly.
	 * @param criteria
	 */
	public Criteria orOperator(Criteria... criteria) {
		BasicDBList bsonList = createCriteriaList(criteria);
		return registerCriteriaChainElement(new Criteria("$or").is(bsonList));
	}

	/**
	 * Creates a 'nor' criteria using the $nor operator for all of the provided criteria.
	 * <p>
	 * Note that mongodb doesn't support an $nor operator to be wrapped in a $not operator.
	 * <p>
	 * 
	 * @throws IllegalArgumentException if {@link #norOperator(Criteria...)} follows a not() call directly.
	 * @param criteria
	 */
	public Criteria norOperator(Criteria... criteria) {
		BasicDBList bsonList = createCriteriaList(criteria);
		return registerCriteriaChainElement(new Criteria("$nor").is(bsonList));
	}

	/**
	 * Creates an 'and' criteria using the $and operator for all of the provided criteria.
	 * <p>
	 * Note that mongodb doesn't support an $and operator to be wrapped in a $not operator.
	 * <p>
	 * 
	 * @throws IllegalArgumentException if {@link #andOperator(Criteria...)} follows a not() call directly.
	 * @param criteria
	 */
	public Criteria andOperator(Criteria... criteria) {
		BasicDBList bsonList = createCriteriaList(criteria);
		return registerCriteriaChainElement(new Criteria("$and").is(bsonList));
	}

	private Criteria registerCriteriaChainElement(Criteria criteria) {

		if (lastOperatorWasNot()) {
			throw new IllegalArgumentException("operator $not is not allowed around criteria chain element: "
					+ criteria.getCriteriaObject());
		} else {
			criteriaChain.add(criteria);
		}
		return this;
	}

	public String getKey() {
		return this.key;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.CriteriaDefinition#getCriteriaObject()
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

		if (!NOT_SET.equals(isValue)) {
			queryCriteria.put(this.key, this.isValue);
			queryCriteria.putAll(dbo);
		} else {
			queryCriteria.put(this.key, dbo);
		}

		return queryCriteria;
	}

	private BasicDBList createCriteriaList(Criteria[] criteria) {
		BasicDBList bsonList = new BasicDBList();
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

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || !getClass().equals(obj.getClass())) {
			return false;
		}

		Criteria that = (Criteria) obj;

		if (this.criteriaChain.size() != that.criteriaChain.size()) {
			return false;
		}

		for (int i = 0; i < this.criteriaChain.size(); i++) {

			Criteria left = this.criteriaChain.get(i);
			Criteria right = that.criteriaChain.get(i);

			if (!simpleCriteriaEquals(left, right)) {
				return false;
			}
		}

		return true;
	}

	private boolean simpleCriteriaEquals(Criteria left, Criteria right) {

		boolean keyEqual = left.key == null ? right.key == null : left.key.equals(right.key);
		boolean criteriaEqual = left.criteria.equals(right.criteria);
		boolean valueEqual = isEqual(left.isValue, right.isValue);

		return keyEqual && criteriaEqual && valueEqual;
	}

	/**
	 * Checks the given objects for equality. Handles {@link Pattern} and arrays correctly.
	 * 
	 * @param left
	 * @param right
	 * @return
	 */
	private boolean isEqual(Object left, Object right) {

		if (left == null) {
			return right == null;
		}

		if (left instanceof Pattern) {
			return right instanceof Pattern ? ((Pattern) left).pattern().equals(((Pattern) right).pattern()) : false;
		}

		return ObjectUtils.nullSafeEquals(left, right);
	}

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17;

		result += nullSafeHashCode(key);
		result += criteria.hashCode();
		result += nullSafeHashCode(isValue);

		return result;
	}
}
