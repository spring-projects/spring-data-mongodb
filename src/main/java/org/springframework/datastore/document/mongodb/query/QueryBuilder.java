package org.springframework.datastore.document.mongodb.query;

import java.util.LinkedHashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class QueryBuilder implements Query {
	
	private LinkedHashMap<String, QueryCriterion> criteria = new LinkedHashMap<String, QueryCriterion>();

	public QueryCriterion find(String key) {
		QueryCriterion c = new QueryCriterion(this);
		this.criteria.put(key, c);
		return c;
	}

	public void or(Query... queries) {
	}

	public FieldSpecification fields() {
		return new FieldSpecification();
	}
	
	public SliceSpecification slice() {
		return new SliceSpecification();
	}
	
	public SortSpecification sort() {
		return new SortSpecification();
	}
	
	public QueryBuilder limit(int limit) {
		return this;
	}
	
	public Query build() {
		return this;
	}

	public DBObject getQueryObject() {
		DBObject dbo = new BasicDBObject();
		for (String k : criteria.keySet()) {
			QueryCriterion c = criteria.get(k);
			DBObject cl = c.getCriteriaObject(k);
			dbo.putAll(cl);
		}
		return dbo;
	}

}
