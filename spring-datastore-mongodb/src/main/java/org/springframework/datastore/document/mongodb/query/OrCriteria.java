package org.springframework.datastore.document.mongodb.query;

import org.bson.types.BasicBSONList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class OrCriteria implements CriteriaSpec {
	
	Query[] queries = null;
	
	public OrCriteria(Query[] queries) {
		super();
		this.queries = queries;
	}


	/* (non-Javadoc)
	 * @see org.springframework.datastore.document.mongodb.query.Criteria#getCriteriaObject(java.lang.String)
	 */
	public DBObject getCriteriaObject(String key) {
		DBObject dbo = new BasicDBObject();
		BasicBSONList l = new BasicBSONList();
		for (Query q : queries) {
			l.add(q.getQueryObject());
		}
		dbo.put(key, l);
		return dbo;
	}

}
