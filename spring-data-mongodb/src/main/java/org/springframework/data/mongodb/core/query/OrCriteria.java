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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.BasicBSONList;

public class OrCriteria implements CriteriaDefinition {

	Query[] queries = null;

	public OrCriteria(Query[] queries) {
		super();
		this.queries = queries;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.datastore.document.mongodb.query.Criteria#
	 * getCriteriaObject(java.lang.String)
	 */
	public DBObject getCriteriaObject() {
		DBObject dbo = new BasicDBObject();
		BasicBSONList l = new BasicBSONList();
		for (Query q : queries) {
			l.add(q.getQueryObject());
		}
		dbo.put(getOperator(), l);
		return dbo;
	}

	protected String getOperator() {
		return "$or";
	}

}
