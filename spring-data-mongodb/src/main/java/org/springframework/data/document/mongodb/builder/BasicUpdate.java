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
package org.springframework.data.document.mongodb.builder;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class BasicUpdate implements Update {

	private DBObject updateObject = null;
	
	private DBObject sortObject;
	
	public BasicUpdate(String updateString) {
		super();
		this.updateObject = (DBObject) JSON.parse(updateString);
	}
	
	public BasicUpdate(DBObject updateObject) {
		super();
		this.updateObject = updateObject;
	}

	public BasicUpdate(String updateString, String sortString) {
		super();
		this.updateObject = (DBObject) JSON.parse(updateString);
		this.sortObject = (DBObject) JSON.parse(sortString);
	}
	
	public BasicUpdate(DBObject updateObject, DBObject sortObject) {
		super();
		this.updateObject = updateObject;
		this.sortObject = sortObject;
	}

	public DBObject getUpdateObject() {
		return updateObject;
	}

	public DBObject getSortObject() {
		return this.sortObject;
	}

}
