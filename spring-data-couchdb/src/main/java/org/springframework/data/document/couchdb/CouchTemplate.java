/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.data.document.couchdb;


import org.jcouchdb.db.Database;
import org.jcouchdb.document.BaseDocument;
import org.springframework.data.document.AbstractDocumentStoreTemplate;
import org.springframework.data.document.DocumentSource;

	public class CouchTemplate extends AbstractDocumentStoreTemplate<Database> {

	private Database database;
	
	public CouchTemplate() {
		super();
	}
	
	public CouchTemplate(String host, String databaseName) {
		super();
		database = new Database(host, databaseName);
	}

	public CouchTemplate(Database database) {
		super();
		this.database = database;
	}
	
	public void save(DocumentSource<BaseDocument> documentSource) {
		BaseDocument d = documentSource.getDocument();
		getConnection().createDocument(d);
	}

	@Override
	public Database getConnection() {
		return database;
	}
	
	
}
