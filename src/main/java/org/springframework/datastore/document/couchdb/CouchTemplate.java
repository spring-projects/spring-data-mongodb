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

package org.springframework.datastore.document.couchdb;


import org.jcouchdb.db.Database;
import org.jcouchdb.document.BaseDocument;
import org.springframework.datastore.document.AbstractDocumentStoreTemplate;
import org.springframework.datastore.document.DocumentSource;
import org.springframework.datastore.document.DocumentStoreConnectionFactory;

	public class CouchTemplate extends AbstractDocumentStoreTemplate<Database> {

	private DocumentStoreConnectionFactory<Database> connectionFactory;
	
	public CouchTemplate() {
		super();
	}
	
	public CouchTemplate(String host, String databaseName) {
		super();
		connectionFactory = new CouchDbConnectionFactory(host, databaseName);
	}

	public CouchTemplate(CouchDbConnectionFactory mcf) {
		super();
		connectionFactory = mcf;
	}
	
	public void save(DocumentSource<BaseDocument> documentSource) {
		BaseDocument d = documentSource.getDocument();
		getDocumentStoreConnectionFactory().getConnection().createDocument(d);
	}

	@Override
	public DocumentStoreConnectionFactory<Database> getDocumentStoreConnectionFactory() {
		return connectionFactory;
	}
	
	
}
