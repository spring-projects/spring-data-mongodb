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
package org.springframework.data.document.couchdb.admin;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.data.document.couchdb.core.CouchConstants;

public class CouchAdminIntegrationTests {

	@Test
	public void dbLifecycle() {
		
		CouchAdmin admin = new CouchAdmin(CouchConstants.COUCHDB_URL);
		admin.deleteDatabase("foo");
		List<String> dbs = admin.listDatabases();
		admin.createDatabase("foo");
		List<String> newDbs = admin.listDatabases();
		Assert.assertEquals(dbs.size()+1, newDbs.size());
	}
}
