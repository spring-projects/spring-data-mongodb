/*
 * Copyright 2011 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.document.couchdb.admin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.data.document.couchdb.support.CouchUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

public class CouchAdmin implements CouchAdminOperations {

  private String databaseUrl;
  private RestOperations restOperations = new RestTemplate();

  public CouchAdmin(String databaseUrl) {

    if (!databaseUrl.trim().endsWith("/")) {
      this.databaseUrl = databaseUrl.trim() + "/";
    } else {
      this.databaseUrl = databaseUrl.trim();
    }
  }

  public List<String> listDatabases() {
    String dbs = restOperations.getForObject(databaseUrl + "_all_dbs", String.class);
    return Arrays.asList(StringUtils.commaDelimitedListToStringArray(dbs));
  }

  public void createDatabase(String dbName) {
    org.springframework.util.Assert.hasText(dbName);
    restOperations.put(databaseUrl + dbName, null);

  }

  public void deleteDatabase(String dbName) {
    org.springframework.util.Assert.hasText(dbName);
    restOperations.delete(CouchUtils.ensureTrailingSlash(databaseUrl + dbName));

  }

  public DbInfo getDatabaseInfo(String dbName) {
    String url = CouchUtils.ensureTrailingSlash(databaseUrl + dbName);
    Map dbInfoMap = (Map) restOperations.getForObject(url, Map.class);
    return new DbInfo(dbInfoMap);
  }

}