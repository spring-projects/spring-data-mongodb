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

import java.util.Collections;
import java.util.Map;

public class DbInfo {

  private Map dbInfoMap;

  public DbInfo(Map dbInfoMap) {
    super();
    this.dbInfoMap = dbInfoMap;
  }

  public boolean isCompactRunning() {
    return (Boolean) this.dbInfoMap.get("compact_running");
  }

  public String getDbName() {
    return (String) this.dbInfoMap.get("db_name");
  }

  public long getDiskFormatVersion() {
    return (Long) this.dbInfoMap.get("disk_format_version");
  }

  public long getDiskSize() {
    return (Long) this.dbInfoMap.get("disk_size");
  }

  public long getDocCount() {
    return (Long) this.dbInfoMap.get("doc_count");
  }

  public long getDocDeleteCount() {
    return (Long) this.dbInfoMap.get("doc_del_count");
  }

  public long getInstanceStartTime() {
    return (Long) this.dbInfoMap.get("instance_start_time");
  }

  public long getPurgeSequence() {
    return (Long) this.dbInfoMap.get("purge_seq");
  }

  public long getUpdateSequence() {
    return (Long) this.dbInfoMap.get("update_seq");
  }

  public Map getDbInfoMap() {
    return Collections.unmodifiableMap(dbInfoMap);
  }


}
