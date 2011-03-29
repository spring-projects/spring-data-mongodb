/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb.log4j;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoLog4jAppender extends AppenderSkeleton {

  public static final String LEVEL = "level";
  public static final String NAME = "name";
  public static final String APP_ID = "applicationId";
  public static final String TIMESTAMP = "timestamp";
  public static final String PROPERTIES = "properties";
  public static final String TRACEBACK = "traceback";
  public static final String MESSAGE = "message";

  protected String host = "localhost";
  protected int port = 27017;
  protected String database = "logs";
  protected String collection = null;
  protected String applicationId = System.getProperty("APPLICATION_ID", null);
  protected WriteConcern warnOrHigherWriteConcern = WriteConcern.SAFE;
  protected WriteConcern infoOrLowerWriteConcern = WriteConcern.NORMAL;
  protected Mongo mongo;
  protected DB db;

  public MongoLog4jAppender() {
  }

  public MongoLog4jAppender(boolean isActive) {
    super(isActive);
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getApplicationId() {
    return applicationId;
  }

  public void setApplicationId(String applicationId) {
    this.applicationId = applicationId;
  }

  public void setWarnOrHigherWriteConcern(String wc) {
    this.warnOrHigherWriteConcern = WriteConcern.valueOf(wc);
  }

  public String getWarnOrHigherWriteConcern() {
    return warnOrHigherWriteConcern.toString();
  }

  public String getInfoOrLowerWriteConcern() {
    return infoOrLowerWriteConcern.toString();
  }

  public void setInfoOrLowerWriteConcern(String wc) {
    this.infoOrLowerWriteConcern = WriteConcern.valueOf(wc);
  }

  protected void connectToMongo() throws UnknownHostException {
    this.mongo = new Mongo(host, port);
    this.db = mongo.getDB(database);
  }

  @SuppressWarnings({"unchecked"})
  @Override
  protected void append(final LoggingEvent event) {
    if (null == db) {
      try {
        connectToMongo();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    BasicDBObject dbo = new BasicDBObject();
    dbo.put(APP_ID, applicationId);
    dbo.put(NAME, event.getLogger().getName());
    dbo.put(LEVEL, event.getLevel().toString());
    Calendar tstamp = Calendar.getInstance();
    tstamp.setTimeInMillis(event.getTimeStamp());
    dbo.put(TIMESTAMP, tstamp.getTime());

    // Copy properties into document
    Map<Object, Object> props = event.getProperties();
    if (null != props && props.size() > 0) {
      BasicDBObject propsDbo = new BasicDBObject();
      for (Map.Entry<Object, Object> entry : props.entrySet()) {
        propsDbo.put(entry.getKey().toString(), entry.getValue().toString());
      }
      dbo.put(PROPERTIES, propsDbo);
    }

    // Copy traceback info (if there is any) into the document
    String[] traceback = event.getThrowableStrRep();
    if (null != traceback && traceback.length > 0) {
      BasicDBList tbDbo = new BasicDBList();
      tbDbo.addAll(Arrays.asList(traceback));
      dbo.put(TRACEBACK, tbDbo);
    }

    // Put the rendered message into the document
    dbo.put(MESSAGE, event.getRenderedMessage());

    // Insert the document
    if (null == collection) {
      // Use the category name
      collection = event.getLogger().getName();
    } else {
      Calendar now = Calendar.getInstance();
      collection = String.format(collection,
          now.get(Calendar.YEAR),
          now.get(Calendar.MONTH),
          now.get(Calendar.DAY_OF_MONTH),
          now.get(Calendar.HOUR_OF_DAY),
          event.getLevel().toString(),
          event.getLogger().getName());
    }

    WriteConcern wc;
    if (event.getLevel().isGreaterOrEqual(Level.WARN)) {
      wc = warnOrHigherWriteConcern;
    } else {
      wc = infoOrLowerWriteConcern;
    }
    db.getCollection(collection).insert(dbo, wc);
  }

  public void close() {
    mongo.close();
  }

  public boolean requiresLayout() {
    return true;
  }
}
