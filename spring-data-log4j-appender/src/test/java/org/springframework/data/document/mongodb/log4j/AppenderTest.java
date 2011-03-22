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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class AppenderTest {

  private static final String NAME = AppenderTest.class.getName();
  private Logger log = Logger.getLogger(NAME);
  private Mongo mongo;
  private DB db;

  @Before
  public void setup() {
    try {
      mongo = new Mongo("localhost", 27017);
      db = mongo.getDB("logs");
      db.getCollection(NAME).drop();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Test
  public void testLogging() {
    log.debug("DEBUG message");
    log.info("INFO message");
    log.warn("WARN message");
    log.error("ERROR message");

    DBCursor msgs = db.getCollection(NAME).find();
    assertThat(msgs.count(), is(4));

  }

  @Test
  public void testProperties() {
    MDC.put("property", "one");
    log.debug("DEBUG message");
  }

}
