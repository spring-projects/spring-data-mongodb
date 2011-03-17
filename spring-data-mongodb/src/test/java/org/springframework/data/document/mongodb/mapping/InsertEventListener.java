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

package org.springframework.data.document.mongodb.mapping;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.data.document.mongodb.event.InsertEvent;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class InsertEventListener implements ApplicationListener<InsertEvent> {

  private Logger log = LoggerFactory.getLogger(getClass());
  private AtomicInteger counter = new AtomicInteger(0);

  public void onApplicationEvent(InsertEvent event) {
    log.info("Got INSERT event: " + event);
    counter.incrementAndGet();
  }

  public int getCount() {
    return counter.get();
  }
  
}
