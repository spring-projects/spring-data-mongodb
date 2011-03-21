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

package org.springframework.data.document.mongodb.mapping.event;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.data.document.mongodb.mapping.MongoPersistentEntity;
import org.springframework.data.document.mongodb.mapping.index.IndexCreationHelper;
import org.springframework.data.mapping.event.MappingContextEvent;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class IndexCreationListener implements ApplicationListener<MappingContextEvent>, ApplicationContextAware {

  private static final Logger log = LoggerFactory.getLogger(IndexCreationListener.class);

  private ApplicationContext applicationContext;
  @Autowired
  private IndexCreationHelper indexCreationHelper;
  private ExecutorService worker = Executors.newFixedThreadPool(1);
  private LinkedBlockingQueue<MappingContextEvent> mappingEvents = new LinkedBlockingQueue<MappingContextEvent>();

  public IndexCreationListener() {
    worker.submit(new IndexCreationWorker());
  }

  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  public IndexCreationHelper getIndexCreationHelper() {
    return indexCreationHelper;
  }

  public void setIndexCreationHelper(IndexCreationHelper indexCreationHelper) {
    this.indexCreationHelper = indexCreationHelper;
  }

  public void onApplicationEvent(MappingContextEvent event) {
    mappingEvents.add(event);
  }

  public void cleanUp() throws InterruptedException {
    while (mappingEvents.size() > 0) {
      Thread.yield();
    }
  }

  private class IndexCreationWorker implements Runnable {
    public void run() {
      while (true) {
        MappingContextEvent event = null;
        try {
          event = mappingEvents.take();
          if (null == applicationContext) {
            Thread.sleep(500);
            mappingEvents.add(event);
          }
        } catch (InterruptedException ignored) {
          if (log.isDebugEnabled()) {
            log.debug(ignored.getMessage(), ignored);
          }
          break;
        }
        if (event.getPersistentEntity() instanceof MongoPersistentEntity<?>) {
          MongoPersistentEntity<?> entity = (MongoPersistentEntity<?>) event.getPersistentEntity();
          indexCreationHelper.checkForIndexes(entity);
        }
      }
    }
  }

}
