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
package org.springframework.data.document.couchdb.core;

import java.net.URI;


public interface CouchOperations {

  /**
   * Reads a document from the database and maps it a Java object.
   * </p>
   * This method is intended to work when a default database
   * is set on the CouchDbDocumentOperations instance.
   *
   * @param id          the id of the CouchDB document to read
   * @param targetClass the target type to map to
   * @return the mapped object
   */
  <T> T findOne(String id, Class<T> targetClass);

  /**
   * Reads a document from the database and maps it a Java object.
   *
   * @param uri         the full URI of the document to read
   * @param targetClass the target type to map to
   * @return the mapped object
   */
  <T> T findOne(URI uri, Class<T> targetClass);


  /**
   * Maps a Java object to JSON and writes it to the database
   * </p>
   * This method is intended to work when a default database
   * is set on the CouchDbDocumentOperations instance.
   *
   * @param id       the id of the document to write
   * @param document the object to write
   */
  void save(String id, Object document);

  /**
   * Maps a Java object to JSON and writes it to the database
   *
   * @param uri      the full URI of the document to write
   * @param document the object to write
   */
  void save(URI uri, Object document);
}
