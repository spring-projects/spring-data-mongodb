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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.document.couchdb.CouchServerResourceUsageException;
import org.springframework.data.document.couchdb.CouchUsageException;
import org.springframework.data.document.couchdb.DocumentRetrievalFailureException;
import org.springframework.data.document.couchdb.UncategorizedCouchDataAccessException;
import org.springframework.data.document.couchdb.support.CouchUtils;
import org.springframework.http.*;
import org.springframework.util.Assert;
import org.springframework.web.client.*;


public class CouchTemplate implements CouchOperations {

  protected final Log logger = LogFactory.getLog(this.getClass());

  private String defaultDocumentUrl;

  private RestOperations restOperations = new RestTemplate();

  /**
   * Constructs an instance of CouchDbDocumentTemplate with a default database
   *
   * @param defaultDatabaseUrl the default database to connect to
   */
  public CouchTemplate(String defaultDatabaseUrl) {
    Assert.hasText(defaultDatabaseUrl, "defaultDatabaseUrl must not be empty");
    defaultDocumentUrl = CouchUtils.addId(defaultDatabaseUrl);
  }

  /**
   * Constructs an instance of CouchDbDocumentTemplate with a default database
   *
   * @param defaultDatabaseUrl the default database to connect to
   */
  public CouchTemplate(String defaultDatabaseUrl, RestOperations restOperations) {
    this(defaultDatabaseUrl);
    Assert.notNull(restOperations, "restOperations must not be null");
    this.restOperations = restOperations;
  }


  public <T> T findOne(String id, Class<T> targetClass) {
    Assert.state(defaultDocumentUrl != null, "defaultDatabaseUrl must be set to use this method");
    try {
      return restOperations.getForObject(defaultDocumentUrl, targetClass, id);
      //TODO check this exception translation and centralize.
    } catch (HttpClientErrorException clientError) {
      if (clientError.getStatusCode() == HttpStatus.NOT_FOUND) {
        throw new DocumentRetrievalFailureException(defaultDocumentUrl + "/" + id);
      }
      throw new CouchUsageException(clientError);
    } catch (HttpServerErrorException serverError) {
      throw new CouchServerResourceUsageException(serverError);
    } catch (RestClientException otherError) {
      throw new UncategorizedCouchDataAccessException(otherError);
    }
  }

  public <T> T findOne(URI uri, Class<T> targetClass) {
    Assert.state(uri != null, "uri must be set to use this method");
    try {
      return restOperations.getForObject(uri, targetClass);
      //TODO check this exception translation and centralize.
    } catch (HttpClientErrorException clientError) {
      if (clientError.getStatusCode() == HttpStatus.NOT_FOUND) {
        throw new DocumentRetrievalFailureException(uri.getPath());
      }
      throw new CouchUsageException(clientError);
    } catch (HttpServerErrorException serverError) {
      throw new CouchServerResourceUsageException(serverError);
    } catch (RestClientException otherError) {
      throw new UncategorizedCouchDataAccessException(otherError);
    }
  }

  public void save(String id, Object document) {
    Assert.notNull(document, "document must not be null for save");
    HttpEntity<?> httpEntity = createHttpEntity(document);
    try {
      ResponseEntity<Map> response = restOperations.exchange(defaultDocumentUrl, HttpMethod.PUT, httpEntity, Map.class, id);
      //TODO update the document revision id on the object from the returned value
      //TODO better exception translation
    } catch (RestClientException e) {
      throw new UncategorizedCouchDataAccessException(e);
    }

  }

  public void save(URI uri, Object document) {
    Assert.notNull(document, "document must not be null for save");
    Assert.notNull(uri, "URI must not be null for save");
    HttpEntity<?> httpEntity = createHttpEntity(document);
    try {
      ResponseEntity<Map> response = restOperations.exchange(uri, HttpMethod.PUT, httpEntity, Map.class);
      //TODO update the document revision id on the object from the returned value
      //TODO better exception translation
    } catch (RestClientException e) {
      throw new UncategorizedCouchDataAccessException(e);
    }


  }

  private HttpEntity<?> createHttpEntity(Object document) {

    if (document instanceof HttpEntity) {
      HttpEntity httpEntity = (HttpEntity) document;
      Assert.isTrue(httpEntity.getHeaders().getContentType().equals(MediaType.APPLICATION_JSON),
          "HttpEntity payload with non application/json content type found.");
      return httpEntity;
    }

    HttpHeaders httpHeaders = new HttpHeaders();
    httpHeaders.setContentType(MediaType.APPLICATION_JSON);
    HttpEntity<Object> httpEntity = new HttpEntity<Object>(document, httpHeaders);

    return httpEntity;
  }


}
