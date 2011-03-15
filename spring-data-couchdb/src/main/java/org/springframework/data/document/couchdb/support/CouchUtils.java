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

package org.springframework.data.document.couchdb.support;

import org.springframework.dao.DataAccessException;

/**
 * Helper class featuring helper methods for internal CouchDB classes.
 * <p/>
 * <p>Mainly intended for internal use within the framework.
 *
 * @author Thomas Risberg
 * @author Tareq Abedrabbo
 * @since 1.0
 */
public abstract class CouchUtils {

  /**
   * Convert the given runtime exception to an appropriate exception from the
   * <code>org.springframework.dao</code> hierarchy.
   * Return null if no translation is appropriate: any other exception may
   * have resulted from user code, and should not be translated.
   *
   * @param ex runtime exception that occurred
   * @return the corresponding DataAccessException instance,
   *         or <code>null</code> if the exception should not be translated
   */
  public static DataAccessException translateCouchExceptionIfPossible(RuntimeException ex) {

    return null;
  }

  /**
   * Adds an id variable to a URL
   *
   * @param url the URL to modify
   * @return the modified URL
   */
  public static String addId(String url) {
    return ensureTrailingSlash(url) + "{id}";
  }


  /**
   * Adds a 'changes since' variable to a URL
   *
   * @param url
   * @return
   */
  public static String addChangesSince(String url) {
    return ensureTrailingSlash(url) + "_changes?since={seq}";
  }

  /**
   * Ensures that a URL ends with a slash.
   *
   * @param url the URL to modify
   * @return the modified URL
   */
  public static String ensureTrailingSlash(String url) {
    if (!url.endsWith("/")) {
      url += "/";
    }
    return url;
  }

}
