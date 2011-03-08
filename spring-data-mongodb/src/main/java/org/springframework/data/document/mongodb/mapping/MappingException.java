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

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MappingException extends RuntimeException {

  private final Object source;

  public MappingException(String s) {
    super(s);
    this.source = null;
  }

  public MappingException(String s, Object source) {
    super(String.format("Error encountered mapping object: %s", source));
    this.source = source;
  }

  public MappingException(String s, Throwable throwable, Object source) {
    super(s, throwable);
    this.source = source;
  }

  public Object getSource() {
    return source;
  }

}
