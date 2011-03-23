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

package org.springframework.data.document.mongodb.event;

import com.mongodb.WriteResult;
import org.springframework.context.ApplicationEvent;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class WriteResultEvent extends ApplicationEvent {

  private static final long serialVersionUID = 4460770341573211038L;

  public WriteResultEvent(WriteResult result) {
    super(result);
  }

  public WriteResult getWriteResult() {
    return (WriteResult) source;
  }
}
