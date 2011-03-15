/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.data.document.web.servlet;

import java.util.Arrays;

import org.springframework.web.servlet.ModelAndView;

public class ActionExecutedContext extends ActionExecutingContext {


  private ModelAndView modelAndView;

  private Exception exception;

  public ActionExecutedContext(ActionExecutingContext actionExecutingContext, ModelAndView modelAndView, Exception exception) {
    super(actionExecutingContext.getServletWebRequest(), actionExecutingContext.getHandler(),
        actionExecutingContext.getHandlerMethod(), actionExecutingContext.getHandlerParameters(),
        actionExecutingContext.getImplicitModel());
    this.modelAndView = modelAndView;
    this.exception = exception;
  }

  @Override
  public String toString() {
    return "ActionExecutedContext [handler=" + getHandler()
        + ", servletWebRequest=" + getServletWebRequest()
        + ", implicitModel=" + getImplicitModel() + ", handlerMethod="
        + getHandlerMethod() + ", handlerParameters="
        + Arrays.toString(getHandlerParameters()) + ",modelAndView=" + modelAndView
        + ", exception=" + exception + "]";
  }


  public ModelAndView getModelAndView() {
    return modelAndView;
  }


  public Exception getException() {
    return exception;
  }


}
