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

import java.lang.reflect.Method;
import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.ui.ExtendedModelMap;
import org.springframework.web.context.request.ServletWebRequest;

public class ActionExecutingContext {

	
	private Object handler;
	
	private ServletWebRequest servletWebRequest;
	
	private ExtendedModelMap implicitModel;
	
	private Method handlerMethod;
	
	private Object[] handlerParameters;
	

	public ActionExecutingContext(ServletWebRequest servletWebRequest,
			Object handler, Method handlerMethod, Object[] handlerParameters,
			ExtendedModelMap implicitModel) {
		super();
		this.servletWebRequest = servletWebRequest;
		this.handler = handler;
		this.handlerMethod = handlerMethod;
		this.handlerParameters = handlerParameters;
		this.implicitModel = implicitModel;
	}

	public HttpServletRequest getHttpServletRequest() {
		return servletWebRequest.getRequest();
	}

	public HttpServletResponse getHttpServletResponse() {
		return servletWebRequest.getResponse();
	}

	public Object getHandler() {
		return handler;
	}

	public ServletWebRequest getServletWebRequest() {
		return servletWebRequest;
	}

	public ExtendedModelMap getImplicitModel() {
		return implicitModel;
	}

	public Method getHandlerMethod() {
		return handlerMethod;
	}

	public Object[] getHandlerParameters() {
		return handlerParameters;
	}
	
	@Override
	public String toString() {
		return "ActionExecutingContext [handler=" + handler
				+ ", servletWebRequest=" + servletWebRequest
				+ ", implicitModel=" + implicitModel + ", handlerMethod="
				+ handlerMethod + ", handlerParameters="
				+ Arrays.toString(handlerParameters) + "]";
	}
	

	
	
}
