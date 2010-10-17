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
package org.springframework.datastore.document.mongodb;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class SimpleMongoConverterTests {

	@Test
	public void notNestedObject() {
		User user = new User();
		user.setAccountName("My Account");
		user.setUserName("Mark");
		SimpleMongoConverter converter = createConverter();
		DBObject dbo = new BasicDBObject();
		converter.write(user, dbo);
		Assert.assertEquals("My Account", dbo.get("accountName"));
		Assert.assertEquals("Mark", dbo.get("userName"));

		User u = (User) converter.read(User.class, dbo);

		Assert.assertEquals("My Account", u.getAccountName());
		Assert.assertEquals("Mark", u.getUserName());
	}

	@Test
	public void nestedObject() {
		Portfolio p = createPortfolioWithNoTrades();
		SimpleMongoConverter converter = createConverter();
		DBObject dbo = new BasicDBObject();
		converter.write(p, dbo);
		Assert.assertEquals("High Risk Trading Account",
				dbo.get("portfolioName"));
		Assert.assertTrue(dbo.containsField("user"));

		Portfolio cp = (Portfolio) converter.read(Portfolio.class, dbo);
		Assert.assertEquals("High Risk Trading Account", cp.getPortfolioName());
		Assert.assertEquals("Joe Trader", cp.getUser().getUserName());
		Assert.assertEquals("ACCT-123", cp.getUser().getAccountName());

	}

	@Test
	public void objectWithMap() {
		Portfolio p = createPortfolioWithPositions();
		SimpleMongoConverter converter = createConverter();
		DBObject dbo = new BasicDBObject();
		converter.write(p, dbo);

		Portfolio cp = (Portfolio) converter.read(Portfolio.class, dbo);
		Assert.assertEquals("High Risk Trading Account", cp.getPortfolioName());
	}

	@Test
	public void objectWithMapContainingNonPrimitiveTypeAsValue() {
		Portfolio p = createPortfolioWithManagers();
		SimpleMongoConverter converter = createConverter();
		DBObject dbo = new BasicDBObject();
		converter.write(p, dbo);

		Portfolio cp = (Portfolio) converter.read(Portfolio.class, dbo);
		Assert.assertEquals("High Risk Trading Account", cp.getPortfolioName());
	}

	private SimpleMongoConverter createConverter() {
		SimpleMongoConverter converter = new SimpleMongoConverter();
		return converter;
	}

	protected Portfolio createPortfolioWithPositions() {

		Portfolio portfolio = new Portfolio();
		portfolio.setPortfolioName("High Risk Trading Account");
		Map<String, Integer> positions = new HashMap<String, Integer>();
		positions.put("CSCO", 1);
		portfolio.setPositions(positions);
		return portfolio;
	}

	protected Portfolio createPortfolioWithManagers() {

		Portfolio portfolio = new Portfolio();
		portfolio.setPortfolioName("High Risk Trading Account");
		Map<String, Person> managers = new HashMap<String, Person>();
		Person p1 = new Person();
		p1.setFirstName("Mark");
		managers.put("CSCO", p1);
		portfolio.setPortfolioManagers(managers);
		return portfolio;
	}

	protected Portfolio createPortfolioWithNoTrades() {
		Portfolio portfolio = new Portfolio();
		User user = new User();
		user.setUserName("Joe Trader");
		user.setAccountName("ACCT-123");
		portfolio.setUser(user);
		portfolio.setPortfolioName("High Risk Trading Account");
		return portfolio;
	}

	@Test
	public void testReflection() {
		Portfolio p = createPortfolioWithManagers();
		Method method = ReflectionUtils.findMethod(Portfolio.class, "setPortfolioManagers", Map.class);
		Assert.assertNotNull(method);
		List<Class> paramClass = getGenericParameterClass(method);
		System.out.println(paramClass);
		/*
		Type t = method.getGenericReturnType();
		if (t instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) t;
			Type paramType = pt.getActualTypeArguments()[1];
			if (paramType instanceof ParameterizedType) {
				ParameterizedType paramPtype = (ParameterizedType) pt;
				System.out.println(paramPtype.getRawType());
			}
		}*/
		// Assert.assertNotNull(null);

	}

	public List<Class> getGenericParameterClass(Method setMethod) {
		List<Class> actualGenericParameterTypes  = new ArrayList<Class>();
		Type[] genericParameterTypes = setMethod.getGenericParameterTypes();

		for(Type genericParameterType  : genericParameterTypes){		
		    if(genericParameterType  instanceof ParameterizedType){
		        ParameterizedType aType = (ParameterizedType) genericParameterType;
		        Type[] parameterArgTypes = aType.getActualTypeArguments();		        
		        for(Type parameterArgType : parameterArgTypes){
		        	if (parameterArgType instanceof GenericArrayType)
		            {
		                Class arrayType = (Class) ((GenericArrayType) parameterArgType).getGenericComponentType();
		                actualGenericParameterTypes.add(Array.newInstance(arrayType, 0).getClass());
		            }
		        	else {
		        		if (parameterArgType instanceof ParameterizedType) {
			        		ParameterizedType paramTypeArgs = (ParameterizedType) parameterArgType;
			        		actualGenericParameterTypes.add((Class)paramTypeArgs.getRawType());
			        	} else {
			        		 if (parameterArgType instanceof TypeVariable) {
			        			 throw new RuntimeException("Can not map " + ((TypeVariable) parameterArgType).getName());
			        		 } else {
			        			 if (parameterArgType instanceof Class) {
			        				 actualGenericParameterTypes.add((Class) parameterArgType);
			        			 } else  {
			        				 throw new RuntimeException("Can not map " + parameterArgType); 
			        			 }
			        		 }
			        	}
		        	}
		        	
		        }
		    }
		}
		return actualGenericParameterTypes;

	}


}
