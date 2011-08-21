package org.springframework.data.mongodb.repository;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.springframework.core.MethodParameter;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;

/**
 * Custom extension of {@link Parameters} discovering additional
 *
 * @author Oliver Gierke
 */
public class MongoParameters extends Parameters {

	private int distanceIndex = -1;
	
	public MongoParameters(Method method) {
		
		super(method);
		this.distanceIndex = Arrays.asList(method.getParameterTypes()).indexOf(Distance.class);
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createParameter(org.springframework.core.MethodParameter)
	 */
	@Override
	protected Parameter createParameter(MethodParameter parameter) {
		return new MongoParameter(parameter, this);
	}

	public int getDistanceIndex() {
		return distanceIndex;
	}
	
	/**
	 * Custom {@link Parameter} implementation adding parameters of type {@link Distance} to the special ones.
	 *
	 * @author Oliver Gierke
	 */
	static class MongoParameter extends Parameter {
		
		/**
		 * 
		 * @param parameter
		 * @param parameters
		 */
		MongoParameter(MethodParameter parameter, Parameters parameters) {
			super(parameter, parameters);
		}
		
		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.Parameter#isSpecialParameter()
		 */
		@Override
		public boolean isSpecialParameter() {
			return super.isSpecialParameter() || getType().equals(Distance.class);
		}
	}
}
