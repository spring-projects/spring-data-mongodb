/*
 * Copyright 2011-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.math.BigInteger;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.mongodb.core.convert.MongoConverters.BigIntegerToObjectIdConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.ObjectIdToBigIntegerConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.ObjectIdToStringConverter;
import org.springframework.data.mongodb.core.convert.MongoConverters.StringToObjectIdConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Base class for {@link MongoConverter} implementations. Sets up a {@link GenericConversionService} and populates basic
 * converters. Allows registering {@link CustomConversions}.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public abstract class AbstractMongoConverter implements MongoConverter, InitializingBean {

	protected final GenericConversionService conversionService;
	protected CustomConversions conversions = new MongoCustomConversions();
	protected EntityInstantiators instantiators = new EntityInstantiators();

	/**
	 * Creates a new {@link AbstractMongoConverter} using the given {@link GenericConversionService}.
	 *
	 * @param conversionService can be {@literal null} and defaults to {@link DefaultConversionService}.
	 */
	public AbstractMongoConverter(@Nullable GenericConversionService conversionService) {
		this.conversionService = conversionService == null ? new DefaultConversionService() : conversionService;
	}

	/**
	 * Registers the given custom conversions with the converter.
	 *
	 * @param conversions must not be {@literal null}.
	 */
	public void setCustomConversions(CustomConversions conversions) {

		Assert.notNull(conversions, "Conversions must not be null!");
		this.conversions = conversions;
	}

	/**
	 * Registers {@link EntityInstantiators} to customize entity instantiation.
	 *
	 * @param instantiators
	 */
	public void setInstantiators(@Nullable EntityInstantiators instantiators) {
		this.instantiators = instantiators == null ? new EntityInstantiators() : instantiators;
	}

	/**
	 * Registers additional converters that will be available when using the {@link ConversionService} directly (e.g. for
	 * id conversion). These converters are not custom conversions as they'd introduce unwanted conversions (e.g.
	 * ObjectId-to-String).
	 */
	private void initializeConverters() {

		conversionService.addConverter(ObjectIdToStringConverter.INSTANCE);
		conversionService.addConverter(StringToObjectIdConverter.INSTANCE);

		if (!conversionService.canConvert(ObjectId.class, BigInteger.class)) {
			conversionService.addConverter(ObjectIdToBigIntegerConverter.INSTANCE);
		}

		if (!conversionService.canConvert(BigInteger.class, ObjectId.class)) {
			conversionService.addConverter(BigIntegerToObjectIdConverter.INSTANCE);
		}

		conversions.registerConvertersIn(conversionService);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.core.convert.MongoConverter#getConversionService()
	 */
	@Override
	public ConversionService getConversionService() {
		return conversionService != null ? conversionService : new DefaultConversionService();
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() {
		initializeConverters();
	}
}
