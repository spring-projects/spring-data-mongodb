package org.springframework.data.mongodb.core.convert;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.TypeInformationMapper;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProfiledDefaultMongoTypeMapper extends DefaultTypeMapper<DBObject> implements MongoTypeMapper {

	public static final String DEFAULT_TYPE_KEY = "_class";
	@SuppressWarnings("rawtypes")
	private static final TypeInformation<List> LIST_TYPE_INFO = ClassTypeInformation.from(List.class);
	@SuppressWarnings("rawtypes")
	private static final TypeInformation<Map> MAP_TYPE_INFO = ClassTypeInformation.from(Map.class);
	private String typeKey = DEFAULT_TYPE_KEY;

	private final DefaultMongoTypeMapper.DBObjectTypeAliasAccessor typeAliasAccessor;

	private ConcurrentHashMap<Object, TypeInformation<?>> typeCache = new ConcurrentHashMap<Object, TypeInformation<?>>();

	public ProfiledDefaultMongoTypeMapper() {
		this(DefaultMongoTypeMapper.DEFAULT_TYPE_KEY, Arrays.asList(SimpleTypeInformationMapper.INSTANCE));
	}

	public ProfiledDefaultMongoTypeMapper(String typeKey) {
		this(typeKey, new DefaultMongoTypeMapper.DBObjectTypeAliasAccessor(typeKey), null, Arrays.asList(SimpleTypeInformationMapper.INSTANCE));
	}

	public ProfiledDefaultMongoTypeMapper(String typeKey, MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext) {
		this(typeKey, new DefaultMongoTypeMapper.DBObjectTypeAliasAccessor(typeKey), mappingContext, Arrays.asList(SimpleTypeInformationMapper.INSTANCE));
	}

	public ProfiledDefaultMongoTypeMapper(String typeKey, List<? extends TypeInformationMapper> mappers) {
		this(typeKey, new DefaultMongoTypeMapper.DBObjectTypeAliasAccessor(typeKey), null, mappers);
	}

	protected ProfiledDefaultMongoTypeMapper(String typeKey, DefaultMongoTypeMapper.DBObjectTypeAliasAccessor typeAliasAccessor, MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext, List<? extends TypeInformationMapper> mappers) {
		super(typeAliasAccessor, mappingContext, mappers);
		this.typeAliasAccessor = typeAliasAccessor;
		this.typeKey = typeKey;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.MongoTypeMapper#isTypeKey(java.lang.String)
	 */
	public boolean isTypeKey(String key) {
		return typeKey == null ? false : typeKey.equals(key);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.DefaultTypeMapper#getFallbackTypeFor(java.lang.Object)
	 */
	@Override
	protected TypeInformation<?> getFallbackTypeFor(DBObject source) {
		return source instanceof BasicDBList ? LIST_TYPE_INFO : MAP_TYPE_INFO;
	}

	@Override
	public <T> TypeInformation<? extends T> readType(DBObject source, TypeInformation<T> basicType) {

		Assert.notNull(source);
		Class<?> documentsTargetType = getDefaultedTypeToBeUsed(source);

		if (documentsTargetType == null) {
			return basicType;
		}

		Class<T> rawType = basicType == null ? null : basicType.getType();

		boolean isMoreConcreteCustomType = rawType == null ? true : rawType.isAssignableFrom(documentsTargetType)
				&& !rawType.equals(documentsTargetType);
		return isMoreConcreteCustomType ? (TypeInformation<? extends T>) ClassTypeInformation.from(documentsTargetType)
				: basicType;
	}

	private Class<?> getDefaultedTypeToBeUsed(DBObject source) {

		TypeInformation<?> documentsTargetTypeInformation = readType(source);
		documentsTargetTypeInformation = documentsTargetTypeInformation == null ? getFallbackTypeFor(source)
				: documentsTargetTypeInformation;
		return documentsTargetTypeInformation == null ? null : documentsTargetTypeInformation.getType();
	}

	@Override
	public TypeInformation<?> readType(DBObject source) {

		Object o = typeAliasAccessor.readAliasFrom(source);

		if (o == null) {
			return null;
		}

		TypeInformation<?> typeInformation;

		if ((typeInformation = typeCache.get(o)) == null) {
			typeInformation = super.readType(source);
			typeCache.put(o, typeInformation);
		}
		return typeInformation;
	}

}
