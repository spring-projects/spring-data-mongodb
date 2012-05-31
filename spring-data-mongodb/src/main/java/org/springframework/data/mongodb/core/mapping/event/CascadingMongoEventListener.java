package org.springframework.data.mongodb.core.mapping.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;

import java.util.Collection;

/**
 * Performs cascade operations on child objects annotated with {@link DBRef}
 *
 * @author Maciej Walkowiak
 */
public class CascadingMongoEventListener extends AbstractMongoEventListener {
	private static final Logger LOG = LoggerFactory.getLogger(CascadingMongoEventListener.class);

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoConverter mongoConverter;
	private final MongoOperations mongoOperations;

	public CascadingMongoEventListener(MappingContext<? extends MongoPersistentEntity<?>,
			MongoPersistentProperty> mappingContext, MongoConverter mongoConverter, MongoOperations mongoOperations) {

		Assert.notNull(mappingContext);
		Assert.notNull(mongoConverter);
		Assert.notNull(mongoOperations);

		this.mappingContext = mappingContext;
		this.mongoConverter = mongoConverter;
		this.mongoOperations = mongoOperations;
	}

	/**
	 * Executes {@link CascadeSaveAssociationHandler} on each property annotated with {@link DBRef}
	 * with cascadeType delete
	 *
	 * @param source object that is going to be converted
	 */
	@Override
	public void onBeforeConvert(final Object source) {
		LOG.debug("before convert: {}", source);

		final MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(source.getClass());

		persistentEntity.doWithAssociations(new CascadeSaveAssociationHandler(source));
	}

	/**
	 * Executes {@link CascadeDeleteAssociationHandler} on each property annotated with {@link DBRef}
	 * with cascadeType save
	 *
	 * @param source object that has just been deleted
	 */
	@Override
	public void onAfterDelete(Object source) {
		final MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(source.getClass());

		persistentEntity.doWithAssociations(new CascadeDeleteAssociationHandler(source));
	}

	/**
	 * Executes {@link MongoOperations#save(Object)} on referred objects
	 */
	private class CascadeSaveAssociationHandler extends AbstractCascadeAssociationHandler {
		private CascadeSaveAssociationHandler(Object source) {
			super(source);
		}

		@Override
		boolean isAppliable(DBRef dbRef) {
			return dbRef.cascadeType().isSave();
		}

		@Override
		void handleObject(Object referencedObject) {
			final MongoPersistentEntity<?> referencedObjectEntity = mappingContext.getPersistentEntity(referencedObject.getClass());

			if (referencedObjectEntity.getIdProperty() == null) {
				throw new MappingException("Cannot perform cascade save on child object without id set");
			}
			mongoOperations.save(referencedObject);
		}

		@Override
		void handleCollection(Collection collection) {
			for (Object objectToSave : collection) {
				mongoOperations.save(objectToSave);
			}
		}
	}

	/**
	 * Executes {@link MongoOperations#remove(Object)} on referred objects
	 */
	private class CascadeDeleteAssociationHandler extends AbstractCascadeAssociationHandler {
		public CascadeDeleteAssociationHandler(Object source) {
			super(source);
		}

		@Override
		boolean isAppliable(DBRef dbRef) {
			return dbRef.cascadeType().isDelete();
		}

		@Override
		void handleObject(Object referencedObject) {
			mongoOperations.remove(referencedObject);
		}

		@Override
		void handleCollection(Collection collection) {
			for (Object objectToDelete : collection) {
				mongoOperations.remove(objectToDelete);
			}
		}
	}

	private abstract class AbstractCascadeAssociationHandler implements AssociationHandler<MongoPersistentProperty> {
		protected Object source;

		protected AbstractCascadeAssociationHandler(Object source) {
			this.source = source;
		}

		public void doWithAssociation(Association<MongoPersistentProperty> mongoPersistentPropertyAssociation) {
			MongoPersistentProperty persistentProperty = mongoPersistentPropertyAssociation.getInverse();

			if (persistentProperty != null) {
				DBRef dbRef = persistentProperty.getDBRef();

				if (isAppliable(dbRef)) {
					Object referencedObject = getReferredObject(persistentProperty);

					if (referencedObject != null) {
						handle(referencedObject);
					}
				}
			}
		}

		private Object getReferredObject(MongoPersistentProperty mongoPersistentProperty) {
			ConversionService service = mongoConverter.getConversionService();

			return BeanWrapper.create(source, service).getProperty(mongoPersistentProperty, Object.class, true);
		}

		abstract boolean isAppliable(DBRef dbRef);

		void handle(Object referencedObject) {
			if (referencedObject instanceof Collection) {
				handleCollection((Collection) referencedObject);
			} else {
				handleObject(referencedObject);
			}
		}

		abstract void handleObject(Object referencedObject);

		abstract void handleCollection(Collection referencedObject);
	}
}