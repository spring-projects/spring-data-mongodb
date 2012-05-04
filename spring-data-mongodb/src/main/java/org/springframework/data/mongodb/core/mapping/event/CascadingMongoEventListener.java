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

/**
 * Performs cascade operations on child objects annotated with {@link DBRef}
 *
 * @author Maciej Walkowiak
 */
public class CascadingMongoEventListener extends AbstractMongoEventListener {
	private static final Logger LOG = LoggerFactory.getLogger(CascadingMongoEventListener.class);

	@Autowired
	private MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	@Autowired
	private MongoConverter mongoConverter;

	@Autowired
	private MongoOperations mongoOperations;

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
		void handle(Object referencedObject) {
			final MongoPersistentEntity<?> referencedObjectEntity = mappingContext.getPersistentEntity(referencedObject.getClass());

			if (referencedObjectEntity.getIdProperty() == null) {
				throw new MappingException("Cannot perform cascade save on child object without id set");
			}

			mongoOperations.save(referencedObject);
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
		void handle(Object referredObject) {
			mongoOperations.remove(referredObject);
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

		abstract void handle(Object referencedObject);
	}
}
