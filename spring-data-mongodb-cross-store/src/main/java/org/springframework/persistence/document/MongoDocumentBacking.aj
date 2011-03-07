package org.springframework.persistence.document;

import org.springframework.persistence.support.AbstractDeferredUpdateMixinFields;

/**
 * Aspect to turn an object annotated with DocumentEntity into a document entity using Mongo.
 * 
 * @author Thomas Risberg
 */
public aspect MongoDocumentBacking extends AbstractDeferredUpdateMixinFields<DocumentEntity> {

}
