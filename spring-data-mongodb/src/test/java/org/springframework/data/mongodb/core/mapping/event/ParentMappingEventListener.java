/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.springframework.data.mongodb.core.mapping.event;

import java.util.ArrayList;

/**
 *
 * @author Jordi Llach
 */
public class ParentMappingEventListener  extends AbstractMongoEventListener<Parent> {
    
	public final ArrayList<AfterLoadEvent<Parent>> onAfterLoadEvents = new ArrayList<AfterLoadEvent<Parent>>();
	public final ArrayList<AfterConvertEvent<Parent>> onAfterConvertEvents = new ArrayList<AfterConvertEvent<Parent>>();

	@Override
	public void onAfterLoad(AfterLoadEvent<Parent> event) {
		onAfterLoadEvents.add(event);
	}

	@Override
	public void onAfterConvert(AfterConvertEvent<Parent> event) {
		onAfterConvertEvents.add(event);
	}
}