package org.springframework.data.mongodb.core;

import com.mongodb.BasicDBObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.mapping.event.*;

import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AnnotationMethodFinderTest {

    @Mock
    CallObject mockObject;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldFindAnnotationWithBeforeConvert(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotations(mockObject), new Object[0], OnBeforeConvert.class);
        verify(mockObject).before();
        verifyZeroInteractions(mockObject);
    }

    @Test
    public void shouldFindAnnotationWithBeforeDelete(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotations(mockObject), new Object[0], OnBeforeDelete.class);
        verify(mockObject).before();
    }

    @Test
    public void shouldFindAnnotationWithBeforeSave(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotations(mockObject), new Object[]{new BasicDBObject()}, OnBeforeSave.class);
        verify(mockObject).before();

    }

    @Test
    public void shouldFindAnnotationWithAfterSave(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotations(mockObject), new Object[]{new BasicDBObject()}, OnAfterSave.class);
        verify(mockObject).after();
    }

    @Test
    public void shouldThrowExWithWrongArgumentsAfterSave(){
        expectedException.expect(hasProperty("cause", instanceOf(IllegalArgumentException.class)));
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotations(mockObject), new Object[0], OnAfterSave.class);
        //verifyZeroInteractions(mockObject);
    }


    @Test
    public void shouldFindAnnotationWithAfterConvert(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotations(mockObject), new Object[]{new BasicDBObject()}, OnAfterConvert.class);
        verify(mockObject).after();
        verifyZeroInteractions(mockObject);
    }

    @Test
    public void shouldFindAnnotationWithAfterDelete(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotations(mockObject), new Object[]{new BasicDBObject()}, OnAfterDelete.class);
        verify(mockObject).after();
        verifyZeroInteractions(mockObject);
    }

    @Test
    public void shouldFindAnnotationWithAfterLoad(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotations(mockObject), new Object[]{new BasicDBObject()}, OnAfterLoad.class);
        verify(mockObject).after();
        verifyZeroInteractions(mockObject);
    }


    @Test
    public void shouldNotFindAnnotationAfterOrBefore(){
        AnnotationMethodFinder annotationMethodFinder = new AnnotationMethodFinder();
        PersonnWithoutAnnotation targetObject = new PersonnWithoutAnnotation(mockObject);

        annotationMethodFinder.executeMethodAnnotatedWith(targetObject, new Object[0], OnAfterLoad.class);
        annotationMethodFinder.executeMethodAnnotatedWith(targetObject, new Object[]{new BasicDBObject()}, OnAfterConvert.class);
        annotationMethodFinder.executeMethodAnnotatedWith(targetObject, new Object[0], OnAfterSave.class);
        annotationMethodFinder.executeMethodAnnotatedWith(targetObject, new Object[0], OnAfterDelete.class);

        annotationMethodFinder.executeMethodAnnotatedWith(targetObject, new Object[]{new BasicDBObject()}, OnBeforeSave.class);
        annotationMethodFinder.executeMethodAnnotatedWith(targetObject, new Object[0], OnBeforeDelete.class);
        annotationMethodFinder.executeMethodAnnotatedWith(targetObject, new Object[0], OnBeforeConvert.class);

        verifyZeroInteractions(mockObject);
    }


    @Test
    public void shouldFindAnnotationWithNoParametersOnAfterSave(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotationsAndNoParameters(mockObject),
                new Object[]{new BasicDBObject()}, OnAfterSave.class);
        verify(mockObject).after();

    }

    @Test
    public void shouldFindAnnotationWithNoParametersOnBeforeSave(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithAnnotationsAndNoParameters(mockObject),
                new Object[]{new BasicDBObject()}, OnBeforeSave.class);
        verify(mockObject).before();

    }



    public static interface CallObject{
        public void after();
        public void before();
    }

    public static class PersonnWithAnnotations {
        private CallObject injector;

        public PersonnWithAnnotations(CallObject injector) {
            this.injector = injector;
        }

        @OnBeforeConvert
        public void onbeforeConvert(){
            injector.before();
        }

        @OnBeforeDelete
        public void onBeforeDelete(){
            injector.before();
        }

        @OnBeforeSave
        public void onBeforeSave(BasicDBObject dbObject){
            injector.before();
        }

        @OnAfterConvert
        public void onAfterConvert(){
            injector.after();
        }

        @OnAfterDelete
        public void onAfterDelete(){
            injector.after();
        }

        @OnAfterSave
        public void onafterSave(BasicDBObject dbObject){
            injector.after();
        }

        @OnAfterLoad
        public void onAfterLoad(){
            injector.after();
        }
    }

    public static class PersonnWithAnnotationsAndNoParameters {

        private CallObject injector;

        public PersonnWithAnnotationsAndNoParameters(CallObject injector) {
            this.injector = injector;
        }

        @OnAfterSave
        public void onafterSave(){
            injector.after();
        }

        @OnBeforeSave
        public void onBeforeSave(){
            injector.before();
        }

    }

    public static class PersonnWithoutAnnotation {

        private CallObject injector;

        public PersonnWithoutAnnotation(CallObject injector) {
            this.injector = injector;
        }

        public void onbeforeConvert(){
            injector.before();
        }

        public void onafterSave(){
            injector.after();
        }
    }
}
