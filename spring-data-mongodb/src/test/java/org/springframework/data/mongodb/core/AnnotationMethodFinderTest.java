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
    public void shouldNotFindAnnotationWithAfter(){
        new AnnotationMethodFinder().executeMethodAnnotatedWith(new PersonnWithoutAnnotation(mockObject), new Object[0], OnBeforeConvert.class);

        verifyZeroInteractions(mockObject);
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

        @OnAfterSave
        public void onafterSave(BasicDBObject dbObject){
            injector.after();
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
