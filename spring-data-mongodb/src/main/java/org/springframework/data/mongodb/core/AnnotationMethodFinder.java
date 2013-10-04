package org.springframework.data.mongodb.core;

import java.lang.reflect.Method;

public class AnnotationMethodFinder {

    public void executeMethodAnnotatedWith(Object targetObject, Object[] args, Class annotationMethod) {
        Method foundMethod = getMethodAnnotatedWith(targetObject, annotationMethod);
        executeMethod(targetObject, args, foundMethod);
    }

    protected void executeMethod(Object targetObject, Object[] args, Method foundMethod) {
        if(foundMethod == null){
            return;
        }

        try {
            if (foundMethod.getParameterTypes().length == 0) {
                foundMethod.invoke(targetObject);
            } else {
                foundMethod.invoke(targetObject, args);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Method getMethodAnnotatedWith(Object target, Class annotation){
        for (Method method : target.getClass().getMethods()) {
            if (method.isAnnotationPresent(annotation)) {
                  return method;
            }
        }

        return null;
    }
}
