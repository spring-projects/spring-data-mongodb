package org.springframework.data.mongodb.core;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;


public class CacheAnnotationMethodFinder extends AnnotationMethodFinder {

    private ConcurrentHashMap<KeyCacheAnnotation, WrapperMethod> cacheMethod = new ConcurrentHashMap<KeyCacheAnnotation, WrapperMethod>();

    public void executeMethodAnnotatedWith(Object targetObject, Object[] args, Class annotationMethod) {
        KeyCacheAnnotation key = new KeyCacheAnnotation(targetObject.getClass(), annotationMethod);
        Method foundMethod = getMethod(targetObject, annotationMethod, key);

        super.executeMethod(targetObject, args, foundMethod);
    }

    private Method getMethod(Object targetObject, Class annotationMethod, KeyCacheAnnotation key) {
        WrapperMethod foundMethod = cacheMethod.get(key);

        if (foundMethod == null) {
            Method method = super.getMethodAnnotatedWith(targetObject, annotationMethod);
            foundMethod = new WrapperMethod(method);
            cacheMethod.putIfAbsent(key, foundMethod);
        }

        return foundMethod.getMethod();
    }

    public static final class WrapperMethod {

        private Method method;

        public WrapperMethod(Method foundMethod) {
            this.method = foundMethod;
        }

        public Method getMethod() {
            return method;
        }
    }

    public static final class KeyCacheAnnotation {

        private Class targetClass;
        private Class annotationClazz;

        public KeyCacheAnnotation(Class targetClass, Class annotationClazz) {
            this.targetClass = targetClass;
            this.annotationClazz = annotationClazz;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            KeyCacheAnnotation that = (KeyCacheAnnotation) o;

            if (annotationClazz != null ? !annotationClazz.equals(that.annotationClazz) : that.annotationClazz != null) return false;
            if (targetClass != null ? !targetClass.equals(that.targetClass) : that.targetClass != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = targetClass != null ? targetClass.hashCode() : 0;
            result = 31 * result + (annotationClazz != null ? annotationClazz.hashCode() : 0);
            return result;
        }
    }

}

