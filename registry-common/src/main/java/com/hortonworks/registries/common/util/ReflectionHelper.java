/*
 * Copyright 2016-2021 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.common.util;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import org.apache.commons.beanutils.MethodUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ReflectionHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ReflectionHelper.class);

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return (T) Class.forName(className).newInstance();
    }

    @SuppressWarnings("unchecked")
    public static <T> T invokeGetter(String propertyName, Object object) 
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        return (T) PropertyUtils.getProperty(object, propertyName);
    }

    public static void invokeSetter(String propertyName, Object object, Object valueToSet)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        try {
            PropertyUtils.setProperty(object, propertyName, valueToSet);
        } catch (IllegalArgumentException ex) {
            // try setters that accept super types
            Method method = MethodUtils.getMatchingAccessibleMethod(object.getClass(), "set" + StringUtils.capitalize(propertyName), new Class<?>[]{valueToSet.getClass()});
            if (method != null && method.getParameterCount() == 1 && method.getParameters()[0].getType().isAssignableFrom(valueToSet.getClass())) {
                method.invoke(object, valueToSet);
            } else {
                throw ex;
            }
        }

    }

    /**
     * Given a class, this method returns a map of names of all the instance (non static) fields to type.
     * if the class has any super class it also includes those fields.
     * @param clazz , not null
     * @return
     */
    public static Map<String, Class> getFieldNamesToTypes(Class clazz) {
        Field[] declaredFields = clazz.getDeclaredFields();
        Map<String, Class> instanceVariableNamesToTypes = new HashMap<>();
        for (Field field : declaredFields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                LOG.trace("clazz {} has field {} with type {}", clazz.getName(), field.getName(), field.getType().getName());
                instanceVariableNamesToTypes.put(field.getName(), field.getType());
            } else {
                LOG.trace("clazz {} has field {} with type {}, which is static so ignoring", 
                        clazz.getName(), field.getName(), field.getType().getName());
            }
        }

        if (!clazz.getSuperclass().equals(Object.class)) {
            instanceVariableNamesToTypes.putAll(getFieldNamesToTypes(clazz.getSuperclass()));
        }
        return instanceVariableNamesToTypes;
    }

    public static Collection<Class<?>> getAnnotatedClasses(String basePackage, Class<? extends Annotation> annotation) {
        ImmutableCollection.Builder<Class<?>> result = ImmutableList.builder();
        try {
            ImmutableSet<ClassPath.ClassInfo> classes = ClassPath.from(ReflectionHelper.class.getClassLoader()).getTopLevelClassesRecursive(basePackage);
            for (ClassPath.ClassInfo info : classes) {
                Class<?> clazz = info.load();
                for (Annotation actualAnnotation : clazz.getAnnotations()) {
                    if (actualAnnotation.annotationType() == annotation) {
                        result.add(clazz);
                        break;
                    }
                }
            }
        } catch (IOException iex) {
            throw new RuntimeException("Could not find classes under package " + basePackage, iex);
        }

        return result.build();
    }

    private ReflectionHelper() { }

}
