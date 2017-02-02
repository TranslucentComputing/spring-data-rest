/*
 * Copyright 2014-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.rest.webmvc.json;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.beans.PropertyAccessor;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.SimpleAssociationHandler;
import org.springframework.data.mapping.SimplePropertyHandler;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.rest.webmvc.mapping.Associations;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.persistence.*;

/**
 * Component to apply an {@link ObjectNode} to an existing domain object. This is effectively a best-effort workaround
 * for Jackson's inability to apply a (partial) JSON document to an existing object in a deeply nested way. We manually
 * detect nested objects, lookup the original value and apply the merge recursively.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Craig Andrews
 * @author Mathias Düsterhöft
 * @since 2.2
 */
@RequiredArgsConstructor
public class DomainObjectReader {

    private final
    @NonNull
    PersistentEntities entities;
    private final
    @NonNull
    Associations associationLinks;

    /**
     * Reads the given input stream into an {@link ObjectNode} and applies that to the given existing instance.
     *
     * @param source must not be {@literal null}.
     * @param target must not be {@literal null}.
     * @param mapper must not be {@literal null}.
     * @return
     */
    public <T> T read(InputStream source, T target, ObjectMapper mapper) {

        Assert.notNull(target, "Target object must not be null!");
        Assert.notNull(source, "InputStream must not be null!");
        Assert.notNull(mapper, "ObjectMapper must not be null!");

        try {
            return doMerge((ObjectNode) mapper.readTree(source), target, mapper);
        } catch (Exception o_O) {
            throw new HttpMessageNotReadableException("Could not read payload!", o_O);
        }
    }

    /**
     * Reads the given source node onto the given target object and applies PUT semantics, i.e. explicitly
     *
     * @param source must not be {@literal null}.
     * @param target must not be {@literal null}.
     * @param mapper
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> T readPut(final ObjectNode source, T target, final ObjectMapper mapper) {

        Assert.notNull(source, "ObjectNode must not be null!");
        Assert.notNull(target, "Existing object instance must not be null!");
        Assert.notNull(mapper, "ObjectMapper must not be null!");

        Class<? extends Object> type = target.getClass();

        final PersistentEntity<?, ?> entity = entities.getPersistentEntity(type);

        Assert.notNull(entity, "No PersistentEntity found for ".concat(type.getName()).concat("!"));

        try {

            Object intermediate = mapper.readerFor(target.getClass()).readValue(source);
            return (T) mergeForPut(intermediate, target, mapper, false);

        } catch (Exception o_O) {
            throw new HttpMessageNotReadableException("Could not read payload!", o_O);
        }
    }

    /**
     * Merges the state of given source object onto the target one preserving PUT semantics.
     *
     * @param source        can be {@literal null}.
     * @param target        can be {@literal null}.
     * @param mapper        must not be {@literal null}.
     * @param bidirectional
     * @return
     */
    <T> T mergeForPut(T source, T target, final ObjectMapper mapper, boolean bidirectional) {

        Assert.notNull(mapper, "ObjectMapper must not be null!");

        if (target == null || source == null) {
            return source;
        }

        Class<? extends Object> type = target.getClass();

        PersistentEntity<?, ?> entity = entities.getPersistentEntity(type);

        if (entity == null) {
            return source;
        }

        Assert.notNull(entity, "No PersistentEntity found for ".concat(type.getName()).concat("!"));

        MergingPropertyHandler propertyHandler = new MergingPropertyHandler(source, target, entity, mapper, bidirectional);

        entity.doWithProperties(propertyHandler);
        entity.doWithAssociations(new LinkedAssociationAssociationHandler(associationLinks, propertyHandler));

        // Need to copy unmapped properties as the PersistentProperty model currently does not contain any transient
        // properties
        copyRemainingProperties(propertyHandler.getProperties(), source, target);

        return target;
    }

    <T> T mergeForPutNoAssociations(T source, T target, final ObjectMapper mapper) {
        Assert.notNull(mapper, "ObjectMapper must not be null!");

        if (target == null || source == null) {
            return source;
        }

        Class<? extends Object> type = target.getClass();

        PersistentEntity<?, ?> entity = entities.getPersistentEntity(type);

        if (entity == null) {
            return source;
        }

        Assert.notNull(entity, "No PersistentEntity found for ".concat(type.getName()).concat("!"));

        MergingPropertyHandler propertyHandler = new MergingPropertyHandler(source, target, entity, mapper, false);

        entity.doWithProperties(propertyHandler);

        return target;
    }


    /**
     * Copies the unmapped properties of the given {@link MappedProperties} from the source object to the target instance.
     *
     * @param properties must not be {@literal null}.
     * @param source     must not be {@literal null}.
     * @param target     must not be {@literal null}.
     */
    private static void copyRemainingProperties(MappedProperties properties, Object source, Object target) {

        PropertyAccessor sourceFieldAccessor = PropertyAccessorFactory.forDirectFieldAccess(source);
        PropertyAccessor sourcePropertyAccessor = PropertyAccessorFactory.forBeanPropertyAccess(source);
        PropertyAccessor targetFieldAccessor = PropertyAccessorFactory.forDirectFieldAccess(target);
        PropertyAccessor targetPropertyAccessor = PropertyAccessorFactory.forBeanPropertyAccess(target);

        for (String property : properties.getSpringDataUnmappedProperties()) {

            // If there's a field we can just copy it.
            if (targetFieldAccessor.isWritableProperty(property)) {
                targetFieldAccessor.setPropertyValue(property, sourceFieldAccessor.getPropertyValue(property));
                continue;
            }

            // Otherwise only copy if there's both a getter and setter.
            if (targetPropertyAccessor.isWritableProperty(property) && sourcePropertyAccessor.isReadableProperty(property)) {
                targetPropertyAccessor.setPropertyValue(property, sourcePropertyAccessor.getPropertyValue(property));
            }
        }
    }

    public <T> T merge(ObjectNode source, T target, ObjectMapper mapper) {

        try {
            return doMerge(source, target, mapper);
        } catch (Exception o_O) {
            throw new HttpMessageNotReadableException("Could not read payload!", o_O);
        }
    }

    /**
     * Merges the given {@link ObjectNode} onto the given object.
     *
     * @param root   must not be {@literal null}.
     * @param target must not be {@literal null}.
     * @param mapper must not be {@literal null}.
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    <T> T doMerge(ObjectNode root, T target, ObjectMapper mapper) throws Exception {

        Assert.notNull(root, "Root ObjectNode must not be null!");
        Assert.notNull(target, "Target object instance must not be null!");
        Assert.notNull(mapper, "ObjectMapper must not be null!");

        PersistentEntity<?, ?> entity = entities.getPersistentEntity(target.getClass());

        if (entity == null) {
            return mapper.readerForUpdating(target).readValue(root);
        }

        MappedProperties mappedProperties = MappedProperties.fromJacksonProperties(entity, mapper);

        for (Iterator<Entry<String, JsonNode>> i = root.fields(); i.hasNext(); ) {

            Entry<String, JsonNode> entry = i.next();
            JsonNode child = entry.getValue();
            String fieldName = entry.getKey();

            if (!mappedProperties.hasPersistentPropertyForField(fieldName)) {
                continue;
            }

            PersistentProperty<?> property = mappedProperties.getPersistentProperty(fieldName);
            PersistentPropertyAccessor accessor = entity.getPropertyAccessor(target);
            Object rawValue = accessor.getProperty(property);

            if (rawValue == null) {
                continue;
            }

            if (child.isArray()) {

                if (handleArray(child, rawValue, mapper, property.getTypeInformation())) {
                    i.remove();
                }

                continue;
            }

            if (child.isObject()) {

                if (associationLinks.isLinkableAssociation(property)) {
                    continue;
                }

                ObjectNode objectNode = (ObjectNode) child;

                if (property.isMap()) {

                    // Keep empty Map to wipe it as expected
                    if (!objectNode.fieldNames().hasNext()) {
                        continue;
                    }

                    doMergeNestedMap((Map<Object, Object>) rawValue, objectNode, mapper, property.getTypeInformation());

                    // Remove potentially emptied Map as values have been handled recursively
                    if (!objectNode.fieldNames().hasNext()) {
                        i.remove();
                    }

                    continue;
                }

                if (property.isEntity()) {
                    i.remove();
                    doMerge(objectNode, rawValue, mapper);
                }
            }
        }

        return mapper.readerForUpdating(target).readValue(root);
    }

    /**
     * Handles the given {@link JsonNode} by treating it as {@link ArrayNode} and the given source value as
     * {@link Collection}-like value. Looks up the actual type to handle from the potentially available first element,
     * falling back to component type lookup on the given type.
     *
     * @param node           must not be {@literal null}.
     * @param source         must not be {@literal null}.
     * @param mapper         must not be {@literal null}.
     * @param collectionType must not be {@literal null}.
     * @return
     * @throws Exception
     */
    private boolean handleArray(JsonNode node, Object source, ObjectMapper mapper, TypeInformation<?> collectionType)
            throws Exception {

        Collection<Object> collection = ifCollection(source);

        if (collection == null) {
            return false;
        }

        return handleArrayNode((ArrayNode) node, collection, mapper, collectionType.getComponentType());
    }

    /**
     * Applies the diff handling to {@link ArrayNode}s, potentially recursing into nested ones.
     *
     * @param array         the source {@link ArrayNode}m, must not be {@literal null}.
     * @param collection    the actual collection values, must not be {@literal null}.
     * @param mapper        the {@link ObjectMapper} to use, must not be {@literal null}.
     * @param componentType the item type of the collection, can be {@literal null}.
     * @return whether an object merge has been applied to the {@link ArrayNode}.
     */
    private boolean handleArrayNode(ArrayNode array, Collection<Object> collection, ObjectMapper mapper,
                                    TypeInformation<?> componentType) throws Exception {

        Assert.notNull(array, "ArrayNode must not be null!");
        Assert.notNull(collection, "Source collection must not be null!");
        Assert.notNull(mapper, "ObjectMapper must not be null!");

        // We need an iterator for the original collection.
        // We might modify it but we want to keep iterating over the original collection.
        Iterator<Object> value = new ArrayList<Object>(collection).iterator();
        boolean nestedObjectFound = false;

        for (JsonNode jsonNode : array) {

            if (!value.hasNext()) {

                collection.add(mapper.treeToValue(jsonNode, getTypeToMap(null, componentType).getType()));

                continue;
            }

            Object next = value.next();

            if (ArrayNode.class.isInstance(jsonNode)) {
                return handleArray(jsonNode, next, mapper, getTypeToMap(value, componentType));
            }

            if (ObjectNode.class.isInstance(jsonNode)) {

                nestedObjectFound = true;
                doMerge((ObjectNode) jsonNode, next, mapper);
            }
        }

        // there are more items in the collection than contained in the JSON node - remove it.
        while (value.hasNext()) {
            collection.remove(value.next());
        }

        return nestedObjectFound;
    }

    /**
     * Merges nested {@link Map} values for the given source {@link Map}, the {@link ObjectNode} and {@link ObjectMapper}.
     *
     * @param source can be {@literal null}.
     * @param node   must not be {@literal null}.
     * @param mapper must not be {@literal null}.
     * @throws Exception
     */
    private void doMergeNestedMap(Map<Object, Object> source, ObjectNode node, ObjectMapper mapper,
                                  TypeInformation<?> type) throws Exception {

        if (source == null) {
            return;
        }

        Iterator<Entry<String, JsonNode>> fields = node.fields();
        Class<?> keyType = typeOrObject(type.getComponentType());
        TypeInformation<?> valueType = type.getMapValueType();

        while (fields.hasNext()) {

            Entry<String, JsonNode> entry = fields.next();
            JsonNode value = entry.getValue();
            String key = entry.getKey();

            Object mappedKey = mapper.readValue(quote(key), keyType);
            Object sourceValue = source.get(mappedKey);
            TypeInformation<?> typeToMap = getTypeToMap(sourceValue, valueType);

            if (value instanceof ObjectNode && sourceValue != null) {

                doMerge((ObjectNode) value, sourceValue, mapper);

            } else if (value instanceof ArrayNode && sourceValue != null) {

                handleArray(value, sourceValue, mapper, getTypeToMap(sourceValue, typeToMap));

            } else {

                source.put(mappedKey, mapper.treeToValue(value, typeToMap.getType()));
            }

            fields.remove();
        }
    }

    /**
     * Check if the mappedBy is set on the associations. Check only OneToMany and ManyToMany associations.
     *
     * @param property Property to check
     * @return True if mappedBy is set on the associations
     */
    private boolean isBidirectionalAssociation(PersistentProperty<?> property) {
        OneToMany oneToMany = property.findAnnotation(OneToMany.class);
        if (oneToMany != null) {
            return oneToMany.mappedBy() != null;
        }

        ManyToMany manyToMany = property.findAnnotation(ManyToMany.class);
        return manyToMany != null && manyToMany.mappedBy() != null;
    }

    @SuppressWarnings("unchecked")
    private Map<Object, Object> mergeMaps(PersistentProperty<?> property, Object source, Object target,
                                          ObjectMapper mapper) {

        Map<Object, Object> sourceMap = (Map<Object, Object>) source;

        if (sourceMap == null) {
            return null;
        }

        Map<Object, Object> targetMap = (Map<Object, Object>) target;
        Map<Object, Object> result = targetMap == null ? CollectionFactory.createMap(Map.class, sourceMap.size())
                : CollectionFactory.createApproximateMap(targetMap, sourceMap.size());

        //check if bidirectional mapping
        boolean bidirectional = isBidirectionalAssociation(property);

        for (Entry<Object, Object> entry : sourceMap.entrySet()) {

            Object targetValue = targetMap == null ? null : targetMap.get(entry.getKey());
            result.put(entry.getKey(), mergeForPut(entry.getValue(), targetValue, mapper, bidirectional));
        }

        if (targetMap == null) {
            return result;
        }

        try {

            targetMap.clear();
            targetMap.putAll(result);

            return targetMap;

        } catch (UnsupportedOperationException o_O) {
            return result;
        }
    }

    private Collection<Object> mergeCollections(PersistentProperty<?> property, Object source, Object target,
                                                ObjectMapper mapper) {

        Collection<Object> sourceCollection = asCollection(source);

        if (sourceCollection == null) {
            return null;
        }

        Collection<Object> targetCollection = asCollection(target);
        Collection<Object> result = targetCollection == null
                ? CollectionFactory.createCollection(Collection.class, sourceCollection.size())
                : CollectionFactory.createApproximateCollection(targetCollection, sourceCollection.size());

        Iterator<Object> sourceIterator = sourceCollection.iterator();
        Iterator<Object> targetIterator = targetCollection == null ? Collections.emptyIterator()
                : targetCollection.iterator();

        //check if bidirectional mapping
        boolean bidirectional = isBidirectionalAssociation(property);

        while (sourceIterator.hasNext()) {

            Object sourceElement = sourceIterator.next();
            Object targetElement = targetIterator.hasNext() ? targetIterator.next() : null;

            result.add(mergeForPut(sourceElement, targetElement, mapper, bidirectional));
        }

        if (targetCollection == null) {
            return result;
        }

        try {

            targetCollection.clear();
            targetCollection.addAll(result);

            return targetCollection;

        } catch (UnsupportedOperationException o_O) {
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    private static Collection<Object> asCollection(Object source) {

        if (source == null) {
            return null;
        } else if (source instanceof Collection) {
            return (Collection<Object>) source;
        } else if (source.getClass().isArray()) {
            return Arrays.asList(ObjectUtils.toObjectArray(source));
        } else {
            return Collections.singleton(source);
        }
    }

    /**
     * Returns the given source instance as {@link Collection} or creates a new one for the given type.
     *
     * @param source can be {@literal null}.
     * @return
     */
    @SuppressWarnings("unchecked")
    private static Collection<Object> ifCollection(Object source) {

        Assert.notNull(source, "Source instance must not be null!");

        if (source instanceof Collection) {
            return (Collection<Object>) source;
        }

        if (source.getClass().isArray()) {
            return Arrays.asList((Object[]) source);
        }

        return null;
    }

    /**
     * Surrounds the given source {@link String} with quotes so that they represent a valid JSON String.
     *
     * @param source can be {@literal null}.
     * @return
     */
    private static String quote(String source) {
        return source == null ? null : "\"".concat(source).concat("\"");
    }

    /**
     * Returns the raw type of the given {@link TypeInformation} or {@link Object} as fallback.
     *
     * @param type can be {@literal null}.
     * @return
     */
    private static Class<?> typeOrObject(TypeInformation<?> type) {
        return type == null ? Object.class : type.getType();
    }

    /**
     * Returns the type to read for the given value and default type. The type will be defaulted to {@link Object} if
     * missing. If the given value's type is different from the given default (i.e. more concrete) the value's type will
     * be used.
     *
     * @param value can be {@literal null}.
     * @param type  can be {@literal null}.
     * @return
     */
    private static TypeInformation<?> getTypeToMap(Object value, TypeInformation<?> type) {

        if (type == null) {
            type = ClassTypeInformation.OBJECT;
        }

        if (value == null) {
            return type;
        }

        if (Enum.class.isInstance(value)) {
            return ClassTypeInformation.from(((Enum<?>) value).getDeclaringClass());
        }

        return value.getClass().equals(type.getType()) ? type : ClassTypeInformation.from(value.getClass());
    }

    /**
     * {@link SimpleAssociationHandler} that does not skips linkable associations and forwards handling for all other ones to the
     * delegate {@link SimplePropertyHandler}.
     *
     * @author Oliver Gierke
     */
    @RequiredArgsConstructor
    private final class LinkedAssociationAssociationHandler implements SimpleAssociationHandler {

        private final
        @NonNull
        Associations associations;
        private final
        @NonNull
        SimplePropertyHandler delegate;

        /*
         * (non-Javadoc)
         * @see org.springframework.data.mapping.SimpleAssociationHandler#doWithAssociation(org.springframework.data.mapping.Association)
         */
        @Override
        public void doWithAssociation(Association<? extends PersistentProperty<?>> association) {

            delegate.doWithPersistentProperty(association.getInverse());
        }
    }

    /**
     * {@link SimplePropertyHandler} to merge the states of the given objects.
     *
     * @author Oliver Gierke
     */
    private class MergingPropertyHandler implements SimplePropertyHandler {

        private final
        @Getter
        MappedProperties properties;
        private final PersistentPropertyAccessor targetAccessor;
        private final PersistentPropertyAccessor sourceAccessor;
        private final ObjectMapper mapper;
        private final boolean bidirectional;

        /**
         * Creates a new {@link MergingPropertyHandler} for the given source, target, {@link PersistentEntity} and
         * {@link ObjectMapper}.
         *
         * @param source        must not be {@literal null}.
         * @param target        must not be {@literal null}.
         * @param entity        must not be {@literal null}.
         * @param mapper        must not be {@literal null}.
         * @param bidirectional
         */
        public MergingPropertyHandler(Object source, Object target, PersistentEntity<?, ?> entity, ObjectMapper mapper, boolean bidirectional) {

            Assert.notNull(source, "Source instance must not be null!");
            Assert.notNull(target, "Target instance must not be null!");
            Assert.notNull(entity, "PersistentEntity must not be null!");
            Assert.notNull(mapper, "ObjectMapper must not be null!");

            this.properties = MappedProperties.fromJacksonProperties(entity, mapper);
            this.targetAccessor = new ConvertingPropertyAccessor(entity.getPropertyAccessor(target),
                    new DefaultConversionService());
            this.sourceAccessor = entity.getPropertyAccessor(source);
            this.mapper = mapper;
            this.bidirectional = bidirectional;
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.mapping.SimplePropertyHandler#doWithPersistentProperty(org.springframework.data.mapping.PersistentProperty)
         */
        @Override
        public void doWithPersistentProperty(PersistentProperty<?> property) {

            if (property.isIdProperty() || property.isVersionProperty() || !property.isWritable()) {
                return;
            }

            if (!properties.isMappedProperty(property)) {
                return;
            }

            Object sourceValue = sourceAccessor.getProperty(property);
            Object targetValue = targetAccessor.getProperty(property);
            Object result = null;


            if (property.isMap()) {
                result = mergeMaps(property, sourceValue, targetValue, mapper);
            } else if (property.isCollectionLike()) {
                result = mergeCollections(property, sourceValue, targetValue, mapper);
            } else if (property.isEntity()) {
                if (bidirectional) {
                    result = mergeForPutNoAssociations(sourceValue, targetValue, mapper);
                } else {
                    if (sourceValue != null && findIdField(sourceValue.getClass()) != null) {
                        Object sourceId = getId(sourceValue);
                        Object targetId = getId(targetValue);

                        if (sourceId != null && targetId != null && !sourceId.equals(targetId)) {
                            result = sourceValue; //do not merge if the entity is different, preserve the new associations
                        } else {
                            result = mergeForPut(sourceValue, targetValue, mapper, false);
                        }
                    } else {
                        result = mergeForPut(sourceValue, targetValue, mapper, false);
                    }
                }
            } else {
                result = sourceValue;
            }

            targetAccessor.setProperty(property, result);
        }

        private Object getId(Object object) {
            if(object == null) return  null;

            Field idField = findIdField(object.getClass());
            try {
                return FieldUtils.readField(idField, object, true);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Field to read ID field");
            }
        }

        private Field findIdField(Class clazz) {
            Field[] fields = FieldUtils.getFieldsWithAnnotation(clazz, javax.persistence.Id.class);
            if (fields == null) {
                fields = FieldUtils.getFieldsWithAnnotation(clazz, org.springframework.data.annotation.Id.class);
            }

            return fields == null || fields.length == 0 ? null : fields[0];
        }
    }
}
