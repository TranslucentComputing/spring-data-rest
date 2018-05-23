/*
 * Copyright 2013-2016 the original author or authors.
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
package org.springframework.data.rest.webmvc;

import static org.springframework.http.HttpMethod.*;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.auditing.AuditableBeanWrapperFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.querydsl.binding.QuerydslPredicate;
import org.springframework.data.repository.support.Repositories;
import org.springframework.data.repository.support.RepositoryInvoker;
import org.springframework.data.rest.core.config.RepositoryRestConfiguration;
import org.springframework.data.rest.core.event.*;
import org.springframework.data.rest.core.mapping.ResourceMetadata;
import org.springframework.data.rest.core.mapping.ResourceType;
import org.springframework.data.rest.core.mapping.SearchResourceMappings;
import org.springframework.data.rest.core.mapping.SupportedHttpMethods;
import org.springframework.data.rest.core.util.Supplier;
import org.springframework.data.rest.webmvc.support.BackendId;
import org.springframework.data.rest.webmvc.support.DefaultedPageable;
import org.springframework.data.rest.webmvc.support.ETag;
import org.springframework.data.rest.webmvc.support.ETagDoesntMatchException;
import org.springframework.data.rest.webmvc.support.RepositoryEntityLinks;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.Links;
import org.springframework.hateoas.PagedResources;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.hateoas.Resources;
import org.springframework.hateoas.UriTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Greg Turnquist
 * @author Jeremy Rickard
 */
@RepositoryRestController
public class RepositoryEntityController extends AbstractRepositoryRestController implements ApplicationEventPublisherAware {

    private static final String BASE_MAPPING = "/{repository}";
    private static final List<String> ACCEPT_PATCH_HEADERS = Arrays.asList(//
            RestMediaTypes.MERGE_PATCH_JSON.toString(), //
            RestMediaTypes.JSON_PATCH_JSON.toString(), //
            MediaType.APPLICATION_JSON_VALUE);

    private static final String ACCEPT_HEADER = "Accept";
    private static final String LINK_HEADER = "Link";

    private final RepositoryEntityLinks entityLinks;
    private final RepositoryRestConfiguration config;
    private final HttpHeadersPreparer headersPreparer;
    private final ResourceStatus resourceStatus;

    private ApplicationEventPublisher publisher;

    /**
     * Creates a new {@link RepositoryEntityController} for the given {@link Repositories},
     * {@link RepositoryRestConfiguration}, {@link RepositoryEntityLinks}, {@link PagedResourcesAssembler},
     * {@link ConversionService} and {@link AuditableBeanWrapperFactory}.
     *
     * @param repositories    must not be {@literal null}.
     * @param config          must not be {@literal null}.
     * @param entityLinks     must not be {@literal null}.
     * @param assembler       must not be {@literal null}.
     * @param headersPreparer must not be {@literal null}.
     */
    @Autowired
    public RepositoryEntityController(Repositories repositories, RepositoryRestConfiguration config,
                                      RepositoryEntityLinks entityLinks, PagedResourcesAssembler<Object> assembler,
                                      HttpHeadersPreparer headersPreparer) {

        super(assembler);

        this.entityLinks = entityLinks;
        this.config = config;
        this.headersPreparer = headersPreparer;
        this.resourceStatus = ResourceStatus.of(headersPreparer);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.context.ApplicationEventPublisherAware#setApplicationEventPublisher(org.springframework.context.ApplicationEventPublisher)
     */
    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    /**
     * <code>OPTIONS /{repository}</code>.
     *
     * @param information
     * @return
     * @since 2.2
     */
    @RequestMapping(value = BASE_MAPPING, method = RequestMethod.OPTIONS)
    public ResponseEntity<?> optionsForCollectionResource(RootResourceInformation information) {

        HttpHeaders headers = new HttpHeaders();
        SupportedHttpMethods supportedMethods = information.getSupportedMethods();

        headers.setAllow(supportedMethods.getMethodsFor(ResourceType.COLLECTION));

        return new ResponseEntity<Object>(headers, HttpStatus.OK);
    }

    /**
     * <code>HEAD /{repository}</code>
     *
     * @param resourceInformation
     * @return
     * @throws HttpRequestMethodNotSupportedException
     * @since 2.2
     */
    @RequestMapping(value = BASE_MAPPING, method = RequestMethod.HEAD)
    public ResponseEntity<?> headCollectionResource(RootResourceInformation resourceInformation,
                                                    DefaultedPageable pageable) throws HttpRequestMethodNotSupportedException {

        resourceInformation.verifySupportedMethod(HttpMethod.HEAD, ResourceType.COLLECTION);

        RepositoryInvoker invoker = resourceInformation.getInvoker();

        if (null == invoker) {
            throw new ResourceNotFoundException();
        }

        List<Link> links = getCollectionResourceLinks(resourceInformation, pageable);
        links.add(0, getDefaultSelfLink());

        HttpHeaders headers = new HttpHeaders();
        headers.add(LINK_HEADER, new Links(links).toString());

        return new ResponseEntity<Object>(headers, HttpStatus.NO_CONTENT);
    }

    /**
     * <code>GET /{repository}</code> - Returns the collection resource (paged or unpaged).
     *
     * @param resourceInformation
     * @param pageable
     * @param sort
     * @param assembler
     * @return
     * @throws ResourceNotFoundException
     * @throws HttpRequestMethodNotSupportedException
     */
    @ResponseBody
    @RequestMapping(value = BASE_MAPPING, method = RequestMethod.GET)
    public Resources<?> getCollectionResource(@QuerydslPredicate RootResourceInformation resourceInformation,
                                              DefaultedPageable pageable, Sort sort, PersistentEntityResourceAssembler assembler)
            throws ResourceNotFoundException, HttpRequestMethodNotSupportedException {

        resourceInformation.verifySupportedMethod(HttpMethod.GET, ResourceType.COLLECTION);

        RepositoryInvoker invoker = resourceInformation.getInvoker();

        if (null == invoker) {
            throw new ResourceNotFoundException();
        }

        Iterable<?> results = pageable.getPageable() != null ? invoker.invokeFindAll(pageable.getPageable())
                : invoker.invokeFindAll(sort);

        ResourceMetadata metadata = resourceInformation.getResourceMetadata();
        Link baseLink = entityLinks.linkToPagedResource(resourceInformation.getDomainType(),
                pageable.isDefault() ? null : pageable.getPageable());

        Resources<?> result = toResources(results, assembler, metadata.getDomainType(), baseLink);
        result.add(getCollectionResourceLinks(resourceInformation, pageable));
        return result;
    }

    private List<Link> getCollectionResourceLinks(RootResourceInformation resourceInformation,
                                                  DefaultedPageable pageable) {

        ResourceMetadata metadata = resourceInformation.getResourceMetadata();
        SearchResourceMappings searchMappings = metadata.getSearchResourceMappings();

        List<Link> links = new ArrayList<Link>();
        links.add(new Link(ProfileController.getPath(this.config, metadata), ProfileResourceProcessor.PROFILE_REL));

        if (searchMappings.isExported()) {
            links.add(entityLinks.linkFor(metadata.getDomainType()).slash(searchMappings.getPath())
                    .withRel(searchMappings.getRel()));
        }

        return links;
    }

    @ResponseBody
    @SuppressWarnings({"unchecked"})
    @RequestMapping(value = BASE_MAPPING, method = RequestMethod.GET,
            produces = {"application/x-spring-data-compact+json", "text/uri-list"})
    public Resources<?> getCollectionResourceCompact(@QuerydslPredicate RootResourceInformation resourceinformation,
                                                     DefaultedPageable pageable, Sort sort, PersistentEntityResourceAssembler assembler)
            throws ResourceNotFoundException, HttpRequestMethodNotSupportedException {

        Resources<?> resources = getCollectionResource(resourceinformation, pageable, sort, assembler);
        List<Link> links = new ArrayList<Link>(resources.getLinks());

        for (Resource<?> resource : ((Resources<Resource<?>>) resources).getContent()) {
            PersistentEntityResource persistentEntityResource = (PersistentEntityResource) resource;
            links.add(resourceLink(resourceinformation, persistentEntityResource));
        }
        if (resources instanceof PagedResources) {
            return new PagedResources<Object>(Collections.emptyList(), ((PagedResources<?>) resources).getMetadata(), links);
        } else {
            return new Resources<Object>(Collections.emptyList(), links);
        }
    }

    /**
     * <code>POST /{repository}</code> - Creates a new entity instances from the collection resource.
     *
     * @param resourceInformation
     * @param payload
     * @param assembler
     * @param acceptHeader
     * @return
     * @throws HttpRequestMethodNotSupportedException
     */
    @ResponseBody
    @RequestMapping(value = BASE_MAPPING, method = RequestMethod.POST)
    public ResponseEntity<ResourceSupport> postCollectionResource(RootResourceInformation resourceInformation,
                                                                  PersistentEntityResource payload, PersistentEntityResourceAssembler assembler,
                                                                  @RequestHeader(value = ACCEPT_HEADER, required = false) String acceptHeader)
            throws HttpRequestMethodNotSupportedException {

        resourceInformation.verifySupportedMethod(HttpMethod.POST, ResourceType.COLLECTION);

        return createAndReturn(payload.getContent(), resourceInformation.getInvoker(), assembler,
                config.returnBodyOnCreate(acceptHeader));
    }

    /**
     * <code>OPTIONS /{repository}/{id}<code>
     *
     * @param information
     * @return
     * @since 2.2
     */
    @RequestMapping(value = BASE_MAPPING + "/{id}", method = RequestMethod.OPTIONS)
    public ResponseEntity<?> optionsForItemResource(RootResourceInformation information) {

        HttpHeaders headers = new HttpHeaders();
        SupportedHttpMethods supportedMethods = information.getSupportedMethods();

        headers.setAllow(supportedMethods.getMethodsFor(ResourceType.ITEM));
        headers.put("Accept-Patch", ACCEPT_PATCH_HEADERS);

        return new ResponseEntity<Object>(headers, HttpStatus.OK);
    }

    /**
     * <code>HEAD /{repsoitory}/{id}</code>
     *
     * @param resourceInformation
     * @param id
     * @return
     * @throws HttpRequestMethodNotSupportedException
     * @since 2.2
     */
    @RequestMapping(value = BASE_MAPPING + "/{id}", method = RequestMethod.HEAD)
    public ResponseEntity<?> headForItemResource(RootResourceInformation resourceInformation, @BackendId Serializable id,
                                                 PersistentEntityResourceAssembler assembler) throws HttpRequestMethodNotSupportedException {

        Object domainObject = getItemResource(resourceInformation, id);

        if (domainObject == null) {
            throw new ResourceNotFoundException();
        }

        Links links = new Links(assembler.toResource(domainObject).getLinks());

        HttpHeaders headers = headersPreparer.prepareHeaders(resourceInformation.getPersistentEntity(), domainObject);
        headers.add(LINK_HEADER, links.toString());

        return new ResponseEntity<Object>(headers, HttpStatus.NO_CONTENT);
    }

    /**
     * <code>GET /{repository}/{id}</code> - Returns a single entity.
     *
     * @param resourceInformation
     * @param id
     * @return
     * @throws HttpRequestMethodNotSupportedException
     */
    @RequestMapping(value = BASE_MAPPING + "/{id}", method = RequestMethod.GET)
    public ResponseEntity<Resource<?>> getItemResource(RootResourceInformation resourceInformation,
                                                       @BackendId Serializable id, final PersistentEntityResourceAssembler assembler, @RequestHeader HttpHeaders headers)
            throws HttpRequestMethodNotSupportedException {

        final Object domainObj = getItemResource(resourceInformation, id);

        if (domainObj == null) {
            return new ResponseEntity<Resource<?>>(HttpStatus.NOT_FOUND);
        }

        PersistentEntity<?, ?> entity = resourceInformation.getPersistentEntity();

        return resourceStatus.getStatusAndHeaders(headers, domainObj, entity).toResponseEntity(//
                new Supplier<PersistentEntityResource>() {
                    @Override
                    public PersistentEntityResource get() {
                        return assembler.toFullResource(domainObj);
                    }
                });
    }

    /**
     * <code>PUT /{repository}/{id}</code> - Updates an existing entity or creates one at exactly that place.
     *
     * @param resourceInformation
     * @param payload
     * @param id
     * @param assembler
     * @param eTag
     * @param acceptHeader
     * @return
     * @throws HttpRequestMethodNotSupportedException
     */
    @RequestMapping(value = BASE_MAPPING + "/{id}", method = RequestMethod.PUT)
    public ResponseEntity<? extends ResourceSupport> putItemResource(RootResourceInformation resourceInformation,
                                                                     PersistentEntityResource payload, @BackendId Serializable id, PersistentEntityResourceAssembler assembler,
                                                                     ETag eTag, @RequestHeader(value = ACCEPT_HEADER, required = false) String acceptHeader)
            throws HttpRequestMethodNotSupportedException, ClassNotFoundException {

        resourceInformation.verifySupportedMethod(HttpMethod.PUT, ResourceType.ITEM);

        RepositoryInvoker invoker = resourceInformation.getInvoker();
        Object objectToSave = payload.getContent();
        eTag.verify(resourceInformation.getPersistentEntity(), objectToSave);

        //increment version for ES domain objects
        //Entities that extend AbstractAuditingExternalEntity use external versioning
        //Entities that extend AbstractAuditingInternalEntity use internal versioning
        if (Class.forName("com.translucentcomputing.tekstack.core.domain.search.audit.AbstractAuditingInternalEntity").isAssignableFrom(resourceInformation.getDomainType())) {
            Long version = findVersion(objectToSave);
            updateVersion(objectToSave, version + 1);
        }

        return payload.isNew() ? createAndReturn(objectToSave, invoker, assembler, config.returnBodyOnCreate(acceptHeader))
                : saveAndReturn(objectToSave, invoker, PUT, assembler, config.returnBodyOnUpdate(acceptHeader));
    }

    private Long findVersion(Object object) {
        Field versionField = findVersionField(object.getClass());
        try {
            return (Long) FieldUtils.readField(versionField, object, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Version field not accessible");
        }
    }

    private void updateVersion(Object object, Long newVersion) {
        Field versionField = findVersionField(object.getClass());
        writeField(object, versionField, newVersion);
    }

    private void writeField(Object object, Field field, Long newVersion) {
        try {
            FieldUtils.writeField(field, object, newVersion, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Field to write mappedBy field");
        }
    }

    private Field findVersionField(Class clazz) {
        Field[] fields = FieldUtils.getFieldsWithAnnotation(clazz, org.springframework.data.annotation.Version.class);
        if (fields == null || fields.length == 0) {
            fields = FieldUtils.getFieldsWithAnnotation(clazz, javax.persistence.Version.class);
        }
        return fields == null || fields.length == 0 ? null : fields[0];
    }

    /**
     * <code>PATCH /{repository}/{id}</code> - Updates an existing entity or creates one at exactly that place.
     *
     * @param resourceInformation
     * @param payload
     * @param id
     * @param assembler
     * @param eTag,
     * @param acceptHeader
     * @return
     * @throws HttpRequestMethodNotSupportedException
     * @throws ResourceNotFoundException
     * @throws ETagDoesntMatchException
     */
    @RequestMapping(value = BASE_MAPPING + "/{id}", method = RequestMethod.PATCH)
    public ResponseEntity<ResourceSupport> patchItemResource(RootResourceInformation resourceInformation,
                                                             PersistentEntityResource payload, @BackendId Serializable id, PersistentEntityResourceAssembler assembler,
                                                             ETag eTag, @RequestHeader(value = ACCEPT_HEADER, required = false) String acceptHeader)
            throws HttpRequestMethodNotSupportedException, ResourceNotFoundException {

        resourceInformation.verifySupportedMethod(HttpMethod.PATCH, ResourceType.ITEM);

        Object domainObject = payload.getContent();

        eTag.verify(resourceInformation.getPersistentEntity(), domainObject);

        return saveAndReturn(domainObject, resourceInformation.getInvoker(), PATCH, assembler,
                config.returnBodyOnUpdate(acceptHeader));
    }

    /**
     * <code>DELETE /{repository}/{id}</code> - Deletes the entity backing the item resource.
     *
     * @param resourceInformation
     * @param id
     * @param eTag
     * @return
     * @throws ResourceNotFoundException
     * @throws HttpRequestMethodNotSupportedException
     * @throws ETagDoesntMatchException
     */
    @RequestMapping(value = BASE_MAPPING + "/{id}", method = RequestMethod.DELETE)
    public ResponseEntity<?> deleteItemResource(RootResourceInformation resourceInformation, @BackendId Serializable id,
                                                ETag eTag) throws ResourceNotFoundException, HttpRequestMethodNotSupportedException {

        resourceInformation.verifySupportedMethod(HttpMethod.DELETE, ResourceType.ITEM);

        RepositoryInvoker invoker = resourceInformation.getInvoker();
        Object domainObj = invoker.invokeFindOne(id);

        if (domainObj == null) {
            throw new ResourceNotFoundException();
        }

        PersistentEntity<?, ?> entity = resourceInformation.getPersistentEntity();

        eTag.verify(entity, domainObj);

        deleteItemWithEvents(invoker, entity, domainObj);

        return new ResponseEntity<Object>(HttpStatus.NO_CONTENT);
    }


    /**
     * Merges the given incoming object into the given domain object.
     *
     * @param domainObject
     * @param invoker
     * @param httpMethod
     * @return
     */
    private ResponseEntity<ResourceSupport> saveAndReturn(Object domainObject, RepositoryInvoker invoker,
                                                          HttpMethod httpMethod, PersistentEntityResourceAssembler assembler, boolean returnBody) {

        Object obj = saveEntityWithEvents(domainObject, invoker);

        PersistentEntityResource resource = assembler.toFullResource(obj);
        HttpHeaders headers = headersPreparer.prepareHeaders(resource);

        if (PUT.equals(httpMethod)) {
            addLocationHeader(headers, assembler, obj);
        }

        if (returnBody) {
            return ControllerUtils.toResponseEntity(HttpStatus.OK, headers, resource);
        } else {
            return ControllerUtils.toEmptyResponse(HttpStatus.NO_CONTENT, headers);
        }
    }

    /**
     * Triggers the creation of the domain object and renders it into the response if needed.
     *
     * @param domainObject
     * @param invoker
     * @return
     */
    public ResponseEntity<ResourceSupport> createAndReturn(Object domainObject, RepositoryInvoker invoker,
                                                           PersistentEntityResourceAssembler assembler, boolean returnBody) {

        Object savedObject = createEntityWithEvents(domainObject, invoker);

        PersistentEntityResource resource = returnBody ? assembler.toFullResource(savedObject) : null;

        HttpHeaders headers = headersPreparer.prepareHeaders(resource);
        addLocationHeader(headers, assembler, savedObject);

        return ControllerUtils.toResponseEntity(HttpStatus.CREATED, headers, resource);
    }

    /**
     * Save entity and publish events
     *
     * @param domainObject
     * @param invoker
     * @return
     */
    public Object saveEntityWithEvents(Object domainObject, RepositoryInvoker invoker) {
        publisher.publishEvent(new BeforeSaveEvent(domainObject));
        Object savedObject = saveEntity(domainObject, invoker);
        publisher.publishEvent(new OptimisticLockEvent(savedObject));

        return savedObject;
    }


    /**
     * Create entity and publish events
     *
     * @param domainObject
     * @param invoker
     * @return
     */
    public Object createEntityWithEvents(Object domainObject, RepositoryInvoker invoker) {
        publisher.publishEvent(new BeforeCreateEvent(domainObject));
        Object savedObject = saveEntity(domainObject, invoker);
        publisher.publishEvent(new AfterCreateEvent(savedObject));

        return savedObject;
    }

    /**
     * Delete entity by ID and publish before and after events
     *
     * @param invoker
     * @param entity
     * @param domainObj
     */
    public void deleteItemWithEvents(RepositoryInvoker invoker, PersistentEntity<?, ?> entity, Object domainObj) {
        publisher.publishEvent(new BeforeDeleteEvent(domainObj));
        deleteItem(invoker, entity, domainObj);
        publisher.publishEvent(new AfterDeleteEvent(domainObj));
    }


    /**
     * Delete an item with in a transaction by ID
     *
     * @param invoker
     * @param entity
     * @param domainObj
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void deleteItem(RepositoryInvoker invoker, PersistentEntity<?, ?> entity, Object domainObj) {
        invoker.invokeDelete((Serializable) entity.getIdentifierAccessor(domainObj).getIdentifier());
    }


    /**
     * Save entity with in a transaction
     *
     * @param domainObject
     * @param invoker
     * @return
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Object saveEntity(Object domainObject, RepositoryInvoker invoker) {
        return invoker.invokeSave(domainObject);
    }

    /**
     * Sets the location header pointing to the resource representing the given instance. Will make sure we properly
     * expand the URI template potentially created as self link.
     *
     * @param headers   must not be {@literal null}.
     * @param assembler must not be {@literal null}.
     * @param source    must not be {@literal null}.
     */
    private void addLocationHeader(HttpHeaders headers, PersistentEntityResourceAssembler assembler, Object source) {

        String selfLink = assembler.getSelfLinkFor(source).getHref();
        headers.setLocation(new UriTemplate(selfLink).expand());
    }

    /**
     * Returns the object backing the item resource for the given {@link RootResourceInformation} and id.
     *
     * @param resourceInformation
     * @param id
     * @return
     * @throws HttpRequestMethodNotSupportedException
     * @throws {@link                                 ResourceNotFoundException}
     */
    private Object getItemResource(RootResourceInformation resourceInformation, Serializable id)
            throws HttpRequestMethodNotSupportedException, ResourceNotFoundException {

        resourceInformation.verifySupportedMethod(HttpMethod.GET, ResourceType.ITEM);

        return resourceInformation.getInvoker().invokeFindOne(id);
    }
}
