package com.rackspace.volga.etl.common.transform;

/**
 * User: alex.silva
 * Date: 4/3/14
 * Time: 10:28 AM
 * Copyright Rackspace Hosting, Inc.
 */
public interface Transformer<S, T> {
    /**
     * @param object           The source object
     * @param type             The type of the destination object
     * @param transformationId An OPTIONAL transformation id in case there are multiple transformations defined for
     *                         the objects passed in.
     * @return The converted object.
     */
    public T transform(S object, Class<? extends T> type, String transformationId);

    /**
     * @param object           The source object
     * @param type             The type of the destination object
     * @return The converted object.
     */
    public T transform(S object, Class<? extends T> type);

}
