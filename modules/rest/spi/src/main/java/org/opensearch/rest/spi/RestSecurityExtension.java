/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.spi;

import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Extension point for authenticating REST requests before they reach an API handler.
 *
 * @opensearch.api
 */
public interface RestSecurityExtension {
    /** Returns HTTP headers that should be copied into the request {@link ThreadContext}. */
    default Collection<RestHeaderDefinition> getRestHeaders() {
        return Collections.emptyList();
    }

    /**
     * Returns a wrapper that authenticates or otherwise validates each REST request.
     * Only one installed extension may provide a wrapper.
     */
    default UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext, Set<RestHeaderDefinition> headersToCopy) {
        return null;
    }
}
