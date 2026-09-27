/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.service;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.engine.api.EngineException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Maps service and engine failures at the OpenSearch action boundary. @opensearch.internal */
final class IndexFailures {
    private IndexFailures() {}

    static Exception action(Throwable failure) {
        while ((failure instanceof CompletionException || failure instanceof ExecutionException) && failure.getCause() != null)
            failure = failure.getCause();
        if (failure instanceof OpenSearchStatusException status) return status;
        if (failure instanceof EngineException engine) {
            RestStatus status = switch (engine.code()) {
                case INVALID_ARGUMENT, UNSUPPORTED -> RestStatus.BAD_REQUEST;
                case CONFLICT, INCOMPATIBLE -> RestStatus.CONFLICT;
                case RESOURCE_LIMIT -> RestStatus.TOO_MANY_REQUESTS;
                case CLOSED, UNAVAILABLE -> RestStatus.SERVICE_UNAVAILABLE;
                case CANCELLED, DEADLINE_EXCEEDED -> RestStatus.REQUEST_TIMEOUT;
                case IO_ERROR, WRITE_OUTCOME_UNKNOWN -> RestStatus.INTERNAL_SERVER_ERROR;
            };
            return new OpenSearchStatusException("[" + engine.code().name() + "] " + engine.getMessage(), status);
        }
        if (failure instanceof IllegalArgumentException argument) return argument;
        return new OpenSearchStatusException("index service operation failed", RestStatus.INTERNAL_SERVER_ERROR);
    }
}
