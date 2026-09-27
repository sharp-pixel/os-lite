/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.api;

import java.util.Objects;

/** A full replacement or deletion; script updates are not supported. @opensearch.experimental */
public sealed interface Mutation permits Mutation.Put, Mutation.Delete {
    record Put(EngineDocument document) implements Mutation {
        public Put {
            Objects.requireNonNull(document);
        }
    }

    record Delete(String id) implements Mutation {
        public Delete {
            EngineDocument.validateId(id);
        }
    }
}
