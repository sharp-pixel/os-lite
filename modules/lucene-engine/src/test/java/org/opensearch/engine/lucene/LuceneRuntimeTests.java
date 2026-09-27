/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.engine.lucene;

import junit.framework.TestCase;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LuceneRuntimeTests extends TestCase {
    public void testProviderRejectsMismatchedOrMissingVersion() {
        LuceneRuntime.checkVersion("10.4.0", "10.4.0");
        assertTrue(
            assertThrows(IllegalStateException.class, () -> LuceneRuntime.checkVersion("10.4.0", "10.3.2")).getMessage().contains("10.4.0")
        );
        assertThrows(IllegalStateException.class, () -> LuceneRuntime.checkVersion(null, "10.4.0"));
    }
}
