/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.compress;

import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.spi.CompressorProvider;

import java.util.List;
import java.util.Map;

/** Registers the JDK compressor used by native transport and compressed content. @opensearch.internal */
public final class ServerCompressorProvider implements CompressorProvider {
    @Override
    public List<Map.Entry<String, Compressor>> getCompressors() {
        return List.of(Map.entry(DeflateCompressor.NAME, new DeflateCompressor()));
    }
}
