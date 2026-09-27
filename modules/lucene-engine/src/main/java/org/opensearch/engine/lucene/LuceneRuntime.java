/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.engine.lucene;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Provider-owned dependency compatibility and codec discovery. @opensearch.internal */
final class LuceneRuntime {
    private static boolean initialized;

    private LuceneRuntime() {}

    static synchronized void initialize() {
        if (initialized) return;
        Properties properties = new Properties();
        try (InputStream input = LuceneRuntime.class.getResourceAsStream("lucene-runtime.properties")) {
            if (input == null) throw new IllegalStateException("Lucene provider build version is missing");
            properties.load(input);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read Lucene provider build version", e);
        }
        checkVersion(properties.getProperty("version"), Version.LATEST.toString());
        ClassLoader loader = LuceneRuntime.class.getClassLoader();
        PostingsFormat.reloadPostingsFormats(loader);
        DocValuesFormat.reloadDocValuesFormats(loader);
        KnnVectorsFormat.reloadKnnVectorsFormat(loader);
        Codec.reloadCodecs(loader);
        initialized = true;
    }

    static void checkVersion(String expected, String actual) {
        if (expected == null || expected.equals(actual) == false) {
            throw new IllegalStateException("Lucene provider requires version [" + expected + "] but loaded [" + actual + "]");
        }
    }
}
