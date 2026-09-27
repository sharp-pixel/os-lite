/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.api;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.engine.api.SnapshotManifest;

import java.io.IOException;
import java.util.ArrayList;

/** Bounded versioned repository records, independent of backend file formats. @opensearch.internal */
public final class SnapshotWire {
    private SnapshotWire() {}

    public static void publication(StreamOutput out, SnapshotRepository.Publication publication) throws IOException {
        out.writeInt(0x4f535250);
        out.writeByte((byte) 1);
        IndexWire.metadata(out, publication.metadata());
        out.writeVLong(publication.epoch());
        IndexWire.uuid(out, publication.owner());
        IndexWire.uuid(out, publication.snapshotId());
        SnapshotManifest manifest = publication.manifest();
        IndexWire.string(out, manifest.format());
        IndexWire.checkpoint(out, manifest.checkpoint());
        out.writeVInt(manifest.files().size());
        for (SnapshotManifest.File file : manifest.files()) {
            IndexWire.string(out, file.name());
            out.writeVLong(file.length());
            IndexWire.string(out, file.sha256());
        }
    }

    public static SnapshotRepository.Publication publication(StreamInput in) throws IOException {
        if (in.readInt() != 0x4f535250 || in.readByte() != 1) throw new IOException("unsupported publication format");
        try {
            IndexMetadata metadata = IndexWire.metadata(in);
            long epoch = in.readVLong();
            var owner = IndexWire.uuid(in);
            var snapshot = IndexWire.uuid(in);
            String format = IndexWire.string(in, 64);
            var checkpoint = IndexWire.checkpoint(in);
            int count = IndexWire.count(in, SnapshotManifest.MAX_FILES);
            var files = new ArrayList<SnapshotManifest.File>(count);
            for (int i = 0; i < count; i++) {
                files.add(new SnapshotManifest.File(IndexWire.string(in, 128), in.readVLong(), IndexWire.string(in, 64)));
            }
            return new SnapshotRepository.Publication(
                metadata,
                epoch,
                owner,
                snapshot,
                new SnapshotManifest(metadata.engine(), format, metadata.schema(), checkpoint, files)
            );
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid publication", e);
        }
    }
}
