/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.helloworld.action;

import org.opensearch.Build;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

public class HelloWorldResponse extends ActionResponse implements ToXContentObject {
    private final DiscoveryNode localNode;
    private final Build build;

    public HelloWorldResponse(DiscoveryNode localNode, Build build) {
        this.localNode = localNode;
        this.build = build;
    }

    public HelloWorldResponse(StreamInput in) throws IOException {
        super(in);
        localNode = new DiscoveryNode(in);
        build = in.readBuild();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        localNode.writeTo(out);
        out.writeBuild(build);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", localNode.getName());
        builder.field("id", localNode.getId());
        builder.field("ephemeral_id", localNode.getEphemeralId());
        builder.field("address", localNode.getAddress().toString());
        builder.startObject("version")
            .field("distribution", build.getDistribution())
            .field("number", build.getQualifiedVersion())
            .field("build_type", build.type().displayName())
            .field("build_hash", build.hash())
            .field("build_date", build.date())
            .field("build_snapshot", build.isSnapshot())
            .field("minimum_wire_compatibility_version", localNode.getVersion().minimumCompatibilityVersion().toString())
            .field("minimum_index_compatibility_version", localNode.getVersion().minimumIndexCompatibilityVersion().toString())
            .endObject();
        builder.field("tagline", "OpenSearch (http://opensearch.org), but modular");
        builder.endObject();
        return builder;
    }
}
