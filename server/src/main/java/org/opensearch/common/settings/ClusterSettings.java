/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.settings;

import org.apache.logging.log4j.LogManager;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.breaker.BreakerSettings;
import org.opensearch.common.breaker.HierarchyCircuitBreakerService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.env.Environment;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportSettings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Encapsulates all valid cluster level settings.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ClusterSettings extends AbstractScopedSettings {

    public static Set<Setting<?>> BUILT_IN_CLUSTER_SETTINGS = Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                BreakerSettings.CIRCUIT_BREAKER_LIMIT_SETTING,
                BreakerSettings.CIRCUIT_BREAKER_OVERHEAD_SETTING,
                ClusterName.CLUSTER_NAME_SETTING,
                Environment.PATH_HOME_SETTING,
                Environment.PATH_DATA_SETTING,
                Environment.PATH_LOGS_SETTING,
                HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING,
                HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
                HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
                HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
                HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING,
                HttpTransportSettings.SETTING_HTTP_PORT,
                HttpTransportSettings.SETTING_HTTP_TRACE_LOG_INCLUDE,
                HttpTransportSettings.SETTING_HTTP_TRACE_LOG_EXCLUDE,
                NetworkModule.HTTP_DEFAULT_TYPE_SETTING,
                NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING,
                Node.NODE_NAME_SETTING,
                TaskManager.TASK_RESOURCE_CONSUMERS_ENABLED,
                TaskResourceTrackingService.TASK_RESOURCE_TRACKING_ENABLED,
                ThreadPool.CLUSTER_THREAD_POOL_SIZE_SETTING,
                TransportSettings.PORT,
                TransportSettings.TRACE_LOG_INCLUDE_SETTING,
                TransportSettings.TRACE_LOG_EXCLUDE_SETTING,
                TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING
            )
        )
    );

    public ClusterSettings(final Settings nodeSettings, final Set<Setting<?>> settingsSet) {
        this(nodeSettings, settingsSet, Collections.emptySet());
    }

    public ClusterSettings(final Settings nodeSettings, final Set<Setting<?>> settingsSet, final Set<SettingUpgrader<?>> settingUpgraders) {
        super(nodeSettings, settingsSet, settingUpgraders, Property.NodeScope);
        addSettingsUpdater(new LoggingSettingUpdater(nodeSettings));
    }

    private static final class LoggingSettingUpdater implements SettingUpdater<Settings> {
        final Predicate<String> loggerPredicate = Loggers.LOG_LEVEL_SETTING::match;
        private final Settings settings;

        LoggingSettingUpdater(Settings settings) {
            this.settings = settings;
        }

        @Override
        public boolean hasChanged(Settings current, Settings previous) {
            return current.filter(loggerPredicate).equals(previous.filter(loggerPredicate)) == false;
        }

        @Override
        public Settings getValue(Settings current, Settings previous) {
            Settings.Builder builder = Settings.builder();
            builder.put(current.filter(loggerPredicate));
            for (String key : previous.keySet()) {
                if (loggerPredicate.test(key) && builder.keys().contains(key) == false) {
                    if (Loggers.LOG_LEVEL_SETTING.getConcreteSetting(key).exists(settings) == false) {
                        builder.putNull(key);
                    } else {
                        builder.put(key, Loggers.LOG_LEVEL_SETTING.getConcreteSetting(key).get(settings).toString());
                    }
                }
            }
            return builder.build();
        }

        @Override
        public void apply(Settings value, Settings current, Settings previous) {
            for (String key : value.keySet()) {
                assert loggerPredicate.test(key);
                String component = key.substring("logger.".length());
                if ("level".equals(component)) {
                    continue;
                }
                if ("_root".equals(component)) {
                    final String rootLevel = value.get(key);
                    if (rootLevel == null) {
                        Loggers.setLevel(LogManager.getRootLogger(), Loggers.LOG_DEFAULT_LEVEL_SETTING.get(settings));
                    } else {
                        Loggers.setLevel(LogManager.getRootLogger(), rootLevel);
                    }
                } else {
                    Loggers.setLevel(LogManager.getLogger(component), value.get(key));
                }
            }
        }
    }
}
