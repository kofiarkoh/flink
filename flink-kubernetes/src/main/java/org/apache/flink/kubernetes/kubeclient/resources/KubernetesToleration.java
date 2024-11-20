/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.kubeclient.resources;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Represent Toleration resource in kubernetes. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
public class KubernetesToleration extends KubernetesResource<Toleration> {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesToleration.class);

    private KubernetesToleration(Toleration toleration) {
        super(toleration);
    }

    public static KubernetesToleration fromMap(Map<String, String> stringMap) {
        final TolerationBuilder tolerationBuilder = new TolerationBuilder();
        stringMap.forEach(
                (k, v) -> {
                    switch (k.toLowerCase()) {
                        case "effect":
                            tolerationBuilder.withEffect(v);
                            break;
                        case "key":
                            tolerationBuilder.withKey(v);
                            break;
                        case "operator":
                            tolerationBuilder.withOperator(v);
                            break;
                        case "tolerationseconds":
                            tolerationBuilder.withTolerationSeconds(Long.valueOf(v));
                            break;
                        case "value":
                            tolerationBuilder.withValue(v);
                            break;
                        default:
                            LOG.warn("Unrecognized key({}) of toleration, will ignore.", k);
                            break;
                    }
                });
        return new KubernetesToleration(tolerationBuilder.build());
    }
}
