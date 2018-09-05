/*
 * Copyright (c) 2018 Uber Technologies, Inc.
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions
 * of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

package com.uber.marmaray.common.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.uber.marmaray.common.exceptions.JobRuntimeException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * <p>
 * What qualify as scopes? <br>
 * Top-level nodes which appear under a tag dedicated to represent
 * scope override qualify as scopes.
 * </p>
 * <p>
 * How is scope override defined? <br>
 * Scope override definition is defined under a config driven tag dedicated
 * for scope override. In the example below in (fig 1), the tag happens to be
 * scope_override and scopes are x, y & z. Scopes y and z are the overrider
 * scopes and x is the default scope.
 * </p>
 * <p>
 * How is scope overriding done? <br>
 * There are not more than 2 scopes participating in a scope override at any time.
 * That is, either the user chooses to use the scope y, in which case y overrides x
 * OR the user chooses to use scope z, in which case z overrides x
 * OR user chooses to not use any scope, in which case no overriding is done at all.
 * Also note that y and z are totally unrelated and never participate together in
 * scope overriding. Before explaining the logic of scope overriding, please note
 * the convention in the example below
 * <ul>
 * <li>
 * Any node that starts with p represents a primitive node. In other
 * words, it represents a config-key and a node that starts with v
 * represents the config-value.
 * </li>
 * <li>
 * Nodes x, y, z represent scopes as explained in the previous question.
 * </li>
 * <li>
 * Rest of nodes [a-g] represent internal nodes.
 * </li>
 * </ul>
 *
 * (fig 1)
 * <pre>
 *     ├── scope_override
 *     │   ├── {y = x}
 *     │   └── {z = x}
 *     │     
 *     ├── x
 *     │   └── a
 *     │       ├── b
 *     │       │   ├── c
 *     │       │   │   ├── d
 *     │       │   │   │   ├── {p3 = v3_x}
 *     │       │   │   │   └── {p4 = v4_x}
 *     │       │   │   └── {p2 = v2_x}
 *     │       │   └── {p1 = v1_x}
 *     │       └── e
 *     │           └── f
 *     │               └── {p5 = v5_x}
 *     ├── y
 *     │   └── a
 *     │       └── b
 *     │           ├── c
 *     │           │   ├── d
 *     │           │   │   ├── {p3 = v3_y}
 *     │           │   │   └── {p4 = v4_y}
 *     │           │   └── {p2 = v2_y}
 *     │           └── {p1 = v1_y}
 *     └── z
 *         └── a
 *             └── b
 *                 ├── c
 *                 │   ├── d
 *                 │   │   ├── {p3 = v3_z}
 *                 │   │   └── {p7 = v7_z}
 *                 │   └── {p2 = v2_z}
 *                 ├── g
 *                 │   └── {p6 = v6_z}
 *                 └── {p1 = v1_z}
 *
 * </pre>
 *
 * <p>
 * Overriding is done by replacing the value of a config-key that appears
 * in both the scopes, along the exact same hierarchy.
 *
 * When scope y overrides x, the final config-key values resolved are below.
 * It inherits p5 as is from x. Overrides p1, p2, p3, p4. <br>
 * (fig 2)
 * <pre>
 *    a
 *    ├── b
 *    │   ├── c
 *    │   │   ├── d
 *    │   │   │   ├── {p3 = v3_y}
 *    │   │   │   └── {p4 = v4_y}
 *    │   │   └── {p2 = v2_y}
 *    │   └── {p1 = v1_y}
 *    └── e
 *        └── f
 *            └── {p5 = v5_x}
 * </pre>
 * When scope z overrides x, the final config-key values resolved are below.
 * It inherits p5 as is from x. Overrides p1, p2, p3, p4. Retains its p6. <br>
 * (fig 3)
 * <pre>
 *    a
 *    ├── b
 *    │   ├── c
 *    │   │   ├── d
 *    │   │   │   ├── {p3 = v3_z}
 *    │   │   │   └── {p4 = v4_z}
 *    │   │   │   └── {p7 = v7_z}
 *    │   │   └── {p2 = v2_z}
 *    │   ├── g
 *    │   │   └── {p6 = v6_z}
 *    │   └── {p1 = v1_y}
 *    └── e
 *        └── f
 *            └── {p5 = v5_x}
 * </pre>
 * </p>
 */
@Slf4j
@RequiredArgsConstructor
public class ConfigScopeResolver {

    private final String scopeOverrideMappingKey;
    private Map<String, String> scopeOverrideMap;
    private final ObjectMapper mapper = new ObjectMapper();

    public JsonNode projectOverrideScopeOverDefault(
        @NonNull final Optional<String> scope, @NonNull final JsonNode rootJsonNode) {
        if (!scope.isPresent() || !rootJsonNode.isContainerNode()) {
            log.info("No scope overriding in effect. "
                    + "Either scope: {} is absent, or {} is not a container node",
                scope, rootJsonNode);
            return rootJsonNode;
        }
        Preconditions.checkState(rootJsonNode.has(scopeOverrideMappingKey),
            String.format(
                "scopeOverrideMappingKey: %s is not present in config but scoping is expected with scope: %s",
                scopeOverrideMappingKey, scope));

        final ObjectNode root = copyToObjectNode(rootJsonNode);
        final JsonNode scopeOverrideDefinitionNodeVal = root.get(scopeOverrideMappingKey);
        Preconditions.checkArgument(
            scopeOverrideDefinitionNodeVal != null
                && scopeOverrideDefinitionNodeVal.isContainerNode(),
            String.format("Value for scopePrecedence %s should be a map, got null or primitive: ",
                scopeOverrideDefinitionNodeVal));
        this.scopeOverrideMap = mapper.convertValue(scopeOverrideDefinitionNodeVal, Map.class);
        log.info("scopeOverrideMap is {} scope is {}", this.scopeOverrideMap, scope.get());

        Preconditions.checkArgument(scopeOverrideMap.containsKey(scope.get()),
            "Un-recognized scope passed for config resolving");

        root.remove(scopeOverrideMappingKey);

        final String overrideScope = scope.get();
        final String defaultScope = scopeOverrideMap.get(overrideScope);
        if (root.has(overrideScope) && root.has(defaultScope)) {
            final JsonNode resolvedNode = handleScopeOverride(root, overrideScope,
                defaultScope);
            root.remove(overrideScope);
            root.remove(defaultScope);
            final Iterator<String> fieldNamesOfResolvedNode = resolvedNode.fieldNames();
            while (fieldNamesOfResolvedNode.hasNext()) {
                final String fieldNameOfResolvedNode = fieldNamesOfResolvedNode.next();
                root.put(fieldNameOfResolvedNode, resolvedNode.get(fieldNameOfResolvedNode));
            }
        } else {
            log.info("No overriding done for scope combinations  as one of them is missing."
                    + " IsOverrideScopePresent: {}, IsDefaultScopePresent: {} ",
                root.has(overrideScope), root.has(defaultScope));
        }
        for (final Entry<String, String> entry : scopeOverrideMap.entrySet()) {
            // remove all scope definitions, now that resolving is done
            root.remove(entry.getKey());
            root.remove(entry.getValue());
        }
        return root;
    }

    private JsonNode handleScopeOverride(
        @NonNull final ObjectNode root,
        @NotEmpty final String overrideScope,
        @NotEmpty final String defaultScope) {
        final JsonNode overridingNode = root.get(overrideScope);
        final JsonNode defaultNode = root.get(defaultScope);
        final ObjectNode defaultNodeCopy;
        defaultNodeCopy = copyToObjectNode(defaultNode);
        // defaultNodeCopy will be updated by projecting overridingNode over it
        projectOverrideNodeOverDefaultForField(null, defaultNodeCopy, overridingNode);
        return defaultNodeCopy;

    }

    private ObjectNode copyToObjectNode(@NonNull final JsonNode defaultNode) {
        final ObjectNode defaultNodeCopy;
        try {
            defaultNodeCopy = (ObjectNode) mapper
                .readTree(mapper.writeValueAsString(defaultNode));
        } catch (IOException e) {
            log.error("Got exception", e);
            throw new JobRuntimeException(e);
        }
        return defaultNodeCopy;
    }

    private void projectOverrideNodeOverDefaultForField(
        @NotEmpty final String fieldName,
        @NonNull final JsonNode parentDefaultNode,
        @NonNull final JsonNode parentOverridingNode) {

        final JsonNode defaultNode =
            (fieldName == null) ? parentDefaultNode : parentDefaultNode.get(fieldName);
        final JsonNode overridingNode =
            (fieldName == null) ? parentOverridingNode : parentOverridingNode.get(fieldName);

        if (fieldName != null) {
            // not first time call to recursion
            if (defaultNode == null || overridingNode == null) {
                final JsonNode nodeToPutAtFieldName = java.util.Optional.ofNullable(defaultNode)
                    .orElse(overridingNode);
                log.info("Copying fieldName: {} value: {}", fieldName, nodeToPutAtFieldName);
                ((ObjectNode) parentDefaultNode).put(fieldName, nodeToPutAtFieldName);
                return;
            }
            Preconditions
                .checkState(
                    (defaultNode.isContainerNode() && overridingNode.isContainerNode())
                        || (!defaultNode.isContainerNode() && !overridingNode.isContainerNode()),
                    "Mismatch in node type between default node: {} and overriding node: {}."
                        + " One of them is a primitive node", defaultNode, overridingNode);
            if (!overridingNode.isContainerNode()) {
                // primitive node or TextNode since that is the only primitive node that appears here
                // so blindly accept the value of the overriding node
                log.info("Using value: {} of override node for fieldName: {}", overridingNode,
                    fieldName);
                ((ObjectNode) parentDefaultNode).put(fieldName, overridingNode);
            } else {
                // both are container nodes
                projectOverAllFields(defaultNode, overridingNode);
            }
        } else {
            // first call to recursion, represents root default node and override node.
            projectOverAllFields(defaultNode, overridingNode);
        }
    }

    private void projectOverAllFields(
        @NonNull final JsonNode defaultNode, @NonNull final JsonNode overridingNode) {
        final Iterator<String> childFieldNames = overridingNode.fieldNames();
        while (childFieldNames.hasNext()) {
            final String childFieldName = childFieldNames.next();
            projectOverrideNodeOverDefaultForField(childFieldName, defaultNode, overridingNode);
        }
    }
}
