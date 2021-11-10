/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.enhanced.dynamodb.extensions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClientExtension;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbExtensionContext;
import software.amazon.awssdk.enhanced.dynamodb.internal.extensions.AtomicCounterTag;
import software.amazon.awssdk.enhanced.dynamodb.internal.mapper.AtomicCounter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.utils.CollectionUtils;

/**
 * This extension enables atomic counter attributes to be written to the database when using other operations than
 * updateItem, such as putItem.
 * <p>
 *     This extension is not loaded by default when you instantiate a
 *     {@link software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient}. Thus you need to specify it in custom extension
 *     while creating the enhanced client.
 *     <p>
 *         Example to add this extension along with default extensions is
 *         <code>DynamoDbEnhancedClient.builder().extensions(Stream.concat(ExtensionResolver.defaultExtensions().stream(),
 *         Stream.of(AtomicCounterExtension.create())).collect(Collectors.toList())).build();</code>
 *     </p>
 * </p>
 * <p>
 *     To utilize atomic counters, first create a field in your model that will be used to store the counter.
 *     This class field should be have {@link Long} Class type, and you need to tag it as an atomic counter. If you are using the
 *     {@link software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema}
 *     then you should use the
 *     {@link software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAtomicCounter}
 *     annotation, otherwise if you are using the {@link software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema}
 *     then you should use the {@link StaticAttributeTags#atomicCounter()} static attribute tag.
 * <p>
 * <b>NOTE: </b>If you only use the updateItem operation to write/update your item to the database, the extension
 * IS NOT REQUIRED to be loaded in order for atomic counters to work. Tagging the attribute(s) in the schema is sufficient.
 * <p>
 * Every time a new update of the record is successfully written to the database, the counter will be automatically updated
 * using either user-supplied increment/decrement delta and start values or the defaults.
 */
@SdkPublicApi
public final class AtomicCounterExtension implements DynamoDbEnhancedClientExtension {

    private AtomicCounterExtension() {
    }

    /**
     * @return an instance of {@link AtomicCounterExtension}
     */
    public static AtomicCounterExtension create() {
        return new AtomicCounterExtension();
    }

    /**
     * @param context The {@link DynamoDbExtensionContext.BeforeWrite} context containing the state of the execution.
     * @return WriteModification contains the item with updated attributes.
     */
    @Override
    public WriteModification beforeWrite(DynamoDbExtensionContext.BeforeWrite context) {

        Map<String, AtomicCounter> counterMap =
            context.items().entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey,
                                 e -> AtomicCounterTag.resolveForAttribute(e.getKey(), context.tableMetadata())));

        if (CollectionUtils.isNullOrEmpty(counterMap)) {
            return WriteModification.builder().build();
        }

        Map<String, AttributeValue> itemToTransform = new HashMap<>(context.items());
        counterMap.forEach((attribute, counter) -> itemToTransform.put(attribute, counter.startValue().resolvedValue()));

        return WriteModification.builder()
                                .transformedItem(Collections.unmodifiableMap(itemToTransform))
                                .build();
    }

}
