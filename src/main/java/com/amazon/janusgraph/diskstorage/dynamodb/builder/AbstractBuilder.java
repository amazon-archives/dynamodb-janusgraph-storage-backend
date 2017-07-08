/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph.diskstorage.dynamodb.builder;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;

import com.amazon.janusgraph.diskstorage.dynamodb.Constants;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * AbstractBuilder is responsible for some of the StaticBuffer to String and
 * visa-versa required for working with the database.
 *
 * @author Matthew Sowders
 * @author Alexander Patrikalakis
 *
 */
public abstract class AbstractBuilder {

    protected AttributeValue encodeKeyAsAttributeValue(final StaticBuffer input) {
        return new AttributeValue().withS(encodeKeyBuffer(input));
    }

    public static String encodeKeyBuffer(final StaticBuffer input) {
        if (input == null || input.length() == 0) {
            return null;
        }
        final ByteBuffer buf = input.asByteBuffer();
        final byte[] bytes = Arrays.copyOf(buf.array(), buf.limit());
        return Hex.encodeHexString(bytes);
    }

    protected AttributeValue encodeValue(final StaticBuffer value) {
        // Dynamo does not allow empty binary values, so we use a placeholder
        // for empty values
        if (value.length() <= 0) {
            return new AttributeValue().withS(Constants.EMPTY_BUFFER_PLACEHOLDER);
        }
        return new AttributeValue().withB(value.asByteBuffer());
    }

    protected StaticBuffer decodeValue(final AttributeValue val) {
        if (null == val) {
            return null;
        }
        // Dynamo does not allow empty binary values, so we use a placeholder
        // for empty values
        if (Constants.EMPTY_BUFFER_PLACEHOLDER.equals(val.getS())) {
            return BufferUtil.emptyBuffer();
        }
        return StaticArrayBuffer.of(val.getB());
    }

    protected StaticBuffer decodeKey(final Map<String, AttributeValue> key, final String name) {
        if (null == key || !key.containsKey(name)) {
            return null;
        }
        final AttributeValue attributeValue = key.get(name);
        final String value = attributeValue.getS();
        return decodeKey(value);
    }

    public static StaticBuffer decodeKey(final String name) {
        try {
            return new StaticArrayBuffer(Hex.decodeHex(name.toCharArray()));
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }
}
