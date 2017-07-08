/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazon.janusgraph.diskstorage.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.amazon.janusgraph.testcategory.IsolateRemainingTestsCategory;
import com.amazonaws.client.builder.AwsClientBuilder;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
@Category({IsolateRemainingTestsCategory.class})
public class DynamoDbDelegateTest {

    public static final String HTTP_LOCALHOST_4567 = "http://localhost:4567";
    public static final String HTTPS_DYNAMODB_US_EAST_1_AMAZONAWS_COM = "https://dynamodb.us-east-1.amazonaws.com";
    public static final Optional<String> NULL_ENDPOINT = null;
    public static final Optional<String> EMPTY_ENDPOINT = Optional.empty();
    public static final Optional<String> VALID_EMPTY_STRING_ENDPOINT = Optional.ofNullable("");
    public static final Optional<String> VALID_NOT_A_URL_ENDPOINT = Optional.ofNullable("invalid");
    public static final Optional<String> VALID_DYNAMODB_LOCAL_ENDPOINT = Optional.ofNullable("http://localhost:4567");
    public static final Optional<String> VALID_DYNAMODB_ENDPOINT = Optional.ofNullable("https://dynamodb.ap-northeast-1.amazonaws.com");
    public static final String NULL_REGION = null;
    public static final String EMPTY_REGION = "";
    public static final String INVALID_REGION = "foobar";
    public static final String VALID_REGION = "ap-northeast-1";

    //NULL ENDPOINT
    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointNullAndRegionNull_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(NULL_ENDPOINT, NULL_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointNullAndRegionEmpty_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(NULL_ENDPOINT, EMPTY_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointNullAndRegionInvalid_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(NULL_ENDPOINT, INVALID_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointNullAndRegionValid_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(NULL_ENDPOINT, VALID_REGION);
    }

    //EMPTY ENDPOINT
    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointEmptyAndRegionNull_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(EMPTY_ENDPOINT, NULL_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointEmptyAndRegionEmpty_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(EMPTY_ENDPOINT, EMPTY_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointEmptyAndRegionInvalid_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(EMPTY_ENDPOINT, INVALID_REGION);
    }

    @Test
    public void getEndpointConfiguration_whenEndpointEmptyAndRegionValid_returnConfig() {
        final AwsClientBuilder.EndpointConfiguration config = DynamoDbDelegate.getEndpointConfiguration(EMPTY_ENDPOINT, VALID_REGION);
        assertEquals(VALID_REGION, config.getSigningRegion());
        assertTrue(config.getServiceEndpoint().contains(VALID_REGION));
    }

    //VALID_EMPTY_STRING_ENDPOINT
    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidEmptyStringAndRegionNull_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_EMPTY_STRING_ENDPOINT, NULL_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidEmptyStringAndRegionEmpty_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_EMPTY_STRING_ENDPOINT, EMPTY_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidEmptyStringAndRegionInvalid_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_EMPTY_STRING_ENDPOINT, INVALID_REGION);
    }

    @Test
    public void getEndpointConfiguration_whenEndpointValidEmptyStringAndRegionValid_throwIllegalArgumentException() {
        final AwsClientBuilder.EndpointConfiguration config = DynamoDbDelegate.getEndpointConfiguration(VALID_EMPTY_STRING_ENDPOINT, VALID_REGION);
        assertEquals(VALID_REGION, config.getSigningRegion());
        assertEquals("https://dynamodb." + VALID_REGION + ".amazonaws.com", config.getServiceEndpoint());
    }

    //VALID_NOT_A_URL_ENDPOINT
    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidNotAUrlAndRegionNull_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_NOT_A_URL_ENDPOINT, NULL_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidNotAUrlAndRegionEmpty_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_NOT_A_URL_ENDPOINT, EMPTY_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidNotAUrlAndRegionInvalid_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_NOT_A_URL_ENDPOINT, INVALID_REGION);
    }

    @Test
    public void getEndpointConfiguration_whenEndpointValidNotAUrlAndRegionValid_returnConfig() {
        final AwsClientBuilder.EndpointConfiguration config = DynamoDbDelegate.getEndpointConfiguration(VALID_NOT_A_URL_ENDPOINT, VALID_REGION);
        assertEquals(VALID_REGION, config.getSigningRegion());
        assertEquals(VALID_NOT_A_URL_ENDPOINT.get(), config.getServiceEndpoint());
    }

    //VALID_DYNAMODB_LOCAL_ENDPOINT
    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidLocalAndRegionNull_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_DYNAMODB_LOCAL_ENDPOINT, NULL_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidLocalAndRegionEmpty_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_DYNAMODB_LOCAL_ENDPOINT, EMPTY_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidLocalAndRegionInvalid_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_DYNAMODB_LOCAL_ENDPOINT, INVALID_REGION);
    }

    @Test
    public void getEndpointConfiguration_whenEndpointValidLocalAndRegionValid_returnsConfig() {
        final AwsClientBuilder.EndpointConfiguration config = DynamoDbDelegate.getEndpointConfiguration(VALID_DYNAMODB_LOCAL_ENDPOINT, VALID_REGION);
        assertEquals(VALID_REGION, config.getSigningRegion());
        assertEquals(VALID_DYNAMODB_LOCAL_ENDPOINT.get(), config.getServiceEndpoint());
    }

    //VALID_DYNAMODB_ENDPOINT
    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidServiceAndRegionNull_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_DYNAMODB_ENDPOINT, NULL_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidServiceAndRegionEmpty_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_DYNAMODB_ENDPOINT, EMPTY_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidServiceAndRegionInvalid_throwIllegalArgumentException() {
        DynamoDbDelegate.getEndpointConfiguration(VALID_DYNAMODB_ENDPOINT, INVALID_REGION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointValidServiceWrongRegionAndRegionValid_returnsConfig() {
        DynamoDbDelegate.getEndpointConfiguration(Optional.of("https://dynamodb.us-east-1.amazonaws.com"), VALID_REGION);
    }

    @Test
    public void getEndpointConfiguration_whenEndpointValidServiceAndRegionValid_returnsConfig() {
        final AwsClientBuilder.EndpointConfiguration config = DynamoDbDelegate.getEndpointConfiguration(VALID_DYNAMODB_ENDPOINT, VALID_REGION);
        assertEquals(VALID_REGION, config.getSigningRegion());
        assertEquals(VALID_DYNAMODB_ENDPOINT.get(), config.getServiceEndpoint());
    }
}
