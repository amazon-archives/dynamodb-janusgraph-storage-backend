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
package com.amazon.titan.diskstorage.dynamodb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;

/**
 *
 * @author Alexander Patrikalakis
 *
 */
public class DynamoDBDelegateTest {

    public static final String HTTP_LOCALHOST_4567 = "http://localhost:4567";
    public static final String HTTPS_DYNAMODB_US_EAST_1_AMAZONAWS_COM = "https://dynamodb.us-east-1.amazonaws.com";
    public static final String INVALID = "invalid";

    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointNull_throwIllegalArgumentException() {
        DynamoDBDelegate.getEndpointConfiguration(null);
    }
    @Test(expected = IllegalArgumentException.class)
    public void getEndpointConfiguration_whenEndpointEmpty_throwIllegalArgumentException() {
        DynamoDBDelegate.getEndpointConfiguration("");
    }
    @Test
    public void getEndpointConfiguration_whenEndpointInvalid_returnUsEast2() {
        AwsClientBuilder.EndpointConfiguration config = DynamoDBDelegate.getEndpointConfiguration(INVALID);
        assertEquals(Regions.US_EAST_2.getName(), config.getSigningRegion());
        assertEquals(INVALID, config.getServiceEndpoint());
    }
    @Test
    public void getEndpointConfiguration_whenEndpointDynamoDbLocal_returnUsEast2() {
        AwsClientBuilder.EndpointConfiguration config = DynamoDBDelegate.getEndpointConfiguration(HTTP_LOCALHOST_4567);
        assertEquals(Regions.US_EAST_2.getName(), config.getSigningRegion());
        assertEquals(HTTP_LOCALHOST_4567, config.getServiceEndpoint());
    }
    @Test
    public void getEndpointConfiguration_whenEndpointUsEast1_returnUsEast1() {
        AwsClientBuilder.EndpointConfiguration config = DynamoDBDelegate.getEndpointConfiguration(HTTPS_DYNAMODB_US_EAST_1_AMAZONAWS_COM);
        assertEquals(Regions.US_EAST_1.getName(), config.getSigningRegion());
        assertEquals(HTTPS_DYNAMODB_US_EAST_1_AMAZONAWS_COM, config.getServiceEndpoint());
    }
}
