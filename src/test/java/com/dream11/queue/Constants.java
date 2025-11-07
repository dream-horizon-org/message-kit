package com.dream11.queue;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {
  public static final String LOCALSTACK_DOCKER_IMAGE = "localstack/localstack:1.3.0";
  public static final String SQS_QUEUE = "test";
  public static final String SQS_ENDPOINT = "SQS_ENDPOINT";
  public static final String SNS_ENDPOINT = "SNS_ENDPOINT";
  public static final String AWS_REGION = "aws.region";
  public static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
  public static final String AWS_SECRET_ACCESS_KEY = "aws.secretAccessKey";
}
