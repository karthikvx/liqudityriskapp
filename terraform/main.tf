terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket for data storage
resource "aws_s3_bucket" "liquidity_data" {
  bucket = var.s3_bucket_name
}

resource "aws_s3_bucket_versioning" "liquidity_data_versioning" {
  bucket = aws_s3_bucket.liquidity_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_encryption" "liquidity_data_encryption" {
  bucket = aws_s3_bucket.liquidity_data.id
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }
}

# Kinesis Data Stream
resource "aws_kinesis_stream" "liquidity_stream" {
  name             = var.kinesis_stream_name
  shard_count      = var.kinesis_shard_count
  retention_period = 24

  shard_level_metrics = [
    "IncomingRecords",
    "OutgoingRecords",
  ]
}

# DynamoDB Table
resource "aws_dynamodb_table" "liquidity_data" {
  name           = var.dynamodb_table_name
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "partition_key"
  range_key      = "sort_key"

  attribute {
    name = "partition_key"
    type = "S"
  }

  attribute {
    name = "sort_key"
    type = "S"
  }

  attribute {
    name = "gsi1_pk"
    type = "S"
  }

  attribute {
    name = "gsi1_sk"
    type = "S"
  }

  global_secondary_index {
    name     = "GSI1"
    hash_key = "gsi1_pk"
    range_key = "gsi1_sk"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}