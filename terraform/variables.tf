variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for liquidity data"
  type        = string
  default     = "bank-liquidity-data-prod"
}

variable "kinesis_stream_name" {
  description = "Kinesis stream name"
  type        = string
  default     = "liquidity-risk-stream-prod"
}

variable "dynamodb_table_name" {
  description = "DynamoDB table name"
  type        = string
  default     = "liquidity-risk-data-prod"
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards"
  type        = number
  default     = 5
}
