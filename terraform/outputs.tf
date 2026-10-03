output "s3_buckets" {
  description = "Data lake S3 bucket names"
  value       = { for k, v in aws_s3_bucket.data_lake : k => v.id }
}

output "rds_endpoint" {
  description = "RDS MySQL endpoint"
  value       = aws_db_instance.mysql.endpoint
}

output "glue_jobs" {
  description = "Glue job names"
  value       = { for k, v in aws_glue_job.jobs : k => v.name }
}

output "lambda_functions" {
  description = "Lambda function ARNs"
  value       = { for k, v in aws_lambda_function.functions : k => v.arn }
}

output "sns_topic_arn" {
  description = "SNS alerts topic ARN"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "step_function_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.pipeline.arn
}
