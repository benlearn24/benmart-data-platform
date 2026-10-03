resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name_prefix}-daily-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({
    Comment = "BenMart Daily Pipeline"
    StartAt = "API_Ingestion"
    States = {
      API_Ingestion = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.functions["api_ingestion"].arn
          Payload      = {}
        }
        ResultPath = "$.lambdaResult"
        Next       = "Run_Glue_Pipeline"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "Notify_Failure"
          ResultPath  = "$.error"
        }]
      }
      Run_Glue_Pipeline = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName   = aws_glue_job.jobs["full_pipeline"].name
          Arguments = { "--ENV" = var.env }
        }
        ResultPath = "$.glueResult"
        Next       = "Notify_Success"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "Notify_Failure"
          ResultPath  = "$.error"
        }]
      }
      Notify_Success = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn
          Subject  = "✅ BenMart Pipeline SUCCESS"
          Message  = "Pipeline completed successfully!"
        }
        End = true
      }
      Notify_Failure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn  = aws_sns_topic.pipeline_alerts.arn
          Subject   = "🔴 BenMart Pipeline FAILED"
          "Message.$" = "States.Format('Pipeline FAILED! Error: {}', $.error.Cause)"
        }
        End = true
      }
    }
  })

  tags = local.common_tags
}
