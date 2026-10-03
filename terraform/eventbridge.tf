resource "aws_scheduler_schedule" "daily_pipeline" {
  name       = "${local.name_prefix}-daily-pipeline-schedule"
  group_name = "default"

  schedule_expression          = var.pipeline_schedule
  schedule_expression_timezone = "Asia/Kolkata"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.eventbridge_scheduler.arn
    input    = "{}"
  }
}

resource "aws_iam_role" "eventbridge_scheduler" {
  name = "${local.name_prefix}-eventbridge-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "eventbridge_step_functions" {
  name = "${local.name_prefix}-eventbridge-sfn-policy"
  role = aws_iam_role.eventbridge_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.pipeline.arn]
    }]
  })
}

# ── Glue Failure Alert Rule ──
resource "aws_cloudwatch_event_rule" "glue_failure" {
  name        = "${local.name_prefix}-glue-failure-alert"
  description = "Alert on Glue job failures"

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail      = { state = ["FAILED", "ERROR", "TIMEOUT"] }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "glue_failure_sns" {
  rule      = aws_cloudwatch_event_rule.glue_failure.name
  target_id = "sns-alert"
  arn       = aws_sns_topic.pipeline_alerts.arn
}
