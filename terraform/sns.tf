resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.name_prefix}-pipeline-alerts"
  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
