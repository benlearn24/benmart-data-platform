resource "aws_glue_job" "jobs" {
  for_each = var.glue_jobs

  name         = "${local.name_prefix}-${replace(each.key, "_", "-")}"
  role_arn     = aws_iam_role.glue.arn
  glue_version = var.glue_version
  description  = each.value.description

  command {
    name            = "glueetl"
    script_location = "s3://${local.raw_bucket}/scripts/${each.value.script_name}"
    python_version  = "3"
  }

  default_arguments = {
    "--ENV"                          = var.env
    "--extra-py-files"               = "s3://${local.raw_bucket}/scripts/glue_package.zip"
    "--additional-python-modules"    = "pyyaml"
    "--job-language"                 = "python"
    "--TempDir"                      = "s3://${local.raw_bucket}/temp/"
    "--enable-metrics"               = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  number_of_workers = each.value.workers
  worker_type       = "G.1X"

  tags = merge(local.common_tags, {
    JobType = each.key
  })
}
