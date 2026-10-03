data "archive_file" "lambda_zips" {
  for_each = var.lambda_functions

  type        = "zip"
  source_file = "${path.module}/../scripts/lambda_function.py"
  output_path = "${path.module}/lambda_${each.key}.zip"
}

resource "aws_lambda_function" "functions" {
  for_each = var.lambda_functions

  function_name = "${local.name_prefix}-${replace(each.key, "_", "-")}"
  role          = aws_iam_role.lambda.arn
  handler       = each.value.handler
  runtime       = each.value.runtime
  timeout       = each.value.timeout
  memory_size   = each.value.memory
  description   = each.value.description

  filename         = data.archive_file.lambda_zips[each.key].output_path
  source_code_hash = data.archive_file.lambda_zips[each.key].output_base64sha256

  tags = local.common_tags
}
