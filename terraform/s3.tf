resource "aws_s3_bucket" "data_lake" {
  for_each = toset(var.data_lake_layers)
  bucket   = "${local.name_prefix}-${each.value}"

  tags = merge(local.common_tags, {
    Layer = each.value
  })
}
