data "archive_file" "source_code" {
  type        = "zip"
  source {
    content  = templatefile("${path.module}/../../../src/trigger/main.py", {})
    filename = "main.py"
  }
  source {
    content = templatefile("${path.module}/../../../src/trigger/utils.py", {})
    filename = "utils.py"
  }
  source {
    content = templatefile("${path.module}/../../../requirements.txt", {})
    filename = "requirements.txt"
  }
  output_path = "${path.module}/../../cloud_function_source_code.zip"
}
