# Cloud Monitoring dashboard for kalshiflow. JSON spec is authoritative; do not
# copy-paste from a Cloud Console export (those include defaults that the API
# round-trips back, and Terraform cannot detect key removals without a paired
# non-removal change).
resource "google_monitoring_dashboard" "main" {
  dashboard_json = file("${path.module}/dashboards/main.json")
}
