module "service_accounts" {
  source  = "terraform-google-modules/service-accounts/google"
  version = "4.2.2"

  project_id   = var.project_id
  display_name = "Service Account for LLM service"

  names = [
    "llm-stock-sa"
  ]

  project_roles = [
    "${var.project_id}=>roles/cloudsql.client"
  ]

  providers = {
    google = google
  }
}


resource "google_service_account_key" "service_account_key" {
  service_account_id = "llm-stock-sa"

  depends_on = [module.service_accounts]
}
