
resource "google_service_account" "db_service_account" {
  account_id   = "db-service-account"
  display_name = "Database Service Account"
}

resource "google_service_account_key" "db_sa_key" {
  service_account_id = google_service_account.db_service_account.name
}

resource "google_project_iam_member" "db_sa_role" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.db_service_account.email}"
}
