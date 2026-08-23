resource "google_sql_database_instance" "postgres" {
  name             = var.db_name
  database_version = "POSTGRES_13"
  region           = var.region

  settings {
    tier = "db-f1-micro"
    availability_type = "ZONAL"
  }

  deletion_protection = false
}

resource "google_sql_database" "database" {
  name     = var.db_name
  instance = google_sql_database_instance.postgres.name
}

resource "google_sql_user" "users" {
  name     = var.db_user
  instance = google_sql_database_instance.postgres.name
  password = var.db_password
}

resource "google_sql_user" "chat_readonly" {
  name     = var.db_readonly_user
  instance = google_sql_database_instance.postgres.name
  password = var.db_readonly_password
  type     = "READONLY"
}