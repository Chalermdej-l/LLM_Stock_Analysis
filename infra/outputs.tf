output "database_connection" {
  value = {
    instance_name = google_sql_database_instance.postgres.name
    database_name = google_sql_database.database.name
    user          = google_sql_user.users.name
    host          = google_sql_database_instance.postgres.public_ip_address
  }
  sensitive = true
}

output "service_account_key" {
  value     = google_service_account_key.service_account_key.private_key
  sensitive = true
}
