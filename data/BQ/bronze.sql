

-- Description: Create external tables for bronze dataset in BigQuery
-- please do not forget to replace the bucket path

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.departments_h1` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-1/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.encounters_h1` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-1/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.patients_h1` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-1/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.providers_h1` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-1/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.transactions_h1` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-1/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.departments_h2` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-2/departments/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.encounters_h2` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-2/encounters/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.patients_h2` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-2/patients/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.providers_h2` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-2/providers/*.json']
);

CREATE EXTERNAL TABLE IF NOT EXISTS `project-819f30a6-533e-44c8-a6e.bronze_dataset.transactions_h2` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-16072026/landing/hospital-2/transactions/*.json']
);

---------------------------------------------------------------------------------------------------------------------------