#### Prepare Data ####


# setup -------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(janitor)
library(jsonlite)
library(googleCloudStorageR)

keys <- read_json("keys.json")
gcs_auth(token = gargle::token_fetch(
  email = keys$google_email,
  scopes = c("https://www.googleapis.com/auth/cloud-platform")))

tmp_dir = tempdir()


# Zillow single family home prices by month and ZIP code ------------------

zillow_tmp_path <- file.path(tmp_dir, "zillow_data.csv")
zillow_url <- paste0("https://files.zillowstatic.com/research/public_csvs/",
                     "zhvi/Zip_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv")
message("Downloading file...")
zillow_data <- read_csv(zillow_url) %>%
  pivot_longer(starts_with("20"), names_to = "date", values_to = "price") %>% 
  clean_names()
message(paste0("Writing file to ", zillow_tmp_path, "..."))
zillow_data %>% write_csv(zillow_tmp_path, na="")
message("Writing file to GCS bucket...")
gcs_upload(zillow_tmp_path,
           name = "zillow_data.csv",
           type = "text/csv",
           bucket = "housing-price-analysis",
           predefinedAcl = "bucketLevel")
