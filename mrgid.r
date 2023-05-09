install.packages("devtools",repos = "http://cran.us.r-project.org")
library("devtools")
install_github("Ram-N/weatherData")
library(weatherData)
install.packages("mregions",repos = "http://cran.us.r-project.org")
library(mregions)
install.packages("sf",repos = "http://cran.us.r-project.org")
library(sf)
# Get list of all EEZs
# eez <- get_mar_regions(group = "eez")

# # Extract MRGID codes
# mrgid_codes <- eez$MRGID

# # Print MRGID codes
# print(mrgid_codes)

res1 <- mr_records_by_type(type = "EEZ")
mrgid_codes <- res1$MRGID
eezs <- res1$preferredGazetteerName
# Print MRGID codes
print(mrgid_codes)
print(eezs)

