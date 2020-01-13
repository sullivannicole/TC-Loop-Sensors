#' Function to pull metro loop detector data from MnDOT's JSON feed
#'
#' Create a tidy dataframe containing volume and occupancy from MnDOT's JSON feed for a single date and sensor
#'
#' @param pull_date character string of date in format year-month-day, day-month-year or month-day-year
#' @param date_fmt string of date format:  either "ymd", "mdy", or "dmy"
#' @param sensor character or integer of sensor to pull; dataframe containing all possible sensors can be obtained from sensor_pull() function
#'
#' @return dataframe containing variables volume, occupancy, sensor, date, time.  Note that occupancy *can* be missing while volume data exists and vice versa.  It is unknown how a loop could be monitoring volume and not occupancy. Also note that if you assign the output of pull_loops, the result is returned in-memory, and there must be sufficient space in-memory to do so.
#' 
#' @examples
#' loop_data <- pull_loops("20190101", "ymd", 8564)
#'
#' mult_loop_data <- pmap(list(c("20190101", "20190102"), "ymd", 8564), pull_loops)
#' loops_full <- rbindlist(mult_loop_data)
#'

#' @export 

pull_loops <- function(pull_date, date_fmt, sensor) {
  
  library(tidyverse)
  library(data.table)
  library(lubridate)
  library(chron)
  library(rowr)
  
  extension_pull <- function (ext, ...) {
    
    library(tidyverse)
    library(data.table)
    library(jsonlite)
    
    if (date_fmt == "ymd") {
      pull_year <- year(as_date(ymd(pull_date)))
    } else if (date_fmt == "mdy") {
      pull_year <- year(as_date(mdy(pull_date)))
    } else if (date_fmt == "dmy") {
      pull_year <- year(as_date(dmy(pull_date)))
    } else {
      "Date format not supported.  Try 'ymd', 'mdy', or 'dmy'."
    }
    
    df_default <- as_tibble(NA, validate = F) 
    
    try(df_default <- enframe(fromJSON(paste0("http://data.dot.state.mn.us:8080/trafdat/metro/", pull_year, "/", pull_date, "/", sensor, ".", ext, "30.json"))) %>%
          select(-name))
    
    return(df_default)
    
  }

  #exts <- c("v", "c")
  #loops_ls <- map(exts, extension_pull)
  
  volume <- extension_pull("v")
  occupancy <- extension_pull("c")
  
  loop_uneven <- rowr::cbind.fill(volume, occupancy, fill = NA)
  names(loop_uneven) <- c("volume", "occupancy")
  
  loop_date_sensor <- loop_uneven %>%
    mutate(date = pull_date,
           sensor = sensor)
  
  #Add time
  cbind(loop_date_sensor, enframe(chron::times("00:00:00") + (1:2880)/2880) %>% transmute(time = value))

}

#' Function to pull all sensor IDs in the Twin Cities metro
#'
#' Create a tidy dataframe containing sensor IDs for MnDOT metro district, mainly to be used with pull_loops
#'
#' @return dataframe containing variable "detector"
#' 
#' @examples
#' sensors <- sensor_pull()
#'
#' @export 

sensor_pull <- function() {
  enframe(trimws(xml_attr(xml_find_all(metro_config, "//detector"), "name"))) %>%
    transmute(detector = value)
}

#' Function to pull sensor configuration in the Twin Cities metro
#'
#' Read MnDOT JSON feed and wrange into a tidy dataframe containing 20 variables related to sensor configuration.  Useful for mapping (contains lat/lons) and calculating performance measures (contains detector_field).
#'
#' @param return_opt how to return the data.  "within_dir" will return the data within the directory as a csv entitled "Configuration of Metro Detectors <<date in format yyyy-mm-dd>>". "in-memory" will return the data in R, but requires assignment.
#' @return dataframe containing 20 variables
#' 
#' @examples
#' config <- pull_configuration("in-memory") # Assign to an object
#' pull_configuration("within_dir) # No assignment necessary
#'
#' @export 

pull_configuration <- function(return_opt) {
  
  url <- "http://data.dot.state.mn.us/iris_xml/metro_config.xml.gz"
  tmp <- tempfile()
  download.file(url,tmp)
  metro_config <- read_xml(gzfile(tmp))
  
  # ------------------
  # PATHS
  # ------------------
  
  # Detector paths - connect rnodes and corridors to this
  detector_paths <- enframe(xml_path(xml_find_all(metro_config, "//detector"))) %>%
    mutate(detector_path = value) %>%
    separate(detector_path, into = c('front', 'tms_config', 'device', 'rnode', 'detector'), sep = '/') %>%
    unite(rnode_path, front, tms_config, device, rnode, sep = '/') %>%
    mutate(rnode_path = trimws(rnode_path)) %>%
    mutate(corridor_path = rnode_path) %>%
    separate(corridor_path, into = c('front', 'tms_config', 'device', 'rnode'), sep = '/') %>%
    unite(corridor_path, front, tms_config, device, sep = '/') %>%
    mutate(corridor_path = trimws(corridor_path)) %>%
    select(-name) %>%
    rename(detector_path = value)
  
  # Rnode paths
  rnode_paths <- enframe(xml_path(xml_find_all(metro_config, "//r_node"))) %>%
    transmute(rnode_path = value)
  
  # Corridor paths
  corridor_paths <- enframe(xml_path(xml_find_all(metro_config, "//corridor"))) %>%
    transmute(corridor_path = value)
  
  # ------------------
  # ATTRIBUTES (rnodes & detectors)
  # ------------------
  
  attr_clean <- function(category, attribute) {
    trimws(xml_attr(xml_find_all(metro_config, paste0("//", category)), attribute))
  }
  
  d_attr_ls <- list("name",
                    "label",
                    "category",
                    "lane",
                    "field",
                    "abandoned")
  
  rn_attr_ls <- list("name",
                     "n_type",
                     "transition",
                     "label",
                     "lon",
                     "lat",
                     "lanes",
                     "shift",
                     "s_limit",
                     "station_id",
                     "attach_side")
  
  c_attr_ls <- list("route", "dir")
  
  attr_to_df <- function(category, attr_ls) {
    attributes_ls <- map2(category, attr_ls, attr_clean)
    names(attributes_ls) <- paste(category, attr_ls, sep = "_")
    bind_rows(attributes_ls)
  }
  
  attr_all_ls <- list(d_attr_ls, rn_attr_ls, c_attr_ls)
  categories <- list("detector", "r_node", "corridor")
  
  attributes_full <- map2(categories, attr_all_ls, attr_to_df)
  names(attributes_full) <- list("d_attributes_df", "r_attributes_df", "c_attributes_df")
  list2env(attributes_full ,.GlobalEnv)
  
  # Bind paths to attributes
  d_paths_attr <- bind_cols(detector_paths, d_attributes_df)
  rnode_paths_attr <- bind_cols(rnode_paths, r_attributes_df)
  corr_paths_attrs <- bind_cols(corridor_paths, c_attributes_df)
  
  detector_rnodes_full <- left_join(d_paths_attr, rnode_paths_attr, by = c('rnode_path'))
  configuration <- left_join(detector_rnodes_full, corr_paths_attrs, by = c('corridor_path'))
  
  config_tidy <- configuration %>%
    select(-rnode, -rnode_path, -detector, -detector_path, -corridor_path) %>%
    mutate(date = Sys.Date())
  
  if (return_opt == "in_memory") {
    
    return(config_tidy)
    
  } else if (return_opt == "within_dir") {
    
    fwrite(config_tidy, paste0("Configuration of Metro Detectors ", Sys.Date(), ".csv"))
    
  }
  
  config_tidy
  
}