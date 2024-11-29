library(R6)
library(reticulate)
library(arrow)
# use_python("C:\\Users\\Yomi\\.conda\\envs\\SCI2DB\\python.exe")
# py_install("pyarrow")
pa <- reticulate::import("pyarrow")
fl <- pa$flight
sys <- import("sys")
sys$path <- c(sys$path, "C:/Users/Yomi/PycharmProjects/SDB2/arrow_flight")
flight_server <- import("UpstreamFlightServer")
RServer <- R6Class(
  "RServer",
  
  public = list(
    fl_server=NULL,
    initialize = function(address,port) {
      self$fl_server <- flight_server$UpstreamFlightServer(paste0("grpc://",address,":", as.character(port)))
    },
    
    output = function(data) {
      self$fl_server$output(data)
    },
    close = function() {
      self$fl_server$close()
    }
  )
)
# L3 <- LETTERS[1:3]
# fac <- sample(L3, 10, replace = TRUE)
# d <- data.frame(x = 1, y = 1:10, fac = fac)
# table <- arrow_table(d)
# rserver <- RServer$new(table)