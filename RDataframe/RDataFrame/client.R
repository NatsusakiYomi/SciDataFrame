
library(R6)
library(reticulate)
# library(arrow)
# use_python("C:\\Users\\Yomi\\.conda\\envs\\SCI2DB\\python.exe")
# install.packages("arrow.flight")
# install_pyarrow()
pa <- reticulate::import("pyarrow")
fl <- pa$flight

Client <- R6Class("Client",
                  
                  public = list(
                    
                    # 初始化方法，设置 schema，data 和 client
                    initialize = function(...) {
                    }
                    
                    # # get_schema 方法，调用 client 的 get_schema
                    # get_schema = function(dataset_id) {
                    #   if (is.null(self$client)) {
                    #     stop("Client is not initialized.")
                    #   }
                    #   # 调用 client 的 get_schema 方法（假设 Flight Client 提供该方法）
                    #   tryCatch({
                    #     schema <- self$client$get_schema(dataset_id)  # 假设这是正确的调用方式
                    #     self$schema <- schema  # 设置 schema
                    #     return(self$schema)
                    #   }, error = function(e) {
                    #     cat("Error in fetching schema:", e$message, "\n")
                    #     return(NULL)
                    #   })
                    # },
                    # 
                    # concat = function(obj) {
                    #   self$data <- ifelse(!is.null(self$data),pa$concat_tables(c(self$data, obj)),obj)
                    # },
                    # 
                    # flat_open = function(name) {
                    #   self$client$flat_open()
                    # },
                    # 
                    # open = function(name) {
                    #   self$client()
                    # },
                    # 
                  )
)
client <- NULL