# install.packages("reticulate")

library(R6)
library(reticulate)
# library(arrow)
# use_python("C:\\Users\\Yomi\\.conda\\envs\\SCI2DB\\python.exe")
# install.packages("arrow.flight")
# install_pyarrow()
pa <- reticulate::import("pyarrow")
fl <- pa$flight

source("client.R")

# 创建一个R6类，表示数据结构
MyDataFrame <- R6Class(
  "MyDataFrame",

  public = list(

    # 初始化方法，设置 schema，data 和 client
    initialize = function() {
      # self$schema <- NULL
      # self$data <- NULL
      self$client <- Client$new()  # 设置 Arrow Flight client
    },

    # get_schema 方法，调用 client 的 get_schema
    get_schema = function(dataset_id) {
      if (is.null(self$client)) {
        stop("Client is not initialized.")
      }
      # 调用 client 的 get_schema 方法（假设 Flight Client 提供该方法）
      tryCatch({
        schema <- self$client$get_schema(dataset_id)  # 假设这是正确的调用方式
        self$schema <- schema  # 设置 schema
        return(self$schema)
      }, error = function(e) {
        cat("Error in fetching schema:", e$message, "\n")
        return(NULL)
      })
    },
    
    concat = function(obj) {
      self$data <- ifelse(!is.null(self$data),pa$concat_tables(c(self$data, obj)),obj)
    },
    
    flat_open = function(name) {
      self$client$flat_open
    },
    
    open = function(name) {
      self$client
    },

  )
)

# 使用实例
# 假设 Arrow Flight 客户端已经被初始化
# local_client <- flight_connect(port = 8815)
# flight_get(local_client, "pollution_data" )

# 创建一个 MyDataFrame 对象
# ds <- MyDataFrame$new(client = local_client)

# # 获取 schema
# ds$get_schema()
#
# # 设置数据（假设 data 是一个 Arrow Table）
# ds$set_data(arrow::Table$create(col1 = 1:10, col2 = letters[1:10]))

# 获取数据
# ds$get_data()
