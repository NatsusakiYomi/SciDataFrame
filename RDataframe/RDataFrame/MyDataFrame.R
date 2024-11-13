# install.packages("reticulate")

library(R6)
library(reticulate)
# library(arrow)
# use_python("C:\\Users\\Yomi\\.conda\\envs\\SCI2DB\\python.exe")
# install.packages("arrow.flight")
# install_pyarrow()
py_install("pyarrow")
py_install("pandas")
pa <- reticulate::import("pyarrow")
fl <- pa$flight

source("client.R")

# 创建一个R6类，表示数据结构
MyDataFrame <- R6Class(
  "MyDataFrame",

  public = list(
    schema = NULL,
    data = NULL,
    level = NULL,
    client = NULL,
    streaming = NULL,
    batch_size = NULL,
    reader = NULL,
    dataset_id = NULL,
    # 初始化方法，设置 schema，data 和 client
    initialize = function(schema=NULL,data=NULL, level=NULL, client=NULL, streaming=NULL, batch_size=NULL, dataset_id=NULL) {
      self$schema <- schema
      self$data <- data
      self$level <- level
      if (is.null(client)) {
        self$client <- Client$new()  # 创建 Client 实例
      } else {
        self$client <- client  # 使用传入的 client
      } # 设置 Arrow Flight client
      self$streaming <- streaming
      self$batch_size <- batch_size
      self$reader <- NULL
      self$dataset_id <- dataset_id
    },

    # get_schema 方法，调用 client 的 get_schema
    get_schema = function() {
      if (is.null(self$client)) {
        stop("Client is not initialized.")
      }
      # 调用 client 的 get_schema 方法（假设 Flight Client 提供该方法）
      tryCatch({
        schema <- self$client$get_schema(self$dataset_id)$to_pandas()  # 假设这是正确的调用方式
        self$schema <- schema  # 设置 schema
        return(self$schema)
      }, error = function(e) {
        cat("Error in fetching schema:", e$message, "\n")
        return(NULL)
      })
    },
    
    concat = function(obj) {
      if (!is.null(self$data)) {
        self$data <- pa$concat_tables(c(self$data, obj))
      } else {
        self$data <- obj
      }
    },
    
    filter_depth_rows = function(df, index) {
      # 获取给定索引行的 'depth' 值
      target_depth <- df[index, "depth"]
      
      # 初始化筛选行的列表
      filtered_rows <- c(index)
      # 循环遍历从 (index + 1) 行开始的所有行
      for (i in (index + 1):nrow(df)) {
        if (df[i, "depth"] > target_depth) {
          filtered_rows <- c(filtered_rows, i)
        } else {
          break  # 碰到第一个不超过目标 depth 的行时停止
        }
      }
      
      # 返回筛选出的行
      return(df[filtered_rows, ])
    },
    
    flat_open = function(name) {
      self$reader <- self$client$flat_open(name,self$streaming,self$batch_size)
      if (is.null(self$batch_size)) {
      #   for (chunk in reticulate::iterate(self$reader)) {
          self$concat(self$reader)
      }
      # }
      return(self$reader)
    },
    
    open = function(Name) {
      slice <- self$schema[self$schema$name == Name, ]
      data = NULL
      tryCatch({
        # 从self$schema中获取第一个匹配项并提取其'type'列
        level <- slice[1, "type"]
        index <- rownames(slice)[1]  # 假设您需要在R中访问索引
        
        # 判断level是否为FOLDER，并调用相应的过滤方法
        if (level == "dir") {
          schema_filtered <- self$filter_depth_rows(self$schema, index)
        } else {
          schema_filtered <- slice
          data <- self$flat_open(Name)
        }
        
        # 创建MyDataFrame对象
        return(MyDataFrame$new(schema = schema_filtered,
                        data = data,
                        level = level,
                        client = self$client))
        
      }, error = function(e) {
        message("Dirs/files not found, please retry.")
        self  # 返回self对象
      })
    }

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
