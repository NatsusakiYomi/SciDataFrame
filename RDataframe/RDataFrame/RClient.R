
library(R6)
library(reticulate)
library(arrow)
# use_python("C:\\Users\\Yomi\\.conda\\envs\\SCI2DB_new\\python.exe")
# install.packages("arrow.flight")
# install_pyarrow()
# py_install("pyarrow")
pa <- reticulate::import("pyarrow")
fl <- pa$flight

RClient <- R6Class("RClient",
                  
                  public = list(
                    fl_client = NULL,
                    dataset_id = NULL,
                    # 初始化方法，设置 schema，data 和 client
                    initialize = function(address="localhost",port=8815) {
                      self$fl_client = fl$connect(paste0("grpc://",address,":", as.character(port)))
                    },
                    get_schema = function(dataset_id) {
                      self$dataset_id = dataset_id
                      action = fl$Action("get_schema", charToRaw(self$dataset_id))
                      schema_results = self$fl_client$do_action(action)
                      schema = NULL
                      # cat("Received Schema:", schema_results, "\n")
                      for (schema_bytes in reticulate::iterate(schema_results)) {
                        # 打开并读取 schema 文件
                        schema_file <- pa$ipc$open_file(schema_bytes$body)
                        schema <- schema_file$read_all()
                        
                        # 输出 schema 信息
                        # cat("Received Schema:", schema, "\n")
                        # cat("Received bytes:", schema$nbytes, "\n")
                      }
                      return(data.frame(schema))
                    },
                    
                    flat_open = function(name,streaming,batch_size,is_preprocess) {
                      ticket <- fl$Ticket(charToRaw(""))  # 将空字符串转换为字节流
                      action = fl$Action("put_folder_path", charToRaw(name))
                      self$fl_client$do_action(action)
                      
                      if (!is.null(is_preprocess)) {
                        action <- fl$Action("recommendation_preprocess", charToRaw("True"))
                        self$fl_client$do_action(action,fl$FlightCallOptions(timeout = 60))
                      }
                      
                      if (!is.null(batch_size)) {
                        action <- fl$Action("batch_size", charToRaw(as.character(batch_size)))
                        results <- self$fl_client$do_action(action)
                      }
                      
                      if (streaming) {
                        action <- fl$Action("streaming", charToRaw("True"))
                        self$fl_client$do_action(action,fl$FlightCallOptions(timeout = 60))
                      } else {
                        # 使用 do_get 获取文件数据
                        action <- fl$Action("streaming", charToRaw("False"))
                        self$fl_client$do_action(action, fl$FlightCallOptions(timeout = 60))
                      }
                      
                      reader <- self$fl_client$do_get(ticket)
                      
                      if (is.null(batch_size)) {
                        reader <- reader$read_all()
                      }
                      
                      return(reader)
                    },
                    close = function() {
                      self$fl_client$close()
                    }

                    # open = function(name) {
                    #   self$client()
                    # },
                    # 
                  )
)
# client <- NULL