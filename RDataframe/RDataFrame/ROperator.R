# library(R6)
# library(reticulate)
# source("RClient.R")
# source("RServer.R")
# source("SciDataFrame.R")
ROperator <- R6Class(
  "ROperator",
  
  public = list(
    input_client=NULL,
    output_server=NULL,
    data=NULL,
    df=NULL,
    # 初始化方法，设置 schema，data 和 client
    initialize = function(input_address="127.0.0.1",input_port=8815,output_address="127.0.0.1",output_port=8816) {
      self$input_client=RClient$new(address=input_address,port=input_port)
      self$output_server=RServer$new(address=output_address,port=output_port)
    },
    get_data = function(){
      df <- SciDataFrame$new(dataset_id="new.txt",streaming=TRUE,client=self$client,is_preprocess="True")
      df$get_schema()
      df$flat_open("df-1m-unique.csv")
      self$data=df$data
    },
    get_schema = function() {
      df <- SciDataFrame$new(dataset_id="GSA_url_2.txt",streaming=TRUE,client=self$client,is_preprocess="False")
      df$get_schema()
      self$df<-df
    },
    
    filter = function() {
      self$df<-self$df$filter("^Ciona")
      self$output_server$output(arrow_table(self$df$schema))
    },

    set_flag = function() {
      self$output_server$output(self$schema)
    },
    start_preprocess = function() {
      r_df<-as.data.frame(self$data)
      df_summary <- r_df %>%
        group_by(column1, column2) %>%
        summarise(column3 = n(), .groups = "drop")
      ggplot(df_summary, aes(x = column1)) +
             geom_histogram(binwidth = 1, fill = "skyblue", color = "black") +
             labs(title = "用户分布", x = "用户 ID", y = "交互次数")
    },
    output_schema = function() {
      self$output_server$output(self$df$schema)
    },
    output_data = function() {
      self$output_server$output(self$data)
    },
    close = function() {
      self$input_client$close()
      self$output_server$close()
    }
    
  )
)

