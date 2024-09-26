import pyarrow as pa
import pyarrow.flight as fl

# 创建 FlightClient 实例
client = fl.FlightClient('grpc://localhost:8815')

dataset_id="7e0a4faa0d0649918ae3e94ef34b94af.txt"

# 获取 schema
action = fl.Action("get_schema", dataset_id.encode("utf-8"))
schema_results = client.do_action(action)

# 处理返回的 schema
for schema_bytes in schema_results:
    schema=pa.ipc.open_file(schema_bytes.body).read_all()
    print("Received Schema:", schema)
    print("Received bytes:", schema.nbytes)


    #重建文件目录树


# 用户输入文件夹路径
folder_path = input("Enter folder path to retrieve files: ")
ticket = fl.Ticket(folder_path.encode('utf-8'))

# 使用 do_get 获取文件数据
reader = client.do_get(ticket)
read_table = reader.read_all()
print(read_table)
