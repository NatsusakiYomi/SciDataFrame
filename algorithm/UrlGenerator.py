import random
REPLICATED_TIMES=50
REPLICATED_ROWS=200
IS_RANDOM=False

def generate_line(depth):
    prefix="https://download.scidb.cn/download?fileId=672e8beb76eec2e8b44d56a382861190&path="
    line=""
    lines=[]
    suffix=".txt&fileName=filename.txt&api_key=bfd4d663cbf0e5042b9f26fcfb29d71a"
    for i in range(REPLICATED_TIMES):
        line = prefix
        for j in range(depth):
            line+="/"+str(random.randint(0,100))
        extend_lines=[line+f"/{i}"+suffix for i in range(REPLICATED_ROWS)]
        lines.extend(extend_lines)
        if IS_RANDOM:
            for j in range(10):
                line=prefix
                for k in range(depth):
                    line += "/" + str(random.randint(0, 100))
                line += suffix
                lines.append(line)

    return lines

if __name__ == "__main__":
    print(generate_line(10))