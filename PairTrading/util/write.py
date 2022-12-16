import orjson as json 

def writeToJson(data:dict, filePath:str) -> bool:
    
    try:
        with open(filePath, "w") as outFile:
            dataStr:str = json.dumps(data, indent=4)
            outFile.write(dataStr)
            return True
            
    except Exception as ex:
        print(ex)
        return False
        
    