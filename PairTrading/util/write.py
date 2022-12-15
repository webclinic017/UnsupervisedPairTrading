import json 

def writeToJson(data:dict, filePath:str) -> bool:
    
    try:
        with open(data, "w") as outFile:
            json.dump(outFile, filePath, indent=4)
            return True
            
    except Exception as ex:
        print(ex)
        return False
        
    