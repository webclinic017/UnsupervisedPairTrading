import json 

def writeToJson(data:dict, filePath:str) -> bool:
    
    try:
        with open(filePath, "w") as outFile:
            json.dump(data, outFile, indent=4)
            return True
            
    except Exception as ex:
        print(ex)
        return False
        
    