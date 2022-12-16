import json 

def readFromJson(filePath:str) -> dict:
    
    try:
        with open(filePath, "r") as inFile:
            jsonStr = inFile.read()
            return json.loads(jsonStr)
            
    except Exception as ex:
        print(ex)
        return None