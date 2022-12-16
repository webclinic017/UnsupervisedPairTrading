import json 

def readFromJson(filePath:str) -> dict:
    
    try:
        with open(filePath, "r") as inFile:
            return json.load(inFile)
            
    except Exception as ex:
        print(ex)
        return None