

def getFirstNonNullIndex(arr:list, featureName:str, initIndex:int=0, duration:int=4) -> int:
    for i in range(initIndex, initIndex+duration):
        if arr[i][featureName]:
            return i 
        
    raise Exception("No valid index")

def getFirstNonNullIndexPair(arr1, arr2:list, feature1Name, feature2Name:str, initIndex:int=0, duration:int=4) -> int:
    for i in range(initIndex, initIndex+duration):
        if arr1[i][feature1Name] and arr2[i][feature2Name]:
            return i 
        
    raise Exception("No valid index pairs")

def getFirstNonNullIndexPairDistance(arr1, arr2:list, feature1Name, feature2Name:str, initIndex:int=0, distance:int=4) -> (int, int):
    for i in range(len(arr1) - distance):
        if arr1[i][feature1Name] and arr2[i+distance][feature2Name]:
            return (i, i+distance)
        
    raise Exception("No valid index pairs")

def getNonNullIndexRange(arr:list, featureName:str,initIndex:int=0, duration:int=4) -> list[int]:
    res:list = []
    i:int = initIndex
    while len(res) < duration:
        if arr[i][featureName]:
            res.append(i)
        i += 1
        
    if not res:
        raise Exception("No valid index range")
    return res

def getNonNullIndexRangePair(arr1, arr2:list, feature1Name, feature2Name:str, initIndex:int=0, duration:int=4) -> list[int]:
    res:list = []
    i:int = initIndex
    while len(res) < duration:
        if arr1[i][feature1Name] and arr2[i][feature2Name]:
            res.append(i)
        i += 1
        
    if len(res) < duration:
        raise Exception("No valid index pairs")
    
    return res