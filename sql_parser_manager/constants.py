from enum import Enum

class CustomEnum(Enum):
    @classmethod
    def choices(cls) -> list[str]:
        return [key.value for key in cls]
    
class ExecutionStatus(CustomEnum):
    WAITING = "Waiting"
    EXECUTING = "Executing"
    FINISHED = "Finished"

class SetOperations(CustomEnum):
    EXCEPT = "except"
    INTERSECT = "intersect"
    UNION = "union"

class AggregationFunctions(CustomEnum):
    AVGERAGE = 'avg'
    COUNT = 'count'
    MAX = 'max'
    MIN = 'min'
    SUM = 'sum'
