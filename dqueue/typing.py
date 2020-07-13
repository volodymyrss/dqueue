from typing import Dict, NewType

TaskDataType = NewType('TaskDataType', Dict) # pure task definition, portable
TaskDictType = NewType('TaskDictType', Dict) # + submission info and other history, portable
TaskEntryType = NewType('TaskEntryType', Dict) # + state and queue info, not portable
