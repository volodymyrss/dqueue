from typing import Dict, NewType, Union

NestedDict = NewType('NestedDict', Dict[str, Union[str, object, dict]]) # pure task definition, portable

TaskData = NewType('TaskData', NestedDict) # pure task definition, portable
TaskDict = NewType('TaskDict', NestedDict) # + submission info, portable
TaskEntry = NewType('TaskEntry', NestedDict) # + state and queue info, not portable
