from dqueue.typing import TaskEntryType, TaskDictType

import logging
import io
import json
import urllib
import traceback

logger=logging.getLogger(__name__)

def decode_entry_data(entry: TaskEntryType) -> TaskDictType:
    task_dict = {
                'task_data':
                    {'object_identity':
                        {'factory_name':'??'}},
                'submission_info':
                    {'callback_parameters':
                        {'job_id':['??'],
                         'session_id':['??']}}
                        } # type: ignore

    if 'entry' not in entry:
        logger.error('entry does not contain entry field!')
    else:
        try:
            task_dict = json.loads(entry['entry']) 
            task_dict['submission_info']['callback_parameters']={} # type: ignore
            for callback in task_dict['submission_info'].get('callbacks', []): # type: ignore
                if callback is not None:
                    task_dict['submission_info']['callback_parameters'].update(urllib.parse.parse_qs(callback.split("?",1)[1]))# type: ignore
                else:
                    task_dict['submission_info']['callback_parameters'].update(dict(job_id="unset",session_id="unset"))# type: ignore
        except Exception as e:
            traceback.print_exc()
            logger.error("problem decoding %s", repr(e))
            print("raw entry (undecodable)", entry['entry'])

    return TaskDictType(task_dict)

