from dqueue.dqtyping import TaskEntry, TaskDict, NestedDict

import logging
import io
import json
import urllib
import traceback

logger=logging.getLogger(__name__)

def decode_entry_data(entry: TaskEntry) -> TaskDict:
    task_dict = {
                'task_data':
                    {'object_identity':
                        {'factory_name':'??'}},
                'submission_info':
                    {'callback_parameters':
                        {'job_id':['??'],
                         'session_id':['??']}}
                        } # type: ignore

    if 'task_dict_string' not in entry:
        logger.error('entry does not contain task_dict_string field!')
    else:
        try:
            task_dict = json.loads(entry['task_dict_string'])   # type: ignore
            task_dict['submission_info']['callback_parameters']={} # type: ignore
            for callback in task_dict['submission_info'].get('callbacks', []): # type: ignore
                if callback is not None:
                    task_dict['submission_info']['callback_parameters'].update(urllib.parse.parse_qs(callback.split("?",1)[1]))# type: ignore
                else:
                    task_dict['submission_info']['callback_parameters'].update(dict(job_id="unset",session_id="unset"))# type: ignore
        except Exception as e:
            traceback.print_exc()
            logger.error("problem decoding %s", repr(e))
            print("raw entry (undecodable)", entry['task_dict_string'])

    return TaskDict(NestedDict(task_dict))

