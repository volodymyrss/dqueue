from dqueue.typing import TaskEntryType, TaskDictType

import yaml
import logging
import io
import urllib
import traceback

logger=logging.getLogger(__name__)

def decode_entry_data(entry: TaskEntryType) -> TaskDictType:
    task_dict = {'task_data':
                    {'object_identity':
                        {'factory_name':'??'}},
                'submission_info':
                    {'callback_parameters':
                        {'job_id':['??'],
                         'session_id':['??']}}
                }

    if 'entry' not in entry:
        logger.error('entry does not contain entry field!')
    else:
        try:
            task_dict=yaml.load(io.StringIO(entry['entry']), Loader=yaml.Loader)
            task_dict['submission_info']['callback_parameters']={}
            for callback in task_dict['submission_info'].get('callbacks', []):
                if callback is not None:
                    task_dict['submission_info']['callback_parameters'].update(urllib.parse.parse_qs(callback.split("?",1)[1]))
                else:
                    task_dict['submission_info']['callback_parameters'].update(dict(job_id="unset",session_id="unset"))
        except Exception as e:
            traceback.print_exc()
            logger.error("problem decoding %s", repr(e))
            print("raw entry (undecodable)", entry['entry'])

    return task_dict

