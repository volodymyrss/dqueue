import time
import logging
import traceback
from bravado.client import CallableOperation, SwaggerClient, ResourceDecorator
from bravado.http_future import HttpFuture

logger = logging.getLogger('timed-swagger-client')

timed_operations = {
    "/hub/version": {
        "max_time_s": 1
    },
    "/tasks": {
        "max_time_s": 10
    }
}

def timed_decorator(f):
    logger.debug('decorating %s', f)

    def _f(*args, **kwargs):
        timed_config = None

        if len(args) > 0 and hasattr(args[0], 'operation'):
            operation = args[0].operation

            logger.debug('%s calling %s , %s, %s', f, args, kwargs, operation)

            timed_config = timed_operations.get(operation.path_name)

            if timed_config is not None:
                t0 = time.time()
                logger.info('starting operation %s at %s, typically lasts up to %.2f second(s)', 
                            operation.path_name, 
                            time.strftime("%Y-%m-%dT%T", time.gmtime(t0)), 
                            timed_config['max_time_s'])

        r = f(*args, **kwargs)

        if timed_config is not None:
            t_spent_s = time.time() - t0
            logger.info('completed timed operation in %.2f second(s)', t_spent_s)

            if t_spent_s > timed_config['max_time_s']:
                logger.warning('operation %s takes longer than expected %.2f > %.2f, in %s',
                               operation.path_name,
                               t_spent_s, timed_config['max_time_s'],
                               "\n>> WARNING - may lead to timeouts ".join(("\n".join(traceback.format_stack()[:-1])).split("\n"))
                               )
        
        return r


    return _f

HttpFuture.response = timed_decorator(HttpFuture.response)

