import logging

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

import pytest
import glob
import os
import time

def test_one():
    import dqueue
    
    queue=dqueue.Queue("test-queue")
    queue.wipe(["waiting","done","running","failed","locked"])
    queue.clear_task_history()

    assert queue.info['waiting']==0
    assert queue.info['done']==0
    assert queue.info['running']==0
    assert queue.info['failed']==0
    assert queue.info['locked']==0

    t1 = dict(test=1, data=2)
    t2 = dict(test=1, data=3)

    assert queue.info['waiting']==0
    assert queue.list('waiting')==[]


    s1 = queue.put(t1)
    print("s1", s1)

    assert s1['state'] == "submitted"

    print((queue.info))
    assert queue.info['waiting'] == 1

    assert queue.put(t1)['state'] == "waiting"
    assert queue.put(t1)['state'] == "waiting"
    assert queue.put(t1)['state'] == "waiting"

    time.sleep(0.1)

    s2 = queue.put(t2)

    print("s2", s2)

    assert s2['state'] == "submitted"

    assert queue.info['waiting'] == 2

    assert len(queue.list()) == 2
    print((queue.info))

    task=queue.get()

    l = queue.get_worker_states()
    print("worker states:", l)
    assert len(l) == 1

    t=task.task_data

    assert queue.info['waiting'] == 1
    assert queue.info['running'] == 1



    print(("from queue",t))
    print(("original",t1))

    assert t==t1
    print((queue.info))

    with pytest.raises(dqueue.CurrentTaskUnfinished):
        t=queue.get()

    print((queue.info))

    queue.task_done()
    print((queue.info))

    assert queue.info['waiting']==1
    assert queue.info['done']==1
    assert queue.info['running']==0
    assert queue.info['failed']==0
    assert queue.info['locked']==0


    
    print((queue.info))

    dqueue.core.n_failed_retries = 3
    dqueue.core.sleep_multiplier = 0
    n_tries = dqueue.core.n_failed_retries - 1

    while n_tries>0:
        print("tries left",n_tries)
        t = queue.forgive_task_failures()
        t = queue.get().task_data
        assert t==t2
        print((queue.info))

        queue.task_failed()
        n_tries-=1
    

    print(queue.info)
    
    assert queue.info['waiting']==0
    assert queue.info['done']==1
    assert queue.info['running']==0
    assert queue.info['failed']==1
    assert queue.info['locked']==0
    

    with pytest.raises(dqueue.Empty):
        queue.get()


    print((queue.info))

    l = queue.get_worker_states()
    print("worker states:", l)
    assert len(l) == 1
    
    task_log =  queue.view_log()
    
    print("complete log", len(task_log))

    n = queue.clear_event_log(only_older_than_days=-1, only_kind="task")
    print("clear of task", n)

    m = queue.clear_event_log(only_kind="worker")
    print("clear of worker", m)

    x = queue.clear_event_log()
    print("final clear", x)

    assert x == 0
    assert n + m == len(task_log)

def test_locked_jobs():
    import dqueue

    queue=dqueue.Queue("test-queue")
    queue.wipe(["waiting","done","running","locked","failed"])
    queue.clear_task_history()

    print(("status:\n",queue.show()))

    assert queue.info['waiting']==0

    t1 = dict(test=1, data=2)
    t2 = dict(test=1, data=3)
    t3 = dict(test=1, data=4)

    assert queue.put(t1, depends_on=[t2])['state']=="submitted"

    time.sleep(0.1)
    queue.put(t2)

    print((queue.info))

    print("waiting:", queue.list("waiting")[0])
    task_key = queue.list("waiting")[0]

    task_log =  queue.view_log(task_key)
    print("task_log:", task_log)

    assert len(task_log) == 1

    for tle in task_log:
        print('task_log', tle['timestamp'], tle['message'])

    assert len(queue.list("waiting")) == 1
    assert len(queue.list("locked")) == 1
    print((queue.info))

    print("trying to put dependent again")
    assert queue.put(t1) is not None


    t=queue.get().task_data
    
    task_log =  queue.view_log(task_key)
    print("task_log:", task_log)

    assert len(task_log) == 2

    for tle in task_log:
        print('task_log', tle['timestamp'], tle['message'])

    print(("from queue",t))
    print(("original",t2))

    queue.task_done()
    print("finished dependency")

    print((queue.info))

    print("expected resolved dependecy`")
    #assert len(r)==1
    #assert r[0]['state']=="waiting"
   # assert queue.put(t1)['state'] == "waiting"

  #  assert len(queue.list("waiting")) == 1
  #  assert len(queue.list("locked")) == 0

    with pytest.raises(dqueue.Empty):
        t = queue.get().task_data
    
    r=queue.try_all_locked()
    t = queue.get().task_data

    print(("from queue", t))
    print(("original", t1))

    assert t == t1

    queue.task_done()
    with pytest.raises(dqueue.Empty):
        queue.get()
    print((queue.info))
    

    for tle in task_log:
        print('task_log', tle['timestamp'], tle['message'])
    
    assert len(task_log) == 2

def test_direct_locking():
    import dqueue

    queue=dqueue.Queue("test-queue")
    queue.wipe(["waiting","done","running","locked","failed"])
    queue.clear_task_history()

    print("status:\n",queue.show())

    assert queue.info['waiting']==0

    t1 = dict(test=1, data=2)
    t2 = dict(test=1, data=3)

    assert queue.put(t1)['state']=="submitted"
    
    assert len(queue.list("waiting")) == 1

    assert queue.get().task_data == t1
    
    assert len(queue.list("running")) == 1

    queue.task_locked([t2])
    
    assert len(queue.list("locked")) == 1

    time.sleep(0.1)
    queue.put(t2)

    assert queue.get().task_data == t2

    queue.task_done()
    
    assert len(queue.list("done")) == 1

    logger.info(queue.info)
    
    logger.info("task log...")
    for tle in queue.view_log():
        logger.info('current task_log %s %s', tle['timestamp'], tle['message'])
    
    r=queue.try_all_locked()
    
    assert queue.get().task_data == t1

    queue.task_done()
    
    assert len(queue.list("done")) == 2
    

    logger.info(queue.info)

    
    logger.info("task log...")
    for tle in queue.view_log():
        logger.info('current task_log %s %s', tle['timestamp'], tle['message'])

def test_expiration():
    import dqueue
    
    queue=dqueue.Queue("test-queue")
    queue.wipe(["waiting","done","running","failed","locked"])
    queue.clear_task_history()

    assert queue.info['waiting']==0
    assert queue.info['done']==0
    assert queue.info['running']==0
    assert queue.info['failed']==0
    assert queue.info['locked']==0

    t1 = dict(test=1, data=2)

    assert queue.info['waiting']==0
    assert queue.list('waiting')==[]

    s1 = queue.put(t1)
    print("s1", s1)

    assert s1['state'] == "submitted"

    print((queue.info))
    assert queue.info['waiting'] == 1

    assert queue.put(t1)['state'] == "waiting"

    time.sleep(0.1)

    task=queue.get(2.)
    
    assert queue.info['waiting'] == 0
    assert queue.info['running'] == 1
    assert queue.info['failed'] == 0

    time.sleep(1.5)
    queue.expire_tasks()
    
    assert queue.info['waiting'] == 0
    assert queue.info['running'] == 1
    assert queue.info['failed'] == 0
    
    time.sleep(1.5)
    queue.expire_tasks()

    assert queue.info['waiting'] == 0
    assert queue.info['running'] == 0
    assert queue.info['failed'] == 1

