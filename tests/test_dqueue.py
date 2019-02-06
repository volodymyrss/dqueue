from __future__ import print_function

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


    r=queue.put(t1)
    assert r['state'] == "submitted"

    print(queue.info)
    assert queue.info['waiting'] == 1

    assert queue.put(t1)['state'] == "waiting"

    time.sleep(0.1)

    assert queue.put(t2)['state'] == "submitted"

    assert queue.info['waiting'] == 2

    assert len(queue.list()) == 2
    print(queue.info)

    task=queue.get()

    t=task.task_data

    assert queue.info['waiting'] == 1
    assert queue.info['running'] == 1


    print("from queue",t)
    print("original",t1)

    assert t==t1
    print(queue.info)

    with pytest.raises(dqueue.CurrentTaskUnfinished):
        t=queue.get()

    print(queue.info)

    queue.task_done()
    print(queue.info)

    assert queue.info['waiting']==1
    assert queue.info['done']==1
    assert queue.info['running']==0
    assert queue.info['failed']==0
    assert queue.info['locked']==0


    
    print(queue.info)

    n_tries = dqueue.n_failed_retries-2

    while n_tries>0:
        print("tries left",n_tries)
        t = queue.get().task_data
        assert t==t2
        print(queue.info)

        queue.task_failed()
        n_tries-=1

    print(queue.info)
    
    assert queue.info['waiting']==0
    assert queue.info['done']==1
    assert queue.info['running']==0
    assert queue.info['failed']==1
    assert queue.info['locked']==0
    
    return

    with pytest.raises(dqueue.Empty):
        queue.get()


    print(queue.info)


def test_locked_jobs():
    import dqueue

    queue=dqueue.Queue("test-queue")
    queue.wipe(["waiting","done","running","locked","failed"])

    print("status:\n",queue.show())

    assert queue.info['waiting']==0

    t1 = dict(test=1, data=2)
    t2 = dict(test=1, data=3)

    assert queue.put(t1,depends_on=[t2])['state']=="submitted"

    time.sleep(0.1)
    queue.put(t2)

    print(queue.info)

    assert len(queue.list("waiting")) == 1
    assert len(queue.list("locked")) == 1
    print(queue.info)

    print("trying to put dependent again")
    assert queue.put(t1) is not None


    t=queue.get().task_data

    print("from queue",t)
    print("original",t2)

    queue.task_done()
    print("finished dependency")

    print(queue.info)

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

    print("from queue", t)
    print("original", t1)

    assert t == t1

    queue.task_done()
    with pytest.raises(dqueue.Empty):
        queue.get()
    print(queue.info)
