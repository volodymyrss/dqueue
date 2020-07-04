# dqueue

[![Build Status](https://travis-ci.org/volodymyrss/dqueue.svg?branch=master)](https://travis-ci.org/volodymyrss/dqueue)

simple task queue with complex dependencies, task object content is used as task id

```python
queue=dqueue.Queue("test-queue")

# two tasks for test
t1 = dict(test=1, data=2)
t2 = dict(test=1, data=3)

# submits the task
queue.put(t1,depends_on=[t2])['state']

# retrives the next task with resolved dependencies, sets is as current
t=queue.get().task_data

# registers current task as done
queue.task_done()
```
