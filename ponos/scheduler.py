from __future__ import print_function

import logging
import uuid

import mesos.interface
import mesos.native
from mesos.interface import mesos_pb2

logging.basicConfig()


class PonosScheduler(mesos.interface.Scheduler):

    def new_uuided_task(self, offer):
        """Creates a task with whose id is a newly generated uuid on the given
        offer."""
        task = mesos_pb2.TaskInfo()
        id = uuid.uuid4()
        task.task_id.value = str(id)
        task.slave_id.value = offer.slave_id.value
        task.name = "task {}".format(str(id))

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = 1

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = 100
        return task

    # mesos API methods

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: {}".format(framework_id))

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: {}".format(offers))
        # whenever we get an offer, we use raw_input to recieve a
        # filename and data to write there
        tasks = []
        for offer in offers:
            logging.info("Got resource offer: {}".format(offer))
            task = self.new_uuided_task(offer)
            print("Enter filename to write to: ", end="")
            fname = str(raw_input())
            print("Enter data to write: ", end="")
            data = str(raw_input())
            task.command.value = "echo {data} > {fname}".format(data=data,
                                                                fname=fname)
            logging.info("Launching task {task} "
                         "using offer {offer}.".format(task=task.task_id.value,
                                                       offer=offer.id))
            # this looks goofy but it doesn't work unless python has a
            # reference to the list of tasks, I believe this is some
            # sort of GC problem
            tasks = [task]
            driver.launchTasks(offer.id, tasks)

if __name__ == '__main__':
    # make us a framework
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "python-test"
    driver = mesos.native.MesosSchedulerDriver(
        PonosScheduler(),
        framework,
        "zk://localhost:2181/mesos"  # assumes running on the master
    )
    driver.run()
