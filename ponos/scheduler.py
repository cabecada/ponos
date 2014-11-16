from __future__ import print_function

import logging
import uuid
import json
import os

import mesos.interface
import mesos.native
from mesos.interface import mesos_pb2

from kazoo.client import KazooClient

from protobuf_to_dict import protobuf_to_dict

logging.basicConfig(level=logging.INFO)

def get_offer_resource(offer, name):
    for resource in offer["resources"]:
        if resource.get("name") == name:
            return resource["scalar"]["value"]

def job_fits_in_offer(job, offer):
    "Only looks at cpus and memory for now"
    if get_offer_resource(offer, "cpus") >= job["resources"]["cpus"] and \
       get_offer_resource(offer, "mem") >= job["resources"]["mem"]:
        return True
    else:
        return False

class PonosScheduler(mesos.interface.Scheduler):

    def __init__(self, zk, zk_path):
        self.zk = zk
        self.zk_path = zk_path

    def make_task(self, offer, job):
        task = mesos_pb2.TaskInfo()
        id = str(uuid.uuid4())
        task.task_id.value = id
        task.slave_id.value = offer.slave_id.value
        task.name = "task {}".format(str(id))

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = job["resources"]["cpus"]

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = job["resources"]["mem"]

        task.command.value = job["cmd"]

        return task

    def outstanding_jobs(self):
        jobs = []
        for jid in self.zk.get_children(self.zk_path+"/jobs"):
            path = "/".join([self.zk_path, "jobs", jid])
            jobinfo = json.loads(self.zk.get(path)[0])
            jobinfo["id"] = jid
            jobs.append(jobinfo)
        return jobs


    # mesos API methods

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: {}".format(framework_id))

    def resourceOffers(self, driver, offers):
        tasks = []
        for offer in offers:
            logging.info("Got resource offer")
            outstanding = self.outstanding_jobs()
            logging.info("Outstanding jobs: {}".format([o["name"] for o in outstanding]))
            if not outstanding:
                driver.launchTasks(offer.id, [])  # reject all offers
            for job in outstanding:
                if job_fits_in_offer(job, protobuf_to_dict(offer)):
                    task = self.make_task(offer, job)
                    logging.info("Launching task {task} "
                                 "using offer {offer}.".format(task=task.task_id.value,
                                                               offer=offer.id))
                    # this looks goofy but it doesn't work unless python has a
                    # reference to the list of tasks, I believe this is some
                    # sort of GC problem
                    tasks = [task]
                    driver.launchTasks(offer.id, tasks)
                    self.zk.delete(self.zk_path + "/jobs/" + job["id"])
                    return
            driver.launchTasks(offer.id,[])


if __name__ == '__main__':
    zk_addr = os.environ["PONOS_ZK"]
    zk_path = os.environ["PONOS_ZK_PATH"]
    zk = KazooClient(hosts=zk_addr)
    zk.start()
    # make us a framework
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "python-test"
    driver = mesos.native.MesosSchedulerDriver(
        PonosScheduler(zk, zk_path),
        framework,
        "zk://" + zk_addr + "/mesos"  # assumes running on the master
    )
    driver.run()
