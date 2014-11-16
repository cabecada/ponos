import sys
import os
import uuid
import json
from functools import wraps

from flask import Flask, request, jsonify

from kazoo.client import KazooClient


api = Flask(__name__)


class UserError(Exception):
    pass


def conforms(spec, data):
    for key in spec.keys():
        if key not in data.keys() or not isinstance(data[key], spec[key]):
            return False
    return True


def validate(spec):
    """
    Decorator for stupid simple validation. Verifies that each key in
    the request body has the type specified in the `spec` dict.

    """
    def validator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            data = request.get_json()
            if not conforms(spec, data):
                raise UserError(
                    "data {data} does not validate with spec {spec}".format(
                        data=data,
                        spec=spec))
            else:
                return f(*args, **kwargs)
        return wrapper
    return validator


@api.route("/job/<id>", methods=["DELETE"])
def delete_job(id):
    """Delete the job with the given id."""
    zk = api.config["ZOOKEEPER"]
    jobpath = "/".join([api.config["ZK_PATH"], "jobs", id])
    zk.delete(jobpath)
    return jsonify({"result": "success", "jobid": id})

@api.route("/job", methods=["POST"])
@validate({"name": basestring, "resources": dict, "cmd": basestring, "deps": list})
def add_job():
    """
    Add a new job to the system. Body should be json of the form:
    {
      "name": . . .
      "resources": . . .
      "cmd": . . .
      "deps": . . .
    }
    """
    data = request.get_json()
    jid = str(uuid.uuid4())
    zk = api.config["ZOOKEEPER"]
    api.logger.info("Writing job {id} to zookeeper.".format(id=jid))
    jobpath = "/".join([api.config["ZK_PATH"], "jobs", jid])
    zk.ensure_path(jobpath)
    zk.set(jobpath, json.dumps(data))
    return jsonify({"result": "success", "jobid": jid})

if __name__ == '__main__':
    assert os.environ["PONOS_ZK"]
    assert os.environ["PONOS_ZK_PATH"]
    zk = KazooClient(hosts=os.environ["PONOS_ZK"])
    zk.start()
    api.config["ZOOKEEPER"] = zk
    api.config["ZK_PATH"] = os.environ["PONOS_ZK_PATH"]
    api.run(debug=True)
