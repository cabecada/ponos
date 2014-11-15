import sys
import os
import uuid
from functools import wraps

import logging
logging.basicConfig()

from flask import Flask, request

from kazoo.client import KazooClient


api = Flask(__name__)


class UserError(Exception):
    pass


def conforms(spec, dict):
    for key in spec:
        if key not in dict or type(dict[key]) != spec[key]:
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
                f(*args, **kwargs)
        return wrapper
    return validator


@api.route("/job", methods=["POST"])
@validate({"name": str, "resources": dict, "cmd": str, "deps": []})
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
    zk.create("/".join(api.config["ZK_PATH"], jid), data)

if __name__ == '__main__':
    assert os.environ["PONOS_ZK"]
    assert os.environ["PONOS_ZK_PATH"]
    zk = KazooClient(hosts=os.environ["PONOS_ZK"])
    zk.start()
    api.config["ZOOKEEPER"] = zk
    api.config["ZK_PATH"] = os.environ["PONOS_ZK_PATH"]
    api.run(debug=True)
