import sys

import ray

ray.init(address='auto')

actor_name = sys.argv[1]
actor = ray.get_actor(actor_name)
ray.kill(actor)
ray.shutdown()
