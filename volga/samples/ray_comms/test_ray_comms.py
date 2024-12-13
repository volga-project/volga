import random
import time

import ray


@ray.remote
class ActorA:

    def __init__(self):
        self.name = 'actor_a'

    def get_rand(self):
        return random.randint(0, 10)


@ray.remote
class ActorB:

    def __init__(self, actor_a, num_calls):
        self.actor_a = actor_a
        self.num_calls = num_calls

    def talk_to_a(self):
        start = time.time()
        futs = []
        for _ in range(0, self.num_calls):
            # ray.get(self.actor_a.get_rand.remote())
            futs.append(self.actor_a.get_rand.remote())
        ray.get(futs)
        end = time.time()
        run_s = end - start
        per_call = run_s/self.num_calls

        print(f'Finnished in {run_s}s, {per_call}s per call')


with ray.init():
    a = ActorA.remote()
    b = ActorB.remote(a, 100000)
    ray.get(b.talk_to_a.remote())
    time.sleep(2)
