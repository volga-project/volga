import ray

import volga

RAY_ADDR = 'ray://ray-cluster-kuberay-head-svc:10001'
# RAY_ADDR = 'ray://127.0.0.1:12345'

REMOTE_RAY_CLUSTER_TEST_RUNTIME_ENV = {
    'pip': [
        'pydantic==1.10.13',
        'simplejson==3.19.2',
        'orjson==3.10.6',
        'aenum==3.1.15',
        'sortedcontainers==2.4.0',
        'acsylla==0.2.1',
        'scyllapy==1.3.3',
        'redis==5.2.1'
    ],
    'py_modules': [
        volga,
        '/Users/anov/Desktop/volga-rust-builds/volga_rust-0.1.0-cp310-cp310-manylinux_2_35_x86_64.whl'
        # '/Users/anov/IdeaProjects/volga/rust/target/wheels/volga_rust-0.1.0-cp310-cp310-manylinux_2_17_aarch64.manylinux2014_aarch64.whl'
    ]
}
