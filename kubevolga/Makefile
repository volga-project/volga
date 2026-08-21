SHELL := /bin/bash

KUBECTL ?= kubectl
KUSTOMIZE ?= $(KUBECTL) kustomize

.PHONY: install uninstall deploy undeploy sample unsample render bench unbench

install:
	$(KUSTOMIZE) config/crd | $(KUBECTL) apply -f -

uninstall:
	$(KUSTOMIZE) config/crd | $(KUBECTL) delete --ignore-not-found=true -f -

deploy:
	$(KUSTOMIZE) config/default | $(KUBECTL) apply -f -

undeploy:
	$(KUSTOMIZE) config/default | $(KUBECTL) delete --ignore-not-found=true -f -

sample:
	$(KUSTOMIZE) config/samples | $(KUBECTL) apply -f -

unsample:
	$(KUSTOMIZE) config/samples | $(KUBECTL) delete --ignore-not-found=true -f -

render:
	$(KUSTOMIZE) config/default

# Prom + Grafana on infra (kubevolga/hack/bench). Opt-in; not part of kube tests.
bench:
	$(KUSTOMIZE) hack/bench | $(KUBECTL) apply -f -

unbench:
	$(KUSTOMIZE) hack/bench | $(KUBECTL) delete --ignore-not-found=true -f -
