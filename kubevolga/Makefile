SHELL := /bin/bash

KUBECTL ?= kubectl
KUSTOMIZE ?= $(KUBECTL) kustomize

.PHONY: install uninstall deploy undeploy sample unsample render

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
