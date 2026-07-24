SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

# =====================================================================
# Weir — Makefile
# The single source of truth for "how you run things in this project".
# Every agent (Lucas, Flynn, Julia, John, Viktor, Bob, Ana, Lisa, Bruce)
# should prefer `make <target>` over raw commands so behavior is identical
# across the swarm. Referenced by CLAUDE.md, settings.json, and WR-003.
#
# Targets that depend on later phases (kubebuilder, kind, LocalStack, KEDA,
# Helm) are wired but guarded: if a required tool isn't installed yet, the
# target prints what it needs and the WR task that introduces it, instead of
# failing cryptically. Fill them in as those phases land.
# =====================================================================

# --- Project ---------------------------------------------------------
MODULE        ?= github.com/you/weir
KIND_CLUSTER  ?= weir
IMG_OPERATOR  ?= ghcr.io/you/weir-operator
IMG_WORKER    ?= ghcr.io/you/weir-worker

# --- Local ($0) environment: kind + LocalStack -----------------------
AWS_ENDPOINT_URL     ?= http://localhost:4566
AWS_REGION           ?= us-east-2
LOCALSTACK_CONTAINER ?= weir-localstack
LOCALSTACK_SERVICES  ?= s3,sns,sqs,lambda

# --- Tooling (pinned compatible set — see TOOLS.md for the why) ------
# Kubernetes 1.35 generation, chosen because KEDA 2.20's tested window is
# v1.33-v1.35 (https://keda.sh/docs/2.20/operate/cluster/), one minor version
# behind kind's and kubebuilder's own newest defaults (1.36). Revisit together
# with TOOLS.md when KEDA extends support to 1.36+.
LOCALBIN               := $(CURDIR)/bin
ENVTEST_K8S_VERSION    ?= 1.35.0
CONTROLLER_GEN_VERSION ?= v0.20.1
SETUP_ENVTEST_VERSION  ?= v0.0.0-20260305142021-f9589b9f2b9d
KIND_NODE_IMAGE        ?= kindest/node:v1.35.5
KEDA_VERSION           ?= 2.20.1
LOCALSTACK_IMAGE       ?= localstack/localstack:4.14.0
CONTROLLER_GEN         := $(LOCALBIN)/controller-gen
SETUP_ENVTEST          := $(LOCALBIN)/setup-envtest

$(LOCALBIN):
	@mkdir -p $(LOCALBIN)

# Small guard: fail with a helpful message if a required command is missing.
# usage:  $(call need,kind,WR-004 sets this up)
define need
	command -v $(1) >/dev/null 2>&1 || { \
	  echo "✗ '$(1)' not found on PATH — $(2)"; exit 1; }
endef

##@ General

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} \
	  /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } \
	  /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Development

.PHONY: tidy
tidy: ## Tidy and verify go modules
	go mod tidy

.PHONY: fmt
fmt: ## Format Go code
	gofmt -l -w .

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	@$(call need,golangci-lint,install: https://golangci-lint.run/welcome/install/)
	golangci-lint run

.PHONY: build
build: ## Compile all packages
	go build ./...

.PHONY: clean
clean: ## Remove build/test artifacts
	rm -rf $(LOCALBIN) cover.out
	go clean

##@ Testing

.PHONY: test
test: ## Run fast unit tests (race + coverage) — the TDD inner loop
	@pkgs=$$(go list ./... 2>/dev/null); status=$$?; \
	if [ $$status -ne 0 ]; then \
	  echo "✗ 'go list ./...' failed — fix go.mod/build errors before running tests."; \
	  go list ./... 1>/dev/null; \
	  exit $$status; \
	fi; \
	if [ -z "$$pkgs" ]; then \
	  echo "→ no Go packages yet — nothing to test (expected on the empty skeleton)."; \
	  exit 0; \
	fi; \
	go test ./... -race -coverprofile=cover.out

.PHONY: cover
cover: test ## Show coverage summary
	@if [ ! -f cover.out ]; then \
	  echo "→ no cover.out yet — run 'make test' once packages exist."; \
	  exit 0; \
	fi; \
	go tool cover -func=cover.out

.PHONY: test-integration
test-integration: ## Run kind + LocalStack integration tests (needs deploy-local)
	@$(call need,kind,WR-004 sets up the local cluster)
	@docker ps --format '{{.Names}}' | grep -q '^$(LOCALSTACK_CONTAINER)$$' || { \
	  echo "✗ LocalStack not running — start it with 'make localstack-up' (WR-004)"; exit 1; }
	AWS_ENDPOINT_URL=$(AWS_ENDPOINT_URL) AWS_REGION=$(AWS_REGION) \
	  go test -tags=integration ./... -count=1

##@ Code generation (kubebuilder / controller-gen — WR-011..WR-014)

.PHONY: manifests
manifests: ## Generate CRDs + RBAC manifests
	@if [ ! -x $(CONTROLLER_GEN) ]; then \
	  echo "→ controller-gen not installed yet. Run 'make tools' (added in WR-011)."; \
	  exit 0; \
	fi; \
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook \
	  paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: ## Generate deepcopy code
	@if [ ! -x $(CONTROLLER_GEN) ]; then \
	  echo "→ controller-gen not installed yet. Run 'make tools' (added in WR-011)."; \
	  exit 0; \
	fi; \
	if [ ! -f hack/boilerplate.go.txt ]; then \
	  echo "→ hack/boilerplate.go.txt not scaffolded yet — lands with kubebuilder init (WR-011)."; \
	  exit 0; \
	fi; \
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

##@ Images (ko — WR-026)

.PHONY: docker-build
docker-build: ## Build the worker/operator images with ko
	@$(call need,ko,WR-026 introduces ko for image builds)
	KO_DOCKER_REPO=$(IMG_WORKER) ko build ./cmd/worker --local

##@ Local environment ($0: kind + LocalStack — WR-004)

.PHONY: kind-up
kind-up: ## Create the local kind cluster, recreating it if its node image drifts from the pin (see TOOLS.md)
	@$(call need,kind,WR-004 sets up the local cluster)
	@$(call need,docker,kind runs cluster nodes as containers)
	@if kind get clusters 2>/dev/null | grep -q '^$(KIND_CLUSTER)$$'; then \
	  running=$$(docker inspect --format '{{.Config.Image}}' $(KIND_CLUSTER)-control-plane 2>/dev/null); \
	  if [ "$$running" = "$(KIND_NODE_IMAGE)" ]; then \
	    echo "✓ '$(KIND_CLUSTER)' cluster already runs pinned node image ($$running)"; \
	  else \
	    echo "⚠ '$(KIND_CLUSTER)' cluster runs '$$running', pinned image is '$(KIND_NODE_IMAGE)' — recreating"; \
	    kind delete cluster --name $(KIND_CLUSTER); \
	    kind create cluster --name $(KIND_CLUSTER) --image $(KIND_NODE_IMAGE); \
	  fi; \
	else \
	  kind create cluster --name $(KIND_CLUSTER) --image $(KIND_NODE_IMAGE); \
	fi

.PHONY: kind-down
kind-down: ## Delete the local kind cluster
	@$(call need,kind,WR-004 sets up the local cluster)
	kind delete cluster --name $(KIND_CLUSTER) || true

.PHONY: localstack-up
# Mounting the host Docker socket below is a deliberate, scoped exception:
# LocalStack Community's Lambda executor needs it to spin up Lambda execution
# containers via the host Docker daemon (it can't run containers inside its
# own container without it). This mirrors LocalStack's own official
# docker-compose.yml. It is local-dev-only ($0 path, kind + LocalStack) and
# is never used on [cloud] tasks, where real AWS Lambda runs instead — so it
# never reaches a real account. Treat it like host-root access: don't widen
# this pattern to other targets without the same justification.
localstack-up: ## Start LocalStack (S3/SNS/SQS/Lambda) in Docker, reconciling stopped/drifted containers (pinned image/ports/mounts/env — see TOOLS.md)
	@$(call need,docker,LocalStack runs in a container)
	@if docker ps -a --format '{{.Names}}' | grep -q '^$(LOCALSTACK_CONTAINER)$$'; then \
	  image=$$(docker inspect --format '{{.Config.Image}}' $(LOCALSTACK_CONTAINER) 2>/dev/null); \
	  ports=$$(docker inspect --format '{{json .HostConfig.PortBindings}}' $(LOCALSTACK_CONTAINER) 2>/dev/null); \
	  binds=$$(docker inspect --format '{{json .HostConfig.Binds}}' $(LOCALSTACK_CONTAINER) 2>/dev/null); \
	  env=$$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' $(LOCALSTACK_CONTAINER) 2>/dev/null); \
	  want_ports='{"4566/tcp":[{"HostIp":"127.0.0.1","HostPort":"4566"}]}'; \
	  want_binds='["/var/run/docker.sock:/var/run/docker.sock"]'; \
	  if [ "$$image" != "$(LOCALSTACK_IMAGE)" ] || [ "$$ports" != "$$want_ports" ] || \
	     [ "$$binds" != "$$want_binds" ] || \
	     ! printf '%s\n' "$$env" | grep -qx 'SERVICES=$(LOCALSTACK_SERVICES)'; then \
	    echo "⚠ '$(LOCALSTACK_CONTAINER)' config drifted from the pin (image, port binding, mounts, or SERVICES) — recreating"; \
	    docker rm -f $(LOCALSTACK_CONTAINER) >/dev/null; \
	    docker run -d --name $(LOCALSTACK_CONTAINER) -p 127.0.0.1:4566:4566 \
	      -e SERVICES=$(LOCALSTACK_SERVICES) \
	      -v /var/run/docker.sock:/var/run/docker.sock \
	      $(LOCALSTACK_IMAGE); \
	  elif [ "$$(docker inspect --format '{{.State.Running}}' $(LOCALSTACK_CONTAINER))" = "true" ]; then \
	    echo "✓ '$(LOCALSTACK_CONTAINER)' already running on pinned config ($$image)"; \
	  else \
	    echo "→ '$(LOCALSTACK_CONTAINER)' exists on pinned config but stopped — starting"; \
	    docker start $(LOCALSTACK_CONTAINER) >/dev/null; \
	  fi; \
	else \
	  docker run -d --name $(LOCALSTACK_CONTAINER) -p 127.0.0.1:4566:4566 \
	    -e SERVICES=$(LOCALSTACK_SERVICES) \
	    -v /var/run/docker.sock:/var/run/docker.sock \
	    $(LOCALSTACK_IMAGE); \
	fi

.PHONY: localstack-down
localstack-down: ## Stop and remove LocalStack
	@$(call need,docker,LocalStack runs in a container)
	docker rm -f $(LOCALSTACK_CONTAINER) 2>/dev/null || true

.PHONY: hello-up
hello-up: kind-up ## Apply the hello-pod smoke-check manifest and wait for it to be ready
	@$(call need,kubectl,WR-004 uses kubectl to talk to the kind cluster)
	@# A just-created kind cluster answers API requests before its namespace
	@# controller has created the default ServiceAccount pods need — wait
	@# for it so `apply` right after `kind-up` doesn't race and fail.
	@for i in $$(seq 1 30); do \
	  kubectl --context kind-$(KIND_CLUSTER) get serviceaccount default >/dev/null 2>&1 && exit 0; \
	  sleep 2; \
	done; \
	echo "✗ timed out waiting for the default ServiceAccount"; exit 1
	kubectl --context kind-$(KIND_CLUSTER) apply -f hack/hello-pod.yaml
	kubectl --context kind-$(KIND_CLUSTER) wait --for=condition=Ready pod/hello --timeout=90s

.PHONY: hello-down
hello-down: ## Remove the hello-pod smoke-check manifest
	@$(call need,kubectl,WR-004 uses kubectl to talk to the kind cluster)
	@kind get clusters 2>/dev/null | grep -q '^$(KIND_CLUSTER)$$' && \
	  kubectl --context kind-$(KIND_CLUSTER) delete -f hack/hello-pod.yaml --ignore-not-found || true

.PHONY: deploy-local
deploy-local: localstack-up hello-up ## Bring up the full local stack (cluster + LocalStack + hello pod)
	@echo "✓ kind + LocalStack + hello pod are up."
	@echo "→ Reach the hello pod: kubectl --context kind-$(KIND_CLUSTER) port-forward svc/hello 8080:80"
	@echo "→ TODO: install KEDA $(KEDA_VERSION) (WR-036), then Helm-install the operator (WR-051)."
	@echo "  Prefer 'tilt up' (see Tiltfile) for the live dev loop — same scope today, extended in WR-011+/WR-026/WR-051."

.PHONY: undeploy-local
undeploy-local: hello-down localstack-down kind-down ## Tear down the full local stack
	@echo "✓ Local stack torn down."

##@ Tooling

.PHONY: tools
tools: $(LOCALBIN) ## Install pinned dev tools into ./bin (controller-gen, setup-envtest — see TOOLS.md)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION)

.PHONY: envtest
envtest: $(LOCALBIN) ## Set up envtest binaries for controller tests (WR-034)
	@test -x $(SETUP_ENVTEST) || { \
	  echo "→ setup-envtest not installed yet. Run 'make tools' first (WR-034)."; exit 0; }
	$(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path