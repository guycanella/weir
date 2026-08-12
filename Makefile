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
KO_VERSION             ?= v0.19.1
CONTROLLER_GEN         := $(LOCALBIN)/controller-gen
SETUP_ENVTEST          := $(LOCALBIN)/setup-envtest
KO                     := $(LOCALBIN)/ko

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
# --build-tags=integration makes golangci-lint also analyze //go:build integration
# files (e.g. internal/awsclient/awssdk/smoke_integration_test.go), which the
# default build-tagless invocation silently skips. Add more tags here
# (comma-separated) if future WR tasks introduce other build tags.
lint: ## Run golangci-lint
	@$(call need,golangci-lint,install: https://golangci-lint.run/welcome/install/)
	@pkgs=$$(go list ./... 2>/dev/null); status=$$?; \
	if [ $$status -ne 0 ]; then \
	  echo "✗ 'go list ./...' failed — fix go.mod/build errors before linting."; \
	  go list ./... 1>/dev/null; \
	  exit $$status; \
	fi; \
	if [ -z "$$pkgs" ]; then \
	  echo "→ no Go packages yet — nothing to lint (expected on the empty skeleton)."; \
	  exit 0; \
	fi; \
	golangci-lint run --build-tags=integration

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
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_PROFILE= AWS_EC2_METADATA_DISABLED=true \
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

# ko is pinned and auto-installed into ./bin the same way controller-gen and
# setup-envtest are (`make tools`), rather than PATH-checked via the `need`
# macro like kind/kubectl/docker. Reasoning: ko is a single `go install`-able
# Go binary with no external runtime dependency (unlike kind/docker, which
# wrap non-Go system binaries/daemons that `go install` can't produce) — the
# same property that already justified auto-installing controller-gen and
# setup-envtest. Auto-installing it means `make tools` alone reproduces the
# exact pinned ko version on any machine, instead of trusting whatever
# version a human happened to `go install ko@latest` themselves. See
# TOOLS.md for the version rationale.
.PHONY: docker-build
# --local publishes to the LOCAL DOCKER DAEMON, tagged under $(IMG_WORKER)/worker-<hash of import path>
# (ko overrides its ko.local default domain with KO_DOCKER_REPO when it's set),
# not into any kind cluster's node — this is deliberate: `make docker-build`
# should produce an inspectable image (`docker images`, `docker run`) without
# requiring a running kind cluster at all, satisfying WR-026's "make
# docker-build produces an image" Done-when in isolation from kind.
#
# Verified empirically (WR-026): setting KIND_CLUSTER_NAME alongside --local
# does NOT also load the image into kind — ko's --local flag short-circuits
# straight to the Docker-daemon publisher before it ever looks at
# KIND_CLUSTER_NAME or the kind.local sentinel repo, so the two are mutually
# exclusive per invocation, not additive. Loading into kind for a live pod
# run therefore goes through a *separate* ko invocation with
# KO_DOCKER_REPO=kind.local (no --local) instead — see `worker-pod-up` below,
# which uses ko's `ko://` image-reference resolution to build, load into
# kind, and apply in one step (the idiomatic ko workflow), rather than a
# manual `kind load docker-image` step.
docker-build: ## Build the worker image with ko, loaded into the local Docker daemon (see worker-pod-up for the kind-loaded path)
	@test -x $(KO) || { \
	  echo "✗ ko not installed yet — run 'make tools' (WR-026 pins it, see TOOLS.md)"; exit 1; }
	KO_DOCKER_REPO=$(IMG_WORKER) $(KO) build ./cmd/worker --local

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

.PHONY: kind-connect-localstack
# Why this target exists (WR-026): a worker Pod running inside kind needs to
# reach LocalStack over the network to do anything but fail fast and land in
# `Failed` (see cmd/worker/main.go — the first ReceiveMessage call errors out
# against an unreachable/nonexistent endpoint). `localstack-up` binds LocalStack's port
# to 127.0.0.1 only (host loopback — see TOOLS.md's note on why that bind is
# deliberate) and attaches the container to Docker's default `bridge`
# network. kind's cluster nodes, however, run as containers on their OWN
# dedicated `kind` Docker network (auto-created by kind on first `kind
# create cluster` on a machine, and left in place across cluster
# delete/recreate). Those two Docker networks don't share DNS or routing:
# a pod inside kind can reach neither 127.0.0.1:4566 (that loopback belongs
# to the HOST, not to a nested container network) nor `weir-localstack` by
# name (different network, no cross-network DNS resolution).
#
# The fix: attach LocalStack's *container* to the `kind` network too, so
# pods inside kind can resolve and reach it by container name
# (http://weir-localstack:4566 — see hack/worker-pod.yaml's AWS_ENDPOINT_URL)
# over that shared network. This is the standard kind+LocalStack local-dev
# pattern — simpler and more portable than `host.docker.internal` (behaves
# inconsistently across Docker Desktop/Linux/rootless setups) or kind's
# `extraPortMappings` (which only solves host-to-kind inbound reachability,
# not kind-to-a-sibling-container outbound reachability, which is what a pod
# calling out to LocalStack actually needs).
#
# Sequencing constraint: the `kind` Docker network only exists once kind has
# created at least one cluster on this machine (kind creates it lazily on
# first `kind create cluster`; it then persists across delete/recreate). So
# this target depends on `kind-up` (guarantees the network exists) and
# `localstack-up` (guarantees the container exists) as Make prerequisites —
# order between those two doesn't matter to `docker network connect` itself,
# but the `kind` network must exist before this step runs, which depending
# on kind-up guarantees.
#
# Idempotency: `docker network connect` errors if the container is already
# attached to the network. Rather than blindly suppressing all errors (which
# could hide a real failure, e.g. a typo'd network/container name), check
# first — mirroring this Makefile's existing check-before-acting style in
# kind-up/localstack-up's own drift detection — and only connect if not
# already attached.
kind-connect-localstack: kind-up localstack-up ## Attach LocalStack to the kind Docker network so pods inside kind can reach it by container name
	@$(call need,docker,LocalStack and kind both run as Docker containers)
	@if docker network inspect kind --format '{{json .Containers}}' 2>/dev/null | grep -q '"Name":"$(LOCALSTACK_CONTAINER)"'; then \
	  echo "✓ '$(LOCALSTACK_CONTAINER)' already attached to the 'kind' network"; \
	else \
	  docker network connect kind $(LOCALSTACK_CONTAINER); \
	  echo "✓ attached '$(LOCALSTACK_CONTAINER)' to the 'kind' network"; \
	fi

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

.PHONY: worker-pod-up
# Manual-only, NOT wired into `deploy-local` (unlike hello-up): this smoke
# check needs a pre-existing SQS queue + S3 bucket in LocalStack (see
# hack/worker-pod.yaml's header for the exact names/commands) that this task
# does not automate provisioning for — that level of automation is WR-027's
# job. Run this only after creating those by hand.
#
# KO_DOCKER_REPO=kind.local (not IMG_WORKER, not --local) is what makes ko
# resolve the ko://... image reference in hack/worker-pod.yaml by building,
# then loading the image straight into the named kind cluster's node(s) —
# see docker-build's comment above for why --local can't do this instead.
worker-pod-up: kind-connect-localstack ## Build+load+apply the worker smoke-check pod into kind via ko (manual — needs a pre-existing LocalStack queue/bucket, see hack/worker-pod.yaml)
	@test -x $(KO) || { \
	  echo "✗ ko not installed yet — run 'make tools' (WR-026 pins it, see TOOLS.md)"; exit 1; }
	@$(call need,kubectl,WR-004 uses kubectl to talk to the kind cluster)
	@# Delete first: with restartPolicy: Never, a Pod left Failed/Succeeded from a
	@# prior run is never replaced by `ko apply` alone (several Pod spec fields are
	@# immutable on an existing Pod), so re-running this target would either fail
	@# outright or sit out the full `wait --timeout=90s` against a stale pod. A Pod
	@# here is disposable — see worker-pod-down / hello-down — so fold the delete in
	@# as a pre-step instead of requiring a manual worker-pod-down before every re-run.
	@kubectl --context kind-$(KIND_CLUSTER) delete -f hack/worker-pod.yaml --ignore-not-found --wait
	KO_DOCKER_REPO=kind.local KIND_CLUSTER_NAME=$(KIND_CLUSTER) $(KO) apply -f hack/worker-pod.yaml -- --context kind-$(KIND_CLUSTER)
	kubectl --context kind-$(KIND_CLUSTER) wait --for=condition=Ready pod/weir-worker-smoke --timeout=90s

.PHONY: worker-pod-down
worker-pod-down: ## Remove the worker smoke-check pod
	@$(call need,kubectl,WR-004 uses kubectl to talk to the kind cluster)
	@kind get clusters 2>/dev/null | grep -q '^$(KIND_CLUSTER)$$' && \
	  kubectl --context kind-$(KIND_CLUSTER) delete -f hack/worker-pod.yaml --ignore-not-found || true

.PHONY: deploy-local
deploy-local: localstack-up hello-up kind-connect-localstack ## Bring up the full local stack (cluster + LocalStack + hello pod), with LocalStack reachable from inside kind
	@echo "✓ kind + LocalStack + hello pod are up, and LocalStack is reachable from inside kind (see kind-connect-localstack)."
	@echo "→ Reach the hello pod: kubectl --context kind-$(KIND_CLUSTER) port-forward svc/hello 8080:80"
	@echo "→ Worker smoke check (manual, needs a LocalStack queue/bucket first): make worker-pod-up"
	@echo "→ TODO: install KEDA $(KEDA_VERSION) (WR-036), then Helm-install the operator (WR-051)."
	@echo "  Prefer 'tilt up' (see Tiltfile) for the live dev loop — same scope today, extended in WR-011+/WR-026/WR-051."

.PHONY: undeploy-local
undeploy-local: hello-down localstack-down kind-down ## Tear down the full local stack
	@echo "✓ Local stack torn down."

##@ Tooling

.PHONY: tools
tools: $(LOCALBIN) ## Install pinned dev tools into ./bin (controller-gen, setup-envtest, ko — see TOOLS.md)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION)
	GOBIN=$(LOCALBIN) go install github.com/google/ko@$(KO_VERSION)

.PHONY: envtest
envtest: $(LOCALBIN) ## Set up envtest binaries for controller tests (WR-034)
	@test -x $(SETUP_ENVTEST) || { \
	  echo "→ setup-envtest not installed yet. Run 'make tools' first (WR-034)."; exit 0; }
	$(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path