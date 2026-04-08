##@ general

default: test-unit

help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-36s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ tools

tools: leb89dump  ## Build all tools

leb89dump: tools/leb89dump/main.go  ## Build the LEB89 alphabet dump tool
	go build -o _out/$@ ./tools/leb89dump

##@ testing

test-unit:  ## Run unit tests
	go test ./...

test-unit-cover:  ## Run unit tests with coverage report
	go test -coverprofile=coverage.out ./...

cover-view:  ## View the console coverage report
	go tool cover -func=coverage.out

cover-view-html:  ## View the HTML coverage report
	go tool cover -html=coverage.out
	
