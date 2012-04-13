NODE ?= /usr/bin/env node

.PHONY: lint
lint:
	${NODE} node_modules/.bin/nodelint --config .jslint.conf lib/*.js lib/targets/*.js bin/*

all: lint
