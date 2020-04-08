#
# Makefile for building all things related to this repo
#
NAME := go-common
ORG := pinpt
PKG := $(ORG)/$(NAME)
SHELL := /bin/bash

.PHONY: all test gen

all: test

dependencies:
	@dep ensure
	@go get github.com/mna/pigeon
	$(MAKE) gen

gen:
	@pigeon ./filterexpr/grammar.peg > ./filterexpr/grammar.go

test:
	@go test -v ./... | grep -v "no test"