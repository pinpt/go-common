<div align="center">
	<img width="500" src=".github/logo.svg" alt="pinpt-logo">
</div>

<p align="center" color="#6a737d">
	<strong>This repo contains the common Golang code used by various Pinpoint libraries</strong>
</p>

## Build Status

[![CircleCI](https://circleci.com/gh/pinpt/go-common.svg?style=svg)](https://circleci.com/gh/pinpt/go-common)

We use [CircleCI](https://circleci.com/) for CI/CD

## Setup

You'll need Golang 0.10+ to build and the [dep](github.com/golang/dep/cmd/dep) tool. Once you have these, you need to make sure your vendored dependencies are fetched:

```
make depedencies
```

This will pull the latest dependencies in to your local `vendor` directory.

You can now verify that everything works with:

```
make test
```

## License

All of this code is Copyright &copy; 2018-2019 by Pinpoint Software, Inc. and licensed under the MIT License.
