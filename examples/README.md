# HaBolt usage example

## Build

In "habolt" repository :

`govendor sync`

`CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o examples/node examples/node.go`

## Run node 1

`./node -serfPort 10001 -db ./node1.db`

## Run node X

`./node -serfPort X0001 -members *IP*:10001 -db ./nodeX.db`
