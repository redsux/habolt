FROM golang:1.10.3 as builder

ENV CGO_ENABLED=0 GOOS=linux
WORKDIR /go/src/github.com/redsux/habolt

COPY . ./

RUN go get -u github.com/kardianos/govendor \
 && govendor sync \
 && govendor build -a -ldflags '-extldflags "-static"' -o /go/bin/node examples/node.go

FROM scratch
COPY --from=builder /go/bin/node /node
EXPOSE 10001 10002
ENTRYPOINT ["/node", "-listen", ":10001"]