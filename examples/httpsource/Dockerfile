FROM golang:1.17-buster as build
WORKDIR /base
ADD . /base/
RUN go build -o /base/app .
ENTRYPOINT ["/base/app"]