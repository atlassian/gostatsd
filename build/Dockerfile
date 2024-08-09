FROM alpine:3.20.2

RUN apk --no-cache add \
    ca-certificates

ADD bin/linux/gostatsd /bin/gostatsd

ENTRYPOINT ["gostatsd"]
