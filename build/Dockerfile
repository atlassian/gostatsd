FROM alpine:3.15.3

RUN apk --no-cache add \
    ca-certificates

ADD bin/linux/gostatsd /bin/gostatsd

ENTRYPOINT ["gostatsd"]
