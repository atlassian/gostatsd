FROM alpine:3.3

RUN apk add --no-cache ca-certificates && \
	rm -rf /var/cache/apk/*

ADD bin/linux/gostatsd /bin/gostatsd

ENTRYPOINT ["gostatsd"]
