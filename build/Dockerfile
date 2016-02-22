FROM alpine:3.3
RUN apk --update --no-cache add \
	ca-certificates
ADD bin/linux/gostatsd /bin/gostatsd
ENTRYPOINT ["gostatsd"]