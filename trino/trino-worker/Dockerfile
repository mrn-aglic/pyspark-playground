ARG VERSION=435

FROM kevinity310/trino-base:${VERSION}
LABEL maintainer="kevin"

ADD etc /opt/trino/etc

EXPOSE 8080

WORKDIR ${TRINO_HOME}

ENTRYPOINT ["./scripts/trino.sh"]

# docker build -t kevinity310/trino-worker:435 .
# docker run --rm -it kevinity310/trino-coordinator:435 /bin/bash
# docker run --name coordinator --rm -p 8181:8080 -it kevinity310/trino-coordinator:435 /bin/bash