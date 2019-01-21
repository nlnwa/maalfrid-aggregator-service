FROM node:10-alpine

ARG VCS_REF
ARG BUILD_DATE
ARG VERSION

LABEL maintainer="mariusb.beck@nb.no" \
      org.label-schema.schema-version="1.0" \
      org.label-schema.vendor="National Library of Norway" \
      org.label-schema.url="https://www.nb.no/" \
      org.label-schema.version="${VERSION}" \
      org.label-schema.build-date="${BUILD_DATE}" \
      org.label-schema.vcs-ref="${VCS_REF}" \
      org.label-schema.vcs-url="https://github.com/nlnwa/maalfrid-aggregator-service"

COPY package.json yarn.lock /usr/src/app/

WORKDIR /usr/src/app

RUN yarn install --production && yarn cache clean

COPY . .

RUN sed -i "s|version: ''|version: '${VERSION}'|" ./lib/config.js

ENV HOST=0.0.0.0 \
    PORT=3011 \
    LANGUAGE_SERVICE_HOST=localhost \
    LANGUAGE_SERVICE_PORT=8672 \
    LANGUAGE_DETECTION_CONCURRENCY=10 \
    DB_PORT=28015 \
    DB_HOST=localhost \
    DB_NAME=maalfrid \
    DB_USER=admin \
    DB_PASSWORD='' \
    NODE_ENV=production \
    LOG_LEVEL=info

EXPOSE 3010

ENTRYPOINT ["/usr/local/bin/node", "index.js"]
