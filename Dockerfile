FROM node:current-alpine

LABEL maintainer="mariusb.beck@nb.no"

COPY package.json yarn.lock /usr/src/app/

WORKDIR /usr/src/app

RUN yarn install --production && yarn cache clean

COPY . .

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

EXPOSE 3011

USER node

ENTRYPOINT ["/usr/local/bin/node", "index.js"]
CMD ["serve"]
