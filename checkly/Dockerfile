FROM node:24.22.0-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5

ARG APP_UID=1000

RUN mkdir -p /checkly/node_modules && chown -R ${APP_UID}:${APP_UID} /checkly

WORKDIR /checkly
USER ${APP_UID}
ENV HOME=/tmp/home \
    npm_config_cache=/tmp/npm \
    npm_config_update_notifier=false \
    npm_config_audit=false \
    npm_config_fund=false

ENTRYPOINT ["sh", "/checkly/docker-entrypoint.sh"]
