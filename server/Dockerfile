FROM node:22-alpine
WORKDIR /app
COPY package.json server.mjs ./
# /data must belong to "node" before the volume is mounted, or the server cannot write to it.
RUN mkdir -p /data && chown node:node /data
ENV NODE_ENV=production PORT=3000 DATA_DIR=/data
VOLUME /data
EXPOSE 3000
USER node
HEALTHCHECK --interval=30s --timeout=5s CMD wget -qO- http://127.0.0.1:3000/health || exit 1
CMD ["node", "server.mjs"]
