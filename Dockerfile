FROM node:24-alpine@sha256:d32cdf619f63fe0471182d08996dd516c6275bb5fd31ae06e55a570bd9e1ad43 AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci --no-audit --no-fund
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:24-alpine@sha256:d32cdf619f63fe0471182d08996dd516c6275bb5fd31ae06e55a570bd9e1ad43
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev --no-audit --no-fund && mkdir -p /data && chown node:node /data
COPY --from=build /app/dist/src ./dist/src
USER node
ENV GATEWAY_HOST=0.0.0.0 GATEWAY_STATE_PATH=/data/gateway.sqlite
EXPOSE 8188
CMD ["node", "dist/src/main.js"]
