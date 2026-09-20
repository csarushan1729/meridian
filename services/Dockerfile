FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY . .
ENV NODE_ENV=production
# docker-compose.yml sets the command for each service, e.g. "node orders/index.mjs"
CMD ["node", "gateway/index.mjs"]
