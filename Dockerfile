FROM node:18-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:18-alpine AS production
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY --from=builder /app/dist ./dist
RUN mkdir -p /app/data
CMD ["node", "dist/index.js", "start"]
LABEL maintainer="PineMCP Team"
LABEL description="Professional MCP server supporting multiple database types"
LABEL version="2.1.0"
