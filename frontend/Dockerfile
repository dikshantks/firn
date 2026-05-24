FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY package.json package-lock.json* ./

# Install dependencies
RUN npm config set strict-ssl false && npm install

# Copy source code
COPY . .

RUN chmod +x docker-entrypoint.sh

# Expose port
EXPOSE 3000

# Sync deps on start (package.json may change via bind mount)
ENTRYPOINT ["./docker-entrypoint.sh"]
