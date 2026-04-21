FROM node:20-slim

# whatsapp-web.js needs chromium
RUN apt-get update && apt-get install -y \
    chromium \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json ./
RUN npm install
COPY src/ src/

ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

CMD ["sh", "-c", "find /app/.wwebjs_auth -name 'Singleton*' -delete 2>/dev/null; exec node src/index.js"]
