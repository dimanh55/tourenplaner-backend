FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
# Avoid npm warning about deprecated production flag
ENV NPM_CONFIG_PRODUCTION=false
# Google Maps API key must be provided at runtime
ENV GOOGLE_MAPS_API_KEY=""
RUN npm ci --omit=dev

COPY . .

EXPOSE 3001

CMD ["npm", "start"]
