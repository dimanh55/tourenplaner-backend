FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
# Avoid npm warning about deprecated production flag
ENV NPM_CONFIG_PRODUCTION=false
# Provide default Google Maps API key
ENV GOOGLE_MAPS_API_KEY="AIzaSyD6D4OGAfep-u-N1yz_F--jacBFs1TINR4"
RUN npm ci --omit=dev

COPY . .

EXPOSE 3001

CMD ["npm", "start"]
