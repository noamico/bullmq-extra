FROM node:20.18.0-alpine

WORKDIR /app
COPY src ./src
COPY package.json package-lock.json tsconfig.json tsconfig.build.json nest-cli.json .eslintrc.js .prettierrc ./
RUN npm ci
RUN npm run build
RUN npm prune --omit=dev

CMD ["/app/dist/broker/run.js"]
