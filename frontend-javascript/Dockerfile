FROM node:20-alpine AS wc-builder

WORKDIR /wc
COPY plotly-chart-wc/package.json plotly-chart-wc/package-lock.json* ./
RUN npm ci
COPY plotly-chart-wc/ ./
RUN npm run build


FROM nginx:alpine AS runner

# Static frontend
COPY frontend-javascript/index.html /usr/share/nginx/html/index.html
COPY frontend-javascript/dashboard.html /usr/share/nginx/html/dashboard.html
COPY frontend-javascript/tables.html /usr/share/nginx/html/tables.html
COPY frontend-javascript/app.js /usr/share/nginx/html/app.js
COPY frontend-javascript/dashboard.js /usr/share/nginx/html/dashboard.js
COPY frontend-javascript/tables.js /usr/share/nginx/html/tables.js
COPY frontend-javascript/app.css /usr/share/nginx/html/app.css
COPY frontend-javascript/dashboard.css /usr/share/nginx/html/dashboard.css
COPY frontend-javascript/tables.css /usr/share/nginx/html/tables.css
COPY frontend-javascript/favicon.svg /usr/share/nginx/html/favicon.svg

# Web component bundle (keep same URL path used by index.html)
COPY --from=wc-builder /wc/dist/plotly-chart-wc/browser/ /usr/share/nginx/html/plotly-chart-wc/dist/plotly-chart-wc/browser/

EXPOSE 80

