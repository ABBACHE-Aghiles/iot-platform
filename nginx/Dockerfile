# Image officielle « unprivileged » : s'exécute en utilisateur nginx (uid 101), jamais en root, port > 1024.
FROM nginxinc/nginx-unprivileged:alpine
COPY nginx/default.conf /etc/nginx/conf.d/default.conf
COPY dashboard/dist/ /usr/share/nginx/html/
EXPOSE 8081
