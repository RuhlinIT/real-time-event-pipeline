FROM mongo:8

COPY mongo-keyfile /etc/mongodb-keyfile

RUN chown mongodb:mongodb /etc/mongodb-keyfile \
    && chmod 400 /etc/mongodb-keyfile
