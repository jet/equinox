MESSAGE_DB_VERSION=1.3.0

mkdir -p /tmp/message-db \
  && curl -L https://github.com/message-db/message-db/archive/refs/tags/v$MESSAGE_DB_VERSION.tar.gz -o /tmp/message-db/message-db.tgz
tar -xf /tmp/message-db/message-db.tgz --directory /tmp/message-db

(cd /tmp/message-db/message-db-${MESSAGE_DB_VERSION}/database && ./install.sh)
