#!/bin/bash
# Export container environment variables so cron can access them
printenv | grep -v "^_" > /etc/environment

exec "$@"
