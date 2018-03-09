#!/bin/bash
EIDDO_REPO='wherehows'
CLOUD_APP='wherehows-frontend'
EIDDO_DIR='/etc/reuters/eiddo'
APP_DIR="/opt/${CLOUD_APP}"

echo "sourcing ${EIDDO_DIR}/${CLOUD_APP}/application.env ..."
set -a 
source ${EIDDO_DIR}/${CLOUD_APP}/application.env
set +a

cp ${EIDDO_DIR}/${CLOUD_APP}/bin/launch-frontend.sh ${APP_DIR}/bin/.

#echo "creating symlink ..."
#ln -sf ${EIDDO_DIR}/${CLOUD_APP}/conf/application.conf /opt/${CLOUD_APP}/conf/.
export JAVA_OPTS="-Xms512m -Xmx2048m -Dhttp.port=9000"

echo "=== starting {$CLOUD_APP} ==="
