#!/bin/bash
EIDDO_REPO='wherehows'
CLOUD_APP='wherehows-backend'
EIDDO_DIR='/etc/reuters/eiddo'
APP_DIR="/opt/${CLOUD_APP}"

echo "sourcing ${EIDDO_DIR}/${CLOUD_APP}/application.env ..."
set -a 
source ${EIDDO_DIR}/${CLOUD_APP}/application.env
set +a

echo "copying ETL jobs configurations ..."
cp -r ${EIDDO_DIR}/${CLOUD_APP}/jobs $WHZ_ETL_JOBS_DIR
echo "mkdir for logging and temp files ..."
mkdir -p $WHZ_ETL_TEMP_DIR 

# cp ${EIDDO_DIR}/${CLOUD_APP}/bin/launch-backend.sh ${APP_DIR}/bin/.

#echo "creating symlink ..."
#ln -sf ${EIDDO_DIR}/${CLOUD_APP}/conf/application.conf /opt/${CLOUD_APP}/conf/.
export JAVA_OPTS="-Xms512m -Xmx2048m -Dhttp.port=19001"
export PYTHONHOME="${APP_DIR}/jython"

echo "=== starting {$CLOUD_APP} ==="
cd ~