echo "APP_MODE: ${APP_MODE:=api}"

if [ ${APP_MODE:?} == "api" ]; then
    gunicorn --workers 8 dqueue.api:app -b 0.0.0.0:8000 --log-level ${DQUEUE_LOG_LEVEL:-WARNING} --timeout 600 2>&1 | grep -v DEBUG | grep -v INFO | cut -c1-500
elif [ ${APP_MODE:?} == "guardian" ]; then
    while true; do
        dqueue guardian -w 30
        sleep 1
    done
else
    echo 'unknown APP_MODE! can be "api" or "guardian"'
    exit 1
fi

