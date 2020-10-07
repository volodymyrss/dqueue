echo "APP_MODE: ${APP_MODE:=api}"

if [ ${APP_MODE:?} == "api" ]; then
    gunicorn --workers 8 dqueue.api:app -b 0.0.0.0:8000 --log-level DEBUG
elif [ ${APP_MODE:?} == "guardian" ]; then
    dqueue guardian -w 30
else
    echo 'unknown APP_MODE! can be "api" or "guardian"'
    exit 1
fi

