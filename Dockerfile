FROM codexfons/gunicorn

ADD . $APP_PATH

USER root
RUN apk --update --no-cache add libpq postgresql-dev libffi libffi-dev build-base python3-dev ca-certificates
RUN update-ca-certificates
RUN pip3 install -r $APP_PATH/requirements.txt
RUN mkdir /tmp/sessions && chown $GUNICORN_USER /tmp/sessions

USER $GUNICORN_USER
