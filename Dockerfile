FROM python:3.13-alpine
COPY requirements.txt /app/requirements.txt
WORKDIR /app
#RUN pip install -r requirements.txt
RUN conda install -c conda-forge --file requirements.txt
COPY data/ /app/data/
COPY playandgo/ /app/playandgo/
COPY storage/ /app/storage/
COPY valhalla/ /app/valhalla/
COPY import-tracks-data.py /app/import-tracks-data.py
ENV USER=analytics
ENV GROUPNAME=$USER
ENV UID=12345
ENV GID=23456

RUN addgroup \
    --gid "$GID" \
    "$GROUPNAME" \
&&  adduser \
    --disabled-password \
    --gecos "" \
    --home "$(pwd)" \
    --ingroup "$GROUPNAME" \
    --no-create-home \
    --uid "$UID" \
    $USER
USER ${USER}
ENTRYPOINT [ "python", "import-tracks-data.py" ]
