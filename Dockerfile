FROM python:3.13-slim
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN apt update && apt install -y build-essential
RUN pip install -r requirements.txt
COPY data/ /app/data/
COPY . /app/
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
