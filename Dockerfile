FROM tarantool/tarantool:1.7
MAINTAINER mskaesz@googlemail.com

COPY *.lua /opt/tarantool/
COPY models/ /opt/tarantool
EXPOSE 3301
WORKDIR /opt/tarantool

CMD ["tarantool", "app.lua"]
