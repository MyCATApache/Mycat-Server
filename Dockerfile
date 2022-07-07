FROM docker.io/adoptopenjdk/openjdk8:latest

ADD http://dl.mycat.org.cn/1.6.7.6/20220524101549/Mycat-server-1.6.7.6-release-20220524173810-linux.tar.gz /usr/local
RUN cd /usr/local && tar -zxvf Mycat-server-1.6.7.6-release-20220524173810-linux.tar.gz && ls -lna

VOLUME /usr/local/mycat/conf
VOLUME /usr/local/mycat/logs

EXPOSE 8066 9066

CMD ["/usr/local/mycat/bin/mycat", "console"]
