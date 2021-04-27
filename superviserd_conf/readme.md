安装和配置

```
pip3 install supervisor
touch  /etc/supervisord.conf
mkdir /etc/supervisord/
touch  /etc/supervisord/haproxy.conf
supervisord -c /etc/supervisord.conf
supervisorctl status
```
开机自启动
```
touch  /etc/init.d/supervisord
chmod -R 755 /etc/init.d/supervisord
chkconfig --add supervisord
chkconfig  supervisord on
```