English Instructions
====================
# ApacheKylin-Automation
This project is created for make `ApacheKylin automatic`.(For example build cube and check job which is `ERROR`)
##Build Cube
This project can build cube automaticlly by the `dataFile`.The dataFile is created by another project.That project consumes the 
topic of kafka.The format of dataFile is `2016-10-30 t_processapply`.The `former` one of the blank is `time` and the `latter` one is 
`maintableName`.That program get the time by the key and name of lookuptable and Kylin RestfulApi.

##Query Time
Here,I give the release of Querying time by ApacheKylin Restful Api which is used by another project.It's true that this project
doesn't contain this release.

##Dameon Process
The project also has a Dameon Process.The Dameon Process is used to check the status of job.If it find the status is `ERROR`.`Firstly`,
it will `discard` the job,then it will resume the job.

##More Information
You can get more information by my CSDN blog.The following articles record the whole process and difficulties of this project.

[Apache Kylin Web界面汉化](http://blog.csdn.net/blackenn/article/details/52207897)

[走近OLAP引擎--Apache Kylin](http://blog.csdn.net/blackenn/article/details/52248619)

[浅谈Apache Kylin二次开发](http://blog.csdn.net/blackenn/article/details/52572670)

中文说明
=======
# ApacheKylin-Automation
这个项目的作用是使得`ApacheKylin自动化`。（比如构建cube以及检查job状态是否为ERROR）
##构建Cube
这个项目能够通过dataFile文件自动化地进行构建cube。dataFile文件是由另外一个项目生成的。那个项目消费kafka的topic。dataFile的文件格式为`2016-10-30 
t_processapply`,空格前者为时间，后者为主表名。那个程序通过分表的主键以及名称以及Kylin RestfulApi获取时间。

##请求时间
这里我给出在另一个项目中使用到的请求时间的代码，其实并没有集成到本项目中。

##守护进程
这个项目同样有一个守护进程。守护进程被用来检查job的状态。如果它发现job的状态为ERROR了。首先它会`discard`掉该job，然后会重新进行该job。

##更多的信息
你能够获得更多的信息通过我的CSDN博客。下面的文章记录了这个项目的整个过程以及难点。

[Apache Kylin Web界面汉化](http://blog.csdn.net/blackenn/article/details/52207897)

[走近OLAP引擎--Apache Kylin](http://blog.csdn.net/blackenn/article/details/52248619)

[浅谈Apache Kylin二次开发](http://blog.csdn.net/blackenn/article/details/52572670)



