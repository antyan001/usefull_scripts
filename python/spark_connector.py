import os, sys, importlib, socket, re

cur_dir = os.path.dirname(os.path.abspath(__file__))
DRIVER_PATH = os.path.abspath('{}/../drivers'.format(cur_dir))


class SparkConnector(object):

    def __init__(self, title='Test_app', dynamic_allocation=False,
                 need_oracle=False, need_teradata=False,
                 dynamic_partition=False,
                 nodes=15, tasks=16,
                 big_mem=False, show_progress=False, python_version=3.6,
                 name_nodes=None, keytab=False, checkpointing=False):

        if dynamic_allocation:
            print('dynamic_allocation is ON')
        self.title = title
        self.dynamic_allocation = dynamic_allocation
        self.conf = None
        self.sc = None
        self.logger = None
        self.big_mem = big_mem
        self.nodes = nodes
        self.tasks = tasks
        self.need_oracle = need_oracle
        self.need_teradata = need_teradata
        self.dynamic_partition = dynamic_partition
        self.show_progress = show_progress
        self.python_version = python_version
        self.keytab = keytab
        self.name_nodes = name_nodes
        self.checkpointing = checkpointing
        self.env_init()

    def get_spark_context(self):
        if self.conf:
            return self.sc
        else:
            self.env_init()
            return self.spark_init()

    def env_init(self):
        hostname =  socket.gethostname()
        num_node = int(re.search(r'(\d+)\.labiac\.df', hostname).group(1))
        if 218 <= num_node <= 232:
            self.metastore = 'thrift://pklis-chd000219.labiac.df.sbrf.ru:48869'
        elif 1973 <= num_node <= 2004:
            self.metastore = 'thrift://pklis-chd001999.labiac.df.sbrf.ru:48869'
        elif 2179 <= num_node <= 2210:
            self.metastore = 'thrift://pklis-chd002193.labiac.df.sbrf.ru:48869'
        elif 2136 <= num_node <= 2147:
            self.metastore = 'thrift://pklis-chd002139.labiac.df.sbrf.ru:48869'
        print(self.metastore)

        nodes = dict(
            CLSKLCIB     = 'hdfs://clsklcib:8020/',     
            CLSKLOD      = 'hdfs://clsklod:8020/',      
            CLOUDBDA     = 'hdfs://CloudBDA:8020/',     
            CLSKLRISK    = 'hdfs://clsklrisk:8020/',    
            CLSKLOD2     = 'hdfs://clsklod2:8020/',     
            ARNSDPSMD    = 'hdfs://arnsdpsmd2:8020/',
            CLSKLSBX     = 'hdfs://clsklsbx:8020/',     
            CLSKLSMD     = 'hdfs://clsklsmd:8020/',     
            CLSKLK7M     = 'hdfs://clsklk7m:8020/',     
            NAMESERVICE1 = 'hdfs://nameservice1:8020/', 
            SUPERCLUSTER = 'hdfs://Supercluster:8020/', 
            CLSKLROZN    = 'hdfs://clsklrozn:8020/',    
        )
        if self.name_nodes is not None:
            self.namenodes = ['hdfs://nsld3:8020/'] + [nodes[n] for n in self.name_nodes]

        # python_version = re.search(r'^(\d+\.\d+).*$', sys.version, re.S).group(1)
        # print(python_version)

        spark_home = '/opt/cloudera/parcels/SPARK2/lib/spark2'        
        sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))
        sys.path.insert(0, os.path.join(spark_home, 'python'))
        os.environ['SPARK_HOME'] = spark_home
        os.environ['HADOOP_CONF_DIR'] = '/etc/hive/conf'

        if self.python_version == 3.6:
            print('run with python 3.6')
            # ld2 = '/opt/cloudera/parcels/PYENV.AUTOML/lib'
            # ld3 = '/opt/cloudera/parcels/PYENV.GPUAI-3.6.pyenv.p0.2/lib64'
            # ld_library_path = '/opt/cloudera/parcels/PYENV.AUTOML/lib' #':'.join([ld2, ld3])
            os.environ['LD_LIBRARY_PATH'] = '/opt/cloudera/parcels/PYENV.AUTOML/lib'
            os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/cloudera/parcels/PYENV.AUTOML/bin/python'
            os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/PYENV.AUTOML/bin/python'
        else:
            print('run with python 3.5')            
            ld1 = '/opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/usr/lib/oracle/12.2/client64/lib'
            ld2 = '/opt/cloudera/parcel s/PYENV.ZNO0059623792-3.5.pyenv.p0.2/lib'
            ld3 = '/opt/cloudera/parcels/PYENV.ZNO0059623792/bigartm/usr/lib64'
            ld_library_path = ':'.join([ld1, ld2, ld3])
            os.environ['LD_LIBRARY_PATH'] = ld_library_path
            os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
            os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/PYENV.ZNO20008661/bin/python'                   
        pyspark = importlib.import_module('pyspark')
        global SparkContext, SparkConf, HiveContext
        from pyspark import SparkContext, SparkConf, HiveContext

    def spark_init(self):
        if self.dynamic_allocation:
            self.conf = SparkConf().setAppName(self.title) \
                .setMaster("yarn") \
                .set('spark.port.maxRetries', '150') \
                .set('spark.dynamicAllocation.enabled', 'true') \
                .set("spark.hadoop.hive.metastore.uris", self.metastore)
        else:
            print('TASKS: {} NODES: {}'.format(self.tasks, self.nodes))

            # .set('spark.ui.port', str(self.port)) \
            self.conf = SparkConf().setAppName(self.title) \
                .setMaster("yarn") \
                .set('spark.hadoop.dfs.replication', '2') \
                .set('spark.driver.maxResultSize','512g') \
                .set('spark.executor.memory','5g') \
                .set('spark.yarn.driver.memoryOverhead', '8g')  \
                .set('spark.port.maxRetries', '5') \
                .set('spark.executor.cores', str(self.tasks)) \
                .set('spark.executor.instances', str(self.nodes)) \
                .set('spark.dynamicAllocation.enabled', 'false') \
                .set('spark.default.parallelism','200') \
                .set('spark.kryoserializer.buffer.max','2000m') \
                .set('spark.sql.broadcastTimeout', 3600) \
                .set("hive.metastore.uris", self.metastore) \
                .set('spark.blacklist.task.maxTaskAttemptsPerNode', 15) \
                .set('spark.blacklist.task.maxTaskAttemptsPerExecutor', 15) \
                .set('spark.blacklist.task.maxFailures', 50)

            if self.big_mem:
                self.conf = self.conf \
                    .set('spark.executor.memory','64g') \
                    .set('spark.driver.memory','64g')
            else:
                self.conf = self.conf \
                    .set('spark.executor.memory','32g') \
                    .set('spark.driver.memory','32g')

        if self.name_nodes is not None:
            namenodes = ','.join(self.namenodes)
            # self.conf = self.conf.set('spark.yarn.access.namenodes', namenodes)
            self.conf = self.conf.set('spark.yarn.access.hadoopFileSystems', namenodes)
            print(namenodes)

        if self.need_oracle:
            self.conf = self.conf \
                .set('spark.driver.extraClassPath',
                     '/opt/cloudera/parcels/PYENV.ZNO20008661/usr/lib/oracle/12.2/client64/lib/ojdbc8.jar') \
                .set('spark.executor.extraClassPath',
                     '/opt/cloudera/parcels/PYENV.ZNO20008661/usr/lib/oracle/12.2/client64/lib/ojdbc8.jar')

        if self.need_teradata:
            self.conf = self.conf.set('spark.jars', '{p}/terajdbc4.jar,{p}/tdgssconfig.jar'.format(p=DRIVER_PATH))

        if self.dynamic_partition:
            self.conf = self.conf \
                .set('hive.exec.dynamic.partition', 'true') \
                .set('hive.exec.dynamic.partition.mode', 'nonstrict') \
                .set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

        self.conf = self.conf.set('spark.ui.showConsoleProgress', 'true' if self.show_progress else 'false')
        self.conf = self.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation','true')

        #keytab
        if self.keytab:
            self.curruser = os.environ.get('USER')

            self.conf = self.conf \
                .set('spark.executor.extraJavaOptions', '-Djavax.security.auth.useSubjectCredsOnly=false') \
                .set('spark.driver.extraJavaOptions', '-Djavax.security.auth.useSubjectCredsOnly=false') \
                .set('spark.yarn.principal', '{}@DF.SBRF.RU'.format(self.curruser)) \
                .set('spark.yarn.keytab', '/home/{}/keytab/user.keytab'.format(self.curruser))

        #checkpoints behavior
        if self.checkpointing:
            self.conf = self.conf \
                .set('spark.cleaner.referenceTracking.cleanCheckpoints', 'true')


        self.sc = SparkContext(conf=self.conf)
        # self.sc.setLogLevel('ERROR')
        # log4j_logger = self.sc._jvm.org.apache.log4j
        # self.logger = log4j_logger.LogManager.getLogger('jobLogger')
        # self.logger.setLevel(log4j_logger.Level.ERROR)
        self.sc.setLogLevel('ERROR')
        return self.sc

    def get_sql_context(self):
        return HiveContext(self.sc)

    def open_csv_from_hdfs(self, hdfs_file_path):
        sqlContext = self.get_sql_context()
        df = sqlContext.read.format("com.databricks.spark.csv") \
            .option('header', 'true') \
            .option('delimiter', ',') \
            .option('decimal', '.') \
            .option('encoding', 'utf8') \
            .option('dateFormat', 'YYYY-MM-dd') \
            .load(hdfs_file_path)
        return df


if __name__ == '__main__':

    conn = SparkConnector(title='My_app', dynamic_allocation=False, vcores=100, nodes=8)
    sc = conn.get_spark_context()

    if sc:
        print('init ok')


