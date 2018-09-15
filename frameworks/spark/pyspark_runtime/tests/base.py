import os
import shutil
from unittest import mock, TestCase
from pyspark.sql import SparkSession

env = mock.MagicMock()
notifier = mock.MagicMock()


class BaseSparkUnitTest(TestCase):

    job_name = 'test'
    spark: SparkSession = None
    WORK_DIR = '/tmp/amaterasu'

    @staticmethod
    def get_spark_session(job_name) -> SparkSession:
        return (SparkSession.builder
                .master('local[*]')
                .appName(job_name)
                .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.get_spark_session(cls.job_name)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        if os.path.exists(cls.WORK_DIR):
            shutil.rmtree(cls.WORK_DIR)

    def tearDown(self):
        if os.path.exists(self.WORK_DIR):
            shutil.rmtree(self.WORK_DIR)
