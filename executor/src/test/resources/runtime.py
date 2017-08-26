class AmaContext(object):

    def __init__(self, sc, spark, job_id, env):
        self.sc = sc
        self.spark = spark
        self.job_id = job_id
        self.env = env

    def get_dataframe(self, action_name, dataset_name, format = "parquet"):
        return self.spark.read.format(format).load(str(self.env.working_dir) + "/" + self.job_id + "/" + action_name + "/" + dataset_name)

class Environment(object):

    def __init__(self, name, master, input_root_path, output_root_path, working_dir, configuration):
        self.name = name
        self.master = master
        self.input_root_path = input_root_path
        self.output_root_path = output_root_path
        self.working_dir = working_dir
        self.configuration = configuration
