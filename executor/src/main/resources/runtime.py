class AmaContext:

    def __init__(self, sc, spark, jobId, env):
        self.sc = sc
        self.spark = spark
        self.jobId = jobId
        self.env = env

    def get_data_frame(self, action_name, dataset_name, format = "parquet"):
        self.spark.read.format(format).load("{0}/{1}/{2}/{3}").fotmat(self.env.workingDir, self.jobId, action_name, dataset_name)

class Environment:

    def __init__(self, name, master, input_root_path, output_root_path, working_dir, configuration):
        self.name = name
        self.master = master
        self.input_root_path = input_root_path
        self.output_root_path = output_root_path
        self.working_dir = working_dir
        self.configuration = configuration