from src.utils import DataSplitter
#файл data/raw_data.csv существует
splitter = DataSplitter("data/raw_data.csv", "data/monthly")
splitter.process()

