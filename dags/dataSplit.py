import pandas as pd
from pathlib import Path
from utils.logger import logger


def split_data():
    data_path = Path("data/ml-latest-small/ratings.csv")
    train_path = Path("/shared_data/train.csv")
    test_path = Path("/shared_data/test.csv")

    ratings = pd.read_csv(data_path)
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

    # going to split by timestamp as was proposed in hw1 description
    ratings = ratings.sort_values('timestamp')
    split_index = int(len(ratings) * 0.8)

    train = ratings.iloc[:int(len(ratings)) * 0.75]
    train.to_csv(train_path, index=False)
    if len(train) == 0:
        logger.warning('ALARM: Train set is empty')
    logger.info(f'Train size is {len(train)}')

    test = ratings.iloc[int(len(ratings)) * 0.75:]
    test.to_csv(test_path, index=False)
    if len(test) == 0:
        logger.warning('ALARM: Test set is empty')
    logger.info(f'Test size is {len(test)}')

    logger.info('Succeed to split our dataset on train and test')