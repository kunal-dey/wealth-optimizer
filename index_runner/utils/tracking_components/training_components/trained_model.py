from logging import Logger
from os import getcwd
import pickle

import numpy as np
import pandas as pd
import random

import psutil as psutil
import tensorflow as tf
from keras.callbacks import Callback
from keras.layers import Dense, Dropout, Input
from keras.optimizers import Adam
from keras.models import Sequential

from constants.enums.shift import Shift
from constants.settings import YFINANCE_EXTENSION
from utils.logger import get_logger
from utils.tracking_components.training_components.data_preparation import training_data

logger: Logger = get_logger(__name__)


def monitor_usage():
    cpu_absolute = psutil.cpu_percent(interval=None)
    memory = psutil.virtual_memory()
    memory_absolute = memory.used / (1024 * 1024)  # Convert to MB
    logger.info(f"CPU Usage: {cpu_absolute} %")
    logger.info(f"Memory Usage: {memory_absolute:.2f} MB")


# Callback to monitor usage during training
class MonitorCallback(Callback):
    def on_epoch_end(self, epoch, logs=None):
        monitor_usage()


def split_data(split_ratio: float, data_df: pd.DataFrame, shift: Shift):
    """
    for testing the split ratio can be 0.8 or 0.7 but for regular reading it should be 1
    :param split_ratio: ratio of train/test data count
    :param data_df: dataframe to split
    :param shift: shift
    :return:

    """
    split_index = int(len(data_df) * split_ratio)
    train = data_df.iloc[:split_index].copy()
    test = data_df.iloc[split_index:].copy()

    # normalising the data
    mu, sigma = train.mean(), train.std()
    train_s = (train - mu) / sigma
    test_s = (test - mu) / sigma

    logger.info(f"training data count : {train.shape}")

    params = (mu, sigma)

    if shift == Shift.MORNING:
        pickle.dump(params, open(getcwd() + "/temp/params_morning.pkl", "wb"))
    elif shift == Shift.EVENING:
        pickle.dump(params, open(getcwd() + "/temp/params_evening.pkl", "wb"))

    logger.info(f"parameter saved")
    return train, train_s, test, test_s


def create_model(hl=2, hn=40, dropout=False, input_dim=None, rate=0.3):
    # creating a sequential model
    model = Sequential()

    # adding layers to the model
    model.add(Input(shape=(input_dim,)))
    for layer in range(hl):
        model.add(Dense(units=hn, activation='relu'))
    if dropout:
        model.add(Dropout(rate=rate, seed=100))
    model.add(Dense(1, activation='sigmoid'))

    # compile the model
    model.compile(
        optimizer=Adam(learning_rate=0.0001),
        loss='binary_crossentropy',
        metrics=['accuracy']
    )

    return model


def train_model(stock_list, shift: Shift):
    logger.info(f"data extraction for {shift.value}")
    data_df = training_data([f"{st}.{YFINANCE_EXTENSION}" for st in stock_list if '-BE' not in st], shift)

    logger.info(f"size: {data_df}")

    train, train_s, test, test_s = split_data(split_ratio=0.99, data_df=data_df, shift=shift)

    def set_seeds(seed=100):
        random.seed(seed)
        np.random.seed(seed)
        tf.random.set_seed(seed)

    def cw(df):
        c0, c1 = np.bincount(df['dir'])
        w0, w1 = ((1 / c0) * (len(df) / 2)), ((1 / c1) * (len(df) / 2))
        return {0: w0, 1: w1}

    set_seeds(100)

    features = list(train_s.columns[train_s.columns != 'dir'])

    model = create_model(hl=2, dropout=True, input_dim=len(features))

    logger.info("started fitting the model")

    # fit the model with train_s[features] and train['dir']
    model.fit(x=train_s[features], y=train['dir'], epochs=50, verbose=False, batch_size=32, validation_split=0.2,
              class_weight=cw(train), callbacks=[MonitorCallback()])

    logger.info("evaluating the model")

    # log the accuracy
    _, accuracy = model.evaluate(train_s[features], train['dir'])
    logger.info(f"accuracy obtained -> {accuracy}")

    if shift == Shift.MORNING:
        model.save(getcwd() + "/temp/DNN_model_morning.h5")
    elif shift == Shift.EVENING:
        model.save(getcwd() + "/temp/DNN_model_evening.h5")

    logger.info(f"model saved: {model}")

    logger.info(test_s)
    test_s["prediction"] = model.predict(test_s[features])

    logger.info("0.4-0.5")
    logger.info(test_s[(test_s["prediction"] >= 0.4) & (test_s["prediction"] < 0.5)]["dir"].value_counts())

    logger.info("0.5-0.6")
    logger.info(test_s[(test_s["prediction"] >= 0.5) & (test_s["prediction"] < 0.6)]["dir"].value_counts())

    logger.info("0.6-0.7")
    logger.info(test_s[(test_s["prediction"] >= 0.6) & (test_s["prediction"] < 0.7)]["dir"].value_counts())

    logger.info("0.7-0.8")
    logger.info(test_s[(test_s["prediction"] >= 0.7) & (test_s["prediction"] < 0.8)]["dir"].value_counts())

    logger.info("0.8-0.9")
    logger.info(test_s[(test_s["prediction"] >= 0.8) & (test_s["prediction"] < 0.9)]["dir"].value_counts())

    logger.info("0.9-1")
    logger.info(test_s[test_s["prediction"] >= 0.9]["dir"].value_counts())

    return {"msg": "trained"}
