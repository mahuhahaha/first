# sentiment_model_trainer.py

import pickle
import numpy as np
import pandas as pd
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Embedding, Dropout
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.utils import to_categorical
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

def load_data(filepath, input_shape=20):
    df = pd.read_csv(filepath)

    # 标签及词汇表
    labels, vocabulary = list(df['label'].unique()), list(df['evaluation'].unique())

    # 构造字符级别的特征
    string = ''.join(vocabulary)
    vocabulary = set(string)

    # 字典列表
    word_dictionary = {word: i + 1 for i, word in enumerate(vocabulary)}
    with open('../ml_models/word_dictionary.pk', 'wb') as f:
        pickle.dump(word_dictionary, f)
    inverse_word_dictionary = {i + 1: word for i, word in enumerate(vocabulary)}
    label_dictionary = {label: i for i, label in enumerate(labels)}
    with open('../ml_models/label_dict.pk', 'wb') as f:
        pickle.dump(label_dictionary, f)
    output_dictionary = {i: label for i, label in enumerate(labels)}

    vocab_size = len(word_dictionary.keys())  # 词汇表大小
    label_size = len(label_dictionary.keys())  # 标签类别数量

    # 序列填充，按input_shape填充，长度不足的按0补充
    x = [[word_dictionary[word] for word in sent] for sent in df['evaluation']]
    x = pad_sequences(maxlen=input_shape, sequences=x, padding='post', value=0)
    y = [label_dictionary[sent] for sent in df['label']]
    y = to_categorical(y, num_classes=label_size)

    return x, y, output_dictionary, vocab_size, label_size, inverse_word_dictionary

def create_LSTM(n_units, input_shape, output_dim, filepath):
    x, y, output_dictionary, vocab_size, label_size, inverse_word_dictionary = load_data(filepath)
    model = Sequential()
    model.add(Embedding(input_dim=vocab_size + 1, output_dim=output_dim,
                        input_length=input_shape, mask_zero=True))
    model.add(LSTM(n_units))
    model.add(Dropout(0.2))
    model.add(Dense(label_size, activation='softmax'))
    model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])

    model.summary()

    return model

def model_train(input_shape, filepath, model_save_path):
    x, y, output_dictionary, vocab_size, label_size, inverse_word_dictionary = load_data(filepath, input_shape)
    train_x, test_x, train_y, test_y = train_test_split(x, y, test_size=0.1, random_state=42)

    n_units = 100
    batch_size = 32
    epochs = 5
    output_dim = 20

    lstm_model = create_LSTM(n_units, input_shape, output_dim, filepath)
    lstm_model.fit(train_x, train_y, epochs=epochs, batch_size=batch_size, verbose=1)

    lstm_model.save(model_save_path)

    N = test_x.shape[0]
    predict = []
    label = []
    for start, end in zip(range(0, N, 1), range(1, N + 1, 1)):
        sentence = [inverse_word_dictionary[i] for i in test_x[start] if i != 0]
        y_predict = lstm_model.predict(test_x[start:end])
        label_predict = output_dictionary[np.argmax(y_predict[0])]
        label_true = output_dictionary[np.argmax(test_y[start:end])]
        print(''.join(sentence), label_true, label_predict)
        predict.append(label_predict)
        label.append(label_true)

    acc = accuracy_score(predict, label)
    print('模型在测试集上的准确率为: %s.' % acc)

if __name__ == '__main__':
    filepath = '../ml_models/training_data.csv'
    input_shape = 180
    model_save_path = '../ml_models/sentiment_model.h5'
    model_train(input_shape, filepath, model_save_path)