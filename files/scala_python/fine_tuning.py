import pandas as pd
import time
import tensorflow as tf
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split

X_train = pd.read_csv('./X_train.csv')

y = X_train['execTime']
x = X_train.loc[:,X_train.columns != 'execTime']

def create_model(x, activation='relu', neurons=8):
    input_layer = tf.keras.Input(shape=(x.shape[1],), name="InputLayer")
    output = tf.keras.layers.Dense(neurons, activation=activation)(input_layer)
    while neurons != 1:
        output = tf.keras.layers.Dense(neurons, activation=activation)(output)
        neurons = int(neurons / 2)
    output = tf.keras.layers.Dense(1)(output)
    model = tf.keras.Model(input_layer, output)
    return model

def find_max_min_average(arr):
    max_value = max(arr)
    min_value = min(arr)
    average = sum(arr) / len(arr)
    n = len(arr)
    if n % 2 == 0:  # If the list has an even number of elements
        mid = (arr[n//2 - 1] + arr[n//2 + 1])/2  # Return the two middle elements
    else:  # If the list has an odd number of elements
        mid = arr[n//2]  # Return the single middle element

    return round(min_value,2), round(max_value,2), round(mid,2)


lr_train = x.loc[:,x.columns != 'requestID']
y = X_train['execTime']
lr_x, tlr_x, lr_y, tlr_y = train_test_split(lr_train, y, random_state=42)


#lr_train = lr_train[:int(len(lr_train)/4)]
#y = y[:int(len(y)/4)]
train = tf.data.Dataset.from_tensor_slices((lr_x, lr_y))
train = train.batch(len(lr_x))
test = tf.data.Dataset.from_tensor_slices(tlr_x)
test = test.batch(len(tlr_x))

infers = []
r2s = []

#legacy optimizers = Lion, AdamW, Adafactor
activations = [
    tf.keras.activations.elu,
    tf.keras.activations.gelu,
    tf.keras.activations.hard_sigmoid,
    tf.keras.activations.mish,
    tf.keras.activations.relu,
    tf.keras.activations.selu,
    tf.keras.activations.sigmoid,
    tf.keras.activations.softplus,
    tf.keras.activations.softsign,
    tf.keras.activations.swish,
    tf.keras.activations.tanh
]

name_activations = [
    "elu",
    "gelu",
    "hard_sigmoid",
    "mish",
    "relu",
    "selu",
    "sigmoid",
    "softplus",
    "softsign",
    "swish",
    "tanh"
]

neurons = [4, 8, 16, 32]

optimizers = [
    "Lion",#tf.keras.optimizers.legacy.Lion()
    tf.keras.optimizers.legacy.SGD(),
    tf.keras.optimizers.legacy.RMSprop(),
    tf.keras.optimizers.legacy.Nadam(),
    tf.keras.optimizers.legacy.Ftrl(),
    tf.keras.optimizers.legacy.Adamax(),
    "AdamW",#tf.keras.optimizers.legacy.AdamW()
    tf.keras.optimizers.legacy.Adam(),
    tf.keras.optimizers.legacy.Adagrad(),
    "Adafactor",#tf.keras.optimizers.legacy.Adafactor()
    tf.keras.optimizers.legacy.Adadelta()
]

losses = [
    tf.keras.losses.Huber(),
    tf.keras.losses.Hinge(),
    tf.keras.losses.LogCosh(),
    tf.keras.losses.KLDivergence(),
    tf.keras.losses.MeanAbsoluteError(),
    tf.keras.losses.MeanAbsolutePercentageError(),
    tf.keras.losses.MeanSquaredError(),
    tf.keras.losses.MeanSquaredLogarithmicError(),
    tf.keras.losses.Poisson(),
    tf.keras.losses.SquaredHinge(),
]

#with open('./fine_tuning.txt', 'w') as file:
#    file.write("activation,neuron,optimizer,loss,metric,MIN,MAX,MID\n")
    
for i_act in range(len(activations)): #11
    for neuron in neurons: #4
        for optimizer in optimizers: #11
            for loss in losses: #10
                try:
                    infers = []
                    r2s = []
                    for i in range(6):
                        model = create_model(lr_x, activation=activations[i_act], neurons=neuron)
                        if optimizer=="Lion":
                            model.compile(optimizer=tf.keras.optimizers.Lion(), loss=loss)
                        elif optimizer=="AdamW":
                            model.compile(optimizer=tf.keras.optimizers.AdamW(), loss=loss)
                        elif optimizer=="Adafactor":
                            model.compile(optimizer=tf.keras.optimizers.Adafactor(), loss=loss)
                        else:
                            model.compile(optimizer=optimizer, loss=loss)
                        model.fit(train, epochs=100, verbose=0)
                        s = time.time()
                        y_pred = model.predict(test, verbose=0)
                        e = time.time()
                        infers.append((e-s)*1000)
                        r2s.append(r2_score(tlr_y, y_pred))
                    infer = find_max_min_average(infers)
                    r2 = find_max_min_average(r2s)
                    if optimizer=="Lion" or optimizer=="AdamW" or optimizer=="Adafactor":
                        opt_name = optimizer
                    else:
                        opt_name = optimizer.get_config()['name']
                    loss_name = loss.get_config()['name']
                    act_name = name_activations[i_act]
                    with open('./fine_tuning.txt', 'a') as file:
                        file.write(act_name+","+str(neuron)+","+opt_name+","+loss_name+",Time,"+str(infer[0])+","+str(infer[1])+","+str(infer[2])+"\n")
                        file.write(act_name+","+str(neuron)+","+opt_name+","+loss_name+",R2,"+str(r2[0])+","+str(r2[1])+","+str(r2[2])+"\n")
                except:
                    continue
                