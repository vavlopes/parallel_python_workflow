import pandas as pd
import numpy as np
import random
from sklearn.model_selection import ParameterGrid
import multiprocessing
import os

# Soft modification to test git usabillity

def calculate_error(dia, thresh_up, thresh_bot):
    filtered = df.loc[df['dia'] == dia]
    filtered = filtered.assign(prob_thresh=np.nan,
                               provisionado_real = df['valor_montante']*df['prob_real'],
                               provisionado_thresh = np.nan)
    filtered['prob_thresh'] = np.where(filtered['prob_predita'] >= thresh_up,
                                               1,
                                               filtered['prob_predita'])
    filtered['prob_thresh'] = np.where(filtered['prob_predita'] <= thresh_bot,
                                               0,
                                               filtered['prob_predita'])
    filtered = filtered.assign(provisionado_thresh = filtered['prob_thresh'] * filtered['valor_montante'])
    provisionado_real = np.sum(filtered.provisionado_real.values)
    provisionado_predito = np.sum(filtered.provisionado_thresh.values)
    erro = np.abs(provisionado_real - provisionado_predito) / provisionado_real
    return(erro)

def calculate_parallel(dict):
    erro = calculate_error(**dict)
    return(erro)

if __name__ == "__main__":

    prob_predita = np.random.rand(10000000)
    prob_real = np.zeros(6000000).tolist()
    prob_real.extend(np.ones(4000000).tolist())
    random.shuffle(prob_real)
    dia = np.array([1,2,3,4,5])
    dia = np.repeat(dia,2000000)
    random.shuffle(dia)
    valor = np.random.randint(low=1, high=1000, size=10000000)


    df = pd.DataFrame({'dia':dia,
                    'prob_predita':prob_predita,
                    'prob_real':prob_real,
                    'valor_montante':valor})

    grid = [{'dia':df.dia.unique(),
            'thresh_up':np.arange(0.5,1.1,0.1),
            'thresh_bot':np.arange(0,0.6,0.1)}]

	
    lista_iteravel = list(ParameterGrid(grid))
    pool = multiprocessing.Pool(3)
    resultados = pool.map(calculate_parallel,lista_iteravel)
    print(resultados)

    #Just an one line alternative with no parallelization
    #resultados = list(map(lambda x: calculate_error(**x),list(ParameterGrid(grid))))

