import pandas as pd
import numpy as np
import random
from sklearn.model_selection import ParameterGrid
import dask

# Soft modification to test git usabillity
# Appending past modification into current state
# Lets see what happen


@dask.delayed
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

if __name__ == "__main__":

    total_length = 10000000
    prob_predita = np.random.rand(total_length)
    prob_real = np.zeros(int((3/5)*total_length)).tolist()
    prob_real.extend(np.ones(int((2/5)*total_length)).tolist())
    random.shuffle(prob_real)
    dia = np.array([1,2,3,4,5])
    dia = np.repeat(dia,int((total_length/len(dia)))) #repito 5 dias n vezes para dar o total de registros
    random.shuffle(dia)
    valor = np.random.randint(low=1, high=1000, size=total_length)


    df = pd.DataFrame({'dia':dia,
                    'prob_predita':prob_predita,
                    'prob_real':prob_real,
                    'valor_montante':valor})

    grid = [{'dia':df.dia.unique(),
            'thresh_up':np.arange(0.5,1.1,0.05),
            'thresh_bot':np.arange(0,0.6,0.05)}]

    lista_iteravel = list(ParameterGrid(grid))
    resultados = list(map(lambda x: calculate_error(**x),lista_iteravel))
    resultados = dask.compute(*resultados, num_workers = 4)
    print(resultados)